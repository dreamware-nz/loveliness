package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/johnjansen/loveliness/pkg/api"
	"github.com/johnjansen/loveliness/pkg/auth"
	"github.com/johnjansen/loveliness/pkg/backup"
	boltpkg "github.com/johnjansen/loveliness/pkg/bolt"
	"github.com/johnjansen/loveliness/pkg/catalog"
	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/config"
	"github.com/johnjansen/loveliness/pkg/ingest"
	"github.com/johnjansen/loveliness/pkg/logging"
	"github.com/johnjansen/loveliness/pkg/replication"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/tlsutil"
	"github.com/johnjansen/loveliness/pkg/transport"
)

func main() {
	// Subcommand dispatch.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "up":
			runUp(os.Args[2:])
			return
		case "query":
			runQuery(os.Args[2:])
			return
		case "help", "--help", "-h":
			printUsage()
			return
		case "version", "--version", "-v":
			fmt.Println("loveliness dev")
			return
		case "serve":
			// Fall through to normal server startup.
		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
			printUsage()
			os.Exit(1)
		}
	}

	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	logging.Setup(cfg.NodeID)

	slog.Info("starting",
		"bind", cfg.BindAddr,
		"raft", cfg.RaftAddr,
		"data_dir", cfg.DataDir,
		"shards", cfg.ShardCount,
	)

	// Calculate threads per shard to avoid CPU oversubscription.
	threadsPerShard := uint64(runtime.NumCPU()) / uint64(cfg.ShardCount)
	if threadsPerShard < 1 {
		threadsPerShard = 1
	}

	// Calculate per-shard buffer pool size.
	// LadybugDB defaults to 80% of system memory PER shard — with N shards
	// that's N*80% which guarantees OOM. Auto-calculate a safe limit.
	var bufferPoolBytes uint64
	if cfg.ShardBufferMB > 0 {
		bufferPoolBytes = uint64(cfg.ShardBufferMB) * 1024 * 1024
	} else {
		// Auto: 70% of total memory / shard count, minimum 256MB.
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		totalMem := m.Sys
		if totalMem < 512*1024*1024 {
			// Sys can underreport early; fall back to a conservative estimate.
			totalMem = 2 * 1024 * 1024 * 1024 // 2GB minimum assumption
		}
		bufferPoolBytes = (totalMem * 7 / 10) / uint64(cfg.ShardCount)
		if bufferPoolBytes < 256*1024*1024 {
			bufferPoolBytes = 256 * 1024 * 1024
		}
	}
	slog.Info("buffer pool configured", "per_shard_mb", bufferPoolBytes/(1024*1024), "shards", cfg.ShardCount)

	// Open local shards.
	// Note: LadybugDB must create its own database directory. We only create
	// the parent directory here — not the shard directories themselves.
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		slog.Error("create data dir failed", "path", cfg.DataDir, "err", err)
		os.Exit(1)
	}
	shards := make([]*shard.Shard, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		shardDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", i))
		store, err := shard.NewLbugStore(shardDir, threadsPerShard, bufferPoolBytes)
		if err != nil {
			slog.Error("open shard failed", "shard", i, "err", err)
			os.Exit(1)
		}
		shards[i] = shard.NewShard(i, store, cfg.MaxConcurrentQueries)
		slog.Info("shard opened", "shard", i, "path", shardDir, "threads", threadsPerShard, "buffer_mb", bufferPoolBytes/(1024*1024))
	}

	// Start Raft cluster.
	c, err := cluster.New(cfg.NodeID, cfg.RaftAddr, cfg.DataDir, cfg.Bootstrap)
	if err != nil {
		slog.Error("start cluster failed", "err", err)
		os.Exit(1)
	}
	slog.Info("raft started", "bootstrap", cfg.Bootstrap)

	// Create query router with schema registry for shard key routing
	// and cross-shard resolution.
	queryTimeout := time.Duration(cfg.QueryTimeoutMs) * time.Millisecond
	reg := schema.NewRegistry()

	// Wire Raft FSM schema callback to keep the registry in sync across all nodes.
	c.SetSchemaCallback(func(tables map[string]string) {
		for name, shardKey := range tables {
			reg.Register(name, shardKey)
		}
	})

	// Populate registry from Raft state first (survives restarts).
	for name, shardKey := range c.GetSchema() {
		reg.Register(name, shardKey)
	}

	// Fallback: introspect shard 0 for tables not yet in Raft state
	// (e.g. tables created before this feature was deployed).
	discoverSchema(shards[0], reg)

	r := router.NewRouterWithSchema(shards, queryTimeout, reg)

	// Replicate DDL changes (CREATE/DROP NODE TABLE) via Raft.
	r.SetDDLHook(c)

	// Phase B: Initialize per-shard Bloom filters (5M expected keys per shard, 1% FPR).
	// At 5M keys and 1% FPR each filter uses ~6 MB, so 4 shards = ~24 MB total.
	for i := 0; i < cfg.ShardCount; i++ {
		r.BloomIndex().InitShard(i, 5_000_000, 0.01)
	}

	// Multi-database support: catalog tracks databases, DatabaseRouter multiplexes queries.
	cat := catalog.NewCatalog()
	dbRouter := router.NewDatabaseRouter(cat, queryTimeout)

	// Configure distributed query routing (cross-node shard access).
	transportClient := transport.NewClient(queryTimeout)
	r.SetRemoteTransport(cfg.NodeID, transport.NewRouterAdapter(transportClient), cluster.NewPlacementAdapter(c))

	// Sync transport client peers from cluster node list.
	syncPeers := func(sm cluster.ShardMap) {
		for _, info := range sm.Nodes {
			if !info.Alive || info.ID == cfg.NodeID {
				continue
			}
			if info.HTTPAddr != "" {
				transportClient.SetPeer(info.ID, info.HTTPAddr)
			}
			if info.GRPCAddr != "" {
				transportClient.SetPeerTCP(info.ID, info.GRPCAddr)
			}
		}
	}
	syncPeers(c.GetShardMap())

	// Initialize WAL for disaster recovery.
	walDir := filepath.Join(cfg.DataDir, "wal")
	wal, err := replication.NewWAL(walDir)
	if err != nil {
		slog.Error("WAL init failed", "err", err)
		os.Exit(1)
	}
	slog.Info("WAL initialized", "dir", walDir, "sequence", wal.LastSequence())
	r.SetWAL(wal)

	// Initialize backup store (S3 if configured, otherwise local).
	backupMgr := backup.NewManager(cfg.DataDir, cfg.NodeID)
	dr := &api.DRExtension{
		BackupMgr:    backupMgr,
		WAL:          wal,
		ReplicaState: replication.NewReplicaState(),
	}

	if cfg.S3Bucket != "" {
		s3Store, err := backup.NewS3Store(context.Background(), backup.S3Config{
			Bucket:   cfg.S3Bucket,
			Region:   cfg.S3Region,
			Prefix:   cfg.S3Prefix,
			Endpoint: cfg.S3Endpoint,
		})
		if err != nil {
			slog.Error("S3 backup store init failed", "err", err)
			os.Exit(1)
		}
		dr.BackupStore = s3Store
		slog.Info("S3 backup store configured", "bucket", cfg.S3Bucket, "prefix", cfg.S3Prefix)
	} else if cfg.BackupDir != "" {
		localStore, err := backup.NewLocalStore(cfg.BackupDir)
		if err != nil {
			slog.Error("local backup store init failed", "err", err)
			os.Exit(1)
		}
		dr.BackupStore = localStore
		slog.Info("local backup store configured", "dir", cfg.BackupDir)
	}

	if cfg.BackupIntervalMin > 0 && dr.BackupStore != nil {
		sched := backup.NewScheduler(
			backupMgr, dr.BackupStore, shards,
			func() uint64 { return wal.LastSequence() },
			time.Duration(cfg.BackupIntervalMin)*time.Minute,
			cfg.BackupRetention,
		)
		dr.Scheduler = sched
		sched.Start()
	}

	// Build TLS configurations if enabled.
	tlsCfg := tlsutil.Config{
		CertFile:   cfg.TLSCert,
		KeyFile:    cfg.TLSKey,
		CAFile:     cfg.TLSCA,
		Mode:       cfg.TLSMode,
		ClientAuth: cfg.TLSClientAuth,
	}
	if tlsCfg.Enabled() {
		slog.Info("TLS enabled", "mode", cfg.TLSMode, "cert", cfg.TLSCert, "ca", cfg.TLSCA)
	}

	// Start TCP transport server for msgpack-based inter-node communication.
	tcpSrv := transport.NewTCPServer(transport.NewShardSet(shards))
	if tlsCfg.Enabled() && tlsCfg.CAFile != "" {
		mtls, err := tlsutil.MutualTLSConfig(tlsCfg)
		if err != nil {
			slog.Error("inter-node mTLS config failed", "err", err)
			os.Exit(1)
		}
		tcpSrv.SetTLS(mtls)
	}
	if err := tcpSrv.Listen(cfg.GRPCAddr); err != nil {
		slog.Error("tcp transport listen failed", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	// Initialize log-backed ingest queue and worker.
	ingestDir := filepath.Join(cfg.DataDir, "ingest")
	ingestQueue, err := ingest.NewQueue(ingestDir)
	if err != nil {
		slog.Error("ingest queue init failed", "err", err)
		os.Exit(1)
	}
	shardAdapter := ingest.NewShardAdapter(shards, r.ResolveShardForKey)
	ingestWorker := ingest.NewWorker(ingestQueue, shardAdapter, shardAdapter, 50_000)
	ingestWorker.Start()
	slog.Info("ingest queue initialized", "dir", ingestDir)

	// Set up authentication.
	tokenAuth := auth.New(cfg.AuthToken)
	if tokenAuth.Enabled() {
		slog.Info("authentication enabled")
	}

	// Start HTTP server with DR extensions.
	srv := api.NewServer(r, c, shards, reg, queryTimeout)
	srv.SetDR(dr)
	srv.SetAuth(tokenAuth)
	srv.SetIngestQueue(ingestQueue)
	srv.SetDatabaseRouter(dbRouter)
	httpServer := &http.Server{
		Addr:         cfg.BindAddr,
		Handler:      srv.Handler(),
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}

	go func() {
		if tlsCfg.Enabled() {
			slog.Info("https listening", "addr", cfg.BindAddr)
			serverTLS, err := tlsutil.ServerTLSConfig(tlsCfg)
			if err != nil {
				slog.Error("http TLS config failed", "err", err)
				os.Exit(1)
			}
			httpServer.TLSConfig = serverTLS
			if err := httpServer.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
				slog.Error("https server error", "err", err)
				os.Exit(1)
			}
		} else {
			slog.Info("http listening", "addr", cfg.BindAddr)
			if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
				slog.Error("http server error", "err", err)
				os.Exit(1)
			}
		}
	}()

	// Start Bolt protocol server (Neo4j wire compatibility).
	var boltSrv *boltpkg.Server
	if cfg.BoltAddr != "" {
		boltAdapter := boltpkg.NewRouterAdapter(r)
		boltSrv = boltpkg.NewServer(cfg.BoltAddr, boltAdapter)
		boltSrv.SetCluster(&clusterTopologyAdapter{cluster: c})
		// Wire multi-database support via DatabaseRouterAdapter.
		dbAdapter := boltpkg.NewDatabaseRouterAdapter(dbRouter, c)
		boltSrv.SetDatabaseExecutor(dbAdapter)
		if tlsCfg.Enabled() {
			boltTLS, err := tlsutil.ServerTLSConfig(tlsCfg)
			if err != nil {
				slog.Error("bolt TLS config failed", "err", err)
				os.Exit(1)
			}
			boltSrv.SetTLS(boltTLS)
		}
		if cfg.AuthToken != "" {
			boltSrv.SetAuth(cfg.AuthToken)
		}
		if err := boltSrv.Start(); err != nil {
			slog.Error("bolt server failed", "err", err)
			os.Exit(1)
		}
	}

	// Set discovery info on the API server for the public /discovery endpoint.
	selfInfo := cluster.JoinInfo{
		NodeID:   cfg.NodeID,
		RaftAddr: cfg.RaftAddr,
		GRPCAddr: cfg.GRPCAddr,
		HTTPAddr: cfg.BindAddr,
		BoltAddr: cfg.BoltAddr,
	}
	srv.SetDiscoveryInfo(selfInfo)

	// Register this node in the cluster so ROUTE responses include it.
	if cfg.Bootstrap {
		_ = c.RegisterNode(cluster.NodeInfo{
			ID:       cfg.NodeID,
			RaftAddr: cfg.RaftAddr,
			GRPCAddr: cfg.GRPCAddr,
			HTTPAddr: cfg.BindAddr,
			BoltAddr: cfg.BoltAddr,
			Alive:    true,
		})
	}

	// DNS-based peer discovery.
	var disc *cluster.Discovery
	if cfg.DiscoverMode == "dns" && cfg.DiscoverAddr != "" {
		interval := 5 * time.Second
		if cfg.DiscoverInterval > 0 {
			interval = time.Duration(cfg.DiscoverInterval) * time.Second
		}

		_, httpPort, _ := net.SplitHostPort(cfg.BindAddr)
		if httpPort == "" {
			httpPort = "8080"
		}

		discCfg := cluster.DiscoveryConfig{
			Addr:              cfg.DiscoverAddr,
			Interval:          interval,
			ExpectedNodes:     cfg.ExpectedNodes,
			HTTPPort:          httpPort,
			Self:              selfInfo,
			BootstrapExplicit: cfg.Bootstrap,
			Resolver:          nil,
		}

		joinFn := func(info cluster.JoinInfo) error {
			body, _ := json.Marshal(map[string]string{
				"node_id":   info.NodeID,
				"raft_addr": info.RaftAddr,
				"grpc_addr": info.GRPCAddr,
				"http_addr": info.HTTPAddr,
				"bolt_addr": info.BoltAddr,
			})
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(
				fmt.Sprintf("http://localhost:%s/join", httpPort),
				"application/json",
				bytes.NewReader(body),
			)
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("join returned %d", resp.StatusCode)
			}
			return nil
		}

		disc = cluster.NewDiscovery(discCfg, joinFn)

		// Auto-bootstrap if not explicitly set.
		if !cfg.Bootstrap {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				should, err := disc.ShouldBootstrap(ctx)
				if err != nil {
					slog.Warn("discovery: auto-bootstrap check failed", "err", err)
					return
				}
				if should {
					slog.Info("discovery: auto-bootstrapping as lowest-IP node")
					if err := c.Bootstrap(); err != nil {
						slog.Warn("discovery: bootstrap failed (may already be bootstrapped)", "err", err)
					} else {
						slog.Info("discovery: raft bootstrap complete, registering self")
						_ = c.RegisterNode(cluster.NodeInfo{
							ID:       cfg.NodeID,
							RaftAddr: cfg.RaftAddr,
							GRPCAddr: cfg.GRPCAddr,
							HTTPAddr: cfg.BindAddr,
							BoltAddr: cfg.BoltAddr,
							Alive:    true,
						})
					}
				}
				disc.Start()
			}()
		} else {
			disc.Start()
		}
		slog.Info("dns discovery enabled", "addr", cfg.DiscoverAddr, "interval", interval)
	}

	// Auto-join cluster if peers are configured (legacy mode).
	if len(cfg.Peers) > 0 && !cfg.Bootstrap {
		go autoJoin(cfg)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutting down", "signal", sig.String())

	// Graceful shutdown: stop workers, drain HTTP, close WAL, raft, shards.
	if disc != nil {
		disc.Stop()
	}
	if boltSrv != nil {
		boltSrv.Stop()
	}
	tcpSrv.Stop()
	transportClient.Close()
	ingestWorker.Stop()
	if dr.Scheduler != nil {
		dr.Scheduler.Stop()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	if err := wal.Close(); err != nil {
		slog.Error("WAL close error", "err", err)
	}
	if err := c.Shutdown(); err != nil {
		slog.Error("raft shutdown error", "err", err)
	}
	for _, s := range shards {
		if err := s.Close(); err != nil {
			slog.Error("shard close error", "shard", s.ID, "err", err)
		}
	}
	slog.Info("stopped")
}

// clusterTopologyAdapter adapts the Cluster to the bolt.ClusterTopology interface.
type clusterTopologyAdapter struct {
	cluster *cluster.Cluster
}

func (a *clusterTopologyAdapter) BoltAddresses() (all []string, leader string) {
	sm := a.cluster.GetShardMap()
	leaderAddr := a.cluster.LeaderAddr()

	for _, info := range sm.Nodes {
		if !info.Alive || info.BoltAddr == "" {
			continue
		}
		all = append(all, info.BoltAddr)
		if info.RaftAddr == leaderAddr {
			leader = info.BoltAddr
		}
	}
	return all, leader
}

// autoJoin attempts to join the cluster via the configured peers.
// Peers are Raft addresses (e.g., "node1:9000"). We derive the HTTP address
// by replacing the port with the standard HTTP port (8080).
func autoJoin(cfg config.Config) {
	// Wait for the cluster to stabilize.
	time.Sleep(3 * time.Second)

	body, _ := json.Marshal(map[string]string{
		"node_id":   cfg.NodeID,
		"raft_addr": cfg.RaftAddr,
		"grpc_addr": cfg.GRPCAddr,
		"http_addr": cfg.BindAddr,
		"bolt_addr": cfg.BoltAddr,
	})

	client := &http.Client{Timeout: 5 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		for _, peer := range cfg.Peers {
			host, _, err := net.SplitHostPort(peer)
			if err != nil {
				host = peer
			}
			url := fmt.Sprintf("http://%s:8080/join", host)

			resp, err := client.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				slog.Debug("auto-join attempt failed", "peer", url, "err", err)
				continue
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				slog.Info("joined cluster", "peer", url)
				return
			}
			slog.Debug("auto-join rejected", "peer", url, "status", resp.StatusCode)
		}
		time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
	}
	slog.Warn("auto-join exhausted retries, node running standalone")
}

// discoverSchema introspects a shard to discover existing node tables and
// register them in the schema registry. This ensures the registry survives
// restarts without requiring schema state in the Raft FSM.
func discoverSchema(s *shard.Shard, reg *schema.Registry) {
	resp, err := s.Query("CALL show_tables() RETURN *")
	if err != nil {
		slog.Warn("schema discovery: show_tables failed", "err", err)
		return
	}
	for _, row := range resp.Rows {
		ttype, _ := row["type"].(string)
		if ttype != "NODE" {
			continue
		}
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		pk := discoverPrimaryKey(s, name)
		if pk != "" {
			reg.Register(name, pk)
			slog.Info("schema discovery: registered table", "table", name, "shard_key", pk)
		}
	}
}

func discoverPrimaryKey(s *shard.Shard, tableName string) string {
	resp, err := s.Query(fmt.Sprintf("CALL table_info('%s') RETURN *", tableName))
	if err != nil {
		slog.Warn("schema discovery: table_info failed", "table", tableName, "err", err)
		return ""
	}
	for _, row := range resp.Rows {
		isPK := false
		switch v := row["primary key"].(type) {
		case bool:
			isPK = v
		case float64:
			isPK = v != 0
		}
		if isPK {
			if name, ok := row["name"].(string); ok {
				return name
			}
		}
	}
	return ""
}
