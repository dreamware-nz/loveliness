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
	"github.com/johnjansen/loveliness/pkg/backup"
	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/config"
	"github.com/johnjansen/loveliness/pkg/ingest"
	"github.com/johnjansen/loveliness/pkg/logging"
	"github.com/johnjansen/loveliness/pkg/replication"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

func main() {
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
		store, err := shard.NewLbugStore(shardDir, threadsPerShard)
		if err != nil {
			slog.Error("open shard failed", "shard", i, "err", err)
			os.Exit(1)
		}
		shards[i] = shard.NewShard(i, store, cfg.MaxConcurrentQueries)
		slog.Info("shard opened", "shard", i, "path", shardDir, "threads", threadsPerShard)
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
	r := router.NewRouterWithSchema(shards, queryTimeout, reg)

	// Phase B: Initialize per-shard Bloom filters (5M expected keys per shard, 1% FPR).
	// At 5M keys and 1% FPR each filter uses ~6 MB, so 4 shards = ~24 MB total.
	for i := 0; i < cfg.ShardCount; i++ {
		r.BloomIndex().InitShard(i, 5_000_000, 0.01)
	}

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

	// Start TCP transport server for msgpack-based inter-node communication.
	tcpSrv := transport.NewTCPServer(transport.NewShardSet(shards))
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

	// Start HTTP server with DR extensions.
	srv := api.NewServer(r, c, shards, reg, queryTimeout)
	srv.SetDR(dr)
	srv.SetIngestQueue(ingestQueue)
	httpServer := &http.Server{
		Addr:         cfg.BindAddr,
		Handler:      srv.Handler(),
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}

	go func() {
		slog.Info("http listening", "addr", cfg.BindAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	// Auto-join cluster if peers are configured.
	if len(cfg.Peers) > 0 && !cfg.Bootstrap {
		go autoJoin(cfg)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutting down", "signal", sig.String())

	// Graceful shutdown: stop workers, drain HTTP, close WAL, raft, shards.
	tcpSrv.Stop()
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
