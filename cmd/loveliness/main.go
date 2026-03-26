package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/johnjansen/loveliness/pkg/api"
	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/config"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	log.Printf("loveliness node %s starting", cfg.NodeID)
	log.Printf("  bind=%s raft=%s data=%s shards=%d",
		cfg.BindAddr, cfg.RaftAddr, cfg.DataDir, cfg.ShardCount)

	// Calculate threads per shard to avoid CPU oversubscription.
	threadsPerShard := uint64(runtime.NumCPU()) / uint64(cfg.ShardCount)
	if threadsPerShard < 1 {
		threadsPerShard = 1
	}

	// Open local shards.
	// In a real multi-node cluster, each node only opens the shards assigned to it.
	// For the PoC, each node opens all shards (they'll be assigned via Raft after bootstrap).
	shards := make([]*shard.Shard, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		shardDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			log.Fatalf("create shard dir %s: %v", shardDir, err)
		}

		store, err := shard.NewLbugStore(shardDir, threadsPerShard)
		if err != nil {
			log.Fatalf("open shard %d: %v", i, err)
		}
		shards[i] = shard.NewShard(i, store, cfg.MaxConcurrentQueries)
		log.Printf("  shard %d opened at %s (threads=%d)", i, shardDir, threadsPerShard)
	}

	// Start Raft cluster.
	c, err := cluster.New(cfg.NodeID, cfg.RaftAddr, cfg.DataDir, cfg.Bootstrap)
	if err != nil {
		log.Fatalf("start cluster: %v", err)
	}
	log.Printf("  raft started (bootstrap=%v)", cfg.Bootstrap)

	// Create query router.
	queryTimeout := time.Duration(cfg.QueryTimeoutMs) * time.Millisecond
	r := router.NewRouter(shards, queryTimeout)

	// Start HTTP server.
	srv := api.NewServer(r, c, queryTimeout)
	httpServer := &http.Server{
		Addr:         cfg.BindAddr,
		Handler:      srv.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: queryTimeout + 5*time.Second,
	}

	go func() {
		log.Printf("  http listening on %s", cfg.BindAddr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	// Graceful shutdown.
	if err := httpServer.Close(); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	if err := c.Shutdown(); err != nil {
		log.Printf("raft shutdown error: %v", err)
	}
	for _, s := range shards {
		if err := s.Close(); err != nil {
			log.Printf("shard %d close error: %v", s.ID, err)
		}
	}
	log.Printf("loveliness node %s stopped", cfg.NodeID)
}
