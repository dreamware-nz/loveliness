package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds the configuration for a Loveliness node.
type Config struct {
	// NodeID is the unique identifier for this node in the cluster.
	NodeID string
	// BindAddr is the address this node listens on for HTTP client requests.
	BindAddr string
	// RaftAddr is the address used for Raft consensus communication between nodes.
	RaftAddr string
	// GRPCAddr is the address used for internal gRPC between nodes (write forwarding, scatter-gather).
	GRPCAddr string
	// DataDir is the base directory for all shard data and Raft state.
	DataDir string
	// Peers is the list of other nodes in the cluster (RaftAddr values).
	Peers []string
	// ShardCount is the total number of shards across the cluster.
	ShardCount int
	// Bootstrap indicates whether this node should bootstrap a new cluster.
	Bootstrap bool
	// MaxConcurrentQueries is the max concurrent CGo calls per shard.
	MaxConcurrentQueries int
	// QueryTimeoutMs is the per-shard query timeout in milliseconds.
	QueryTimeoutMs uint64

	// S3 backup configuration.
	S3Bucket   string
	S3Region   string
	S3Prefix   string
	S3Endpoint string // custom endpoint for MinIO/R2/etc.

	// BackupIntervalMin is the interval in minutes between scheduled backups.
	// Zero disables scheduled backups.
	BackupIntervalMin int
	// BackupRetention is the number of backups to retain (oldest are pruned).
	BackupRetention int
	// BackupDir is the local directory for backup archives (used when S3 is not configured).
	BackupDir string

	// BoltAddr is the address for the Neo4j Bolt protocol listener.
	// Empty string disables the Bolt server.
	BoltAddr string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		NodeID:               "node-1",
		BindAddr:             ":8080",
		RaftAddr:             ":9000",
		GRPCAddr:             ":9001",
		DataDir:              "./data",
		ShardCount:           3,
		Bootstrap:            false,
		MaxConcurrentQueries: 16,
		QueryTimeoutMs:       30000,
		BackupRetention:      3,
		BoltAddr:             ":7687",
	}
}

// FromEnv populates a Config from environment variables.
func FromEnv() Config {
	c := DefaultConfig()
	if v := os.Getenv("LOVELINESS_NODE_ID"); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv("LOVELINESS_BIND_ADDR"); v != "" {
		c.BindAddr = v
	}
	if v := os.Getenv("LOVELINESS_RAFT_ADDR"); v != "" {
		c.RaftAddr = v
	}
	if v := os.Getenv("LOVELINESS_GRPC_ADDR"); v != "" {
		c.GRPCAddr = v
	}
	if v := os.Getenv("LOVELINESS_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("LOVELINESS_PEERS"); v != "" {
		c.Peers = strings.Split(v, ",")
	}
	if v := os.Getenv("LOVELINESS_SHARD_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ShardCount = n
		}
	}
	if v := os.Getenv("LOVELINESS_BOOTSTRAP"); v == "true" || v == "1" {
		c.Bootstrap = true
	}
	if v := os.Getenv("LOVELINESS_MAX_CONCURRENT_QUERIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MaxConcurrentQueries = n
		}
	}
	if v := os.Getenv("LOVELINESS_QUERY_TIMEOUT_MS"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			c.QueryTimeoutMs = n
		}
	}
	if v := os.Getenv("LOVELINESS_S3_BUCKET"); v != "" {
		c.S3Bucket = v
	}
	if v := os.Getenv("LOVELINESS_S3_REGION"); v != "" {
		c.S3Region = v
	}
	if v := os.Getenv("LOVELINESS_S3_PREFIX"); v != "" {
		c.S3Prefix = v
	}
	if v := os.Getenv("LOVELINESS_S3_ENDPOINT"); v != "" {
		c.S3Endpoint = v
	}
	if v := os.Getenv("LOVELINESS_BACKUP_INTERVAL_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.BackupIntervalMin = n
		}
	}
	if v := os.Getenv("LOVELINESS_BACKUP_RETENTION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.BackupRetention = n
		}
	}
	if v := os.Getenv("LOVELINESS_BACKUP_DIR"); v != "" {
		c.BackupDir = v
	}
	if v := os.Getenv("LOVELINESS_BOLT_ADDR"); v != "" {
		c.BoltAddr = v
	}
	return c
}

// Validate checks that the Config has all required fields.
func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if c.BindAddr == "" {
		return fmt.Errorf("bind_addr is required")
	}
	if c.RaftAddr == "" {
		return fmt.Errorf("raft_addr is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.ShardCount < 1 {
		return fmt.Errorf("shard_count must be >= 1")
	}
	if c.MaxConcurrentQueries < 1 {
		return fmt.Errorf("max_concurrent_queries must be >= 1")
	}
	return nil
}
