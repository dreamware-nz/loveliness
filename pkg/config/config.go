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

	// AuthToken is the shared API token for HTTP and Bolt authentication.
	// Empty string disables authentication (dev mode).
	AuthToken string

	// TLS configuration.
	TLSCert       string // path to server certificate
	TLSKey        string // path to server private key
	TLSCA         string // path to CA certificate (for mTLS)
	TLSMode       string // "required", "optional", "off"
	TLSClientAuth string // "require", "request", "none"

	// ShardBufferMB is the buffer pool size in MB per shard.
	// 0 = auto-calculate: (total system memory * 0.7) / shard_count.
	// LadybugDB defaults to 80% of system memory PER shard, which OOMs with multiple shards.
	ShardBufferMB int

	// AllowAllShortestUnsafe opts the node into accepting `ALL SHORTEST`
	// path queries. The default (false) makes the router reject them
	// with an UNSAFE_QUERY error because the LadybugDB native layer
	// segfaults on this construct under load (see GitHub issue #1).
	// Operators who have isolated workers or simply accept the risk
	// can flip this to true.
	AllowAllShortestUnsafe bool

	// ReplicationFactor is the desired number of node copies per shard
	// when bootstrapping a new cluster's placement (primary + (rf-1)
	// replicas). 1 disables replication; 2 places one replica; etc.
	// Clamped at runtime to len(nodes) so a small cluster can still
	// bootstrap and surface the shortfall through observability.
	ReplicationFactor int

	// WriteConsistency controls how many replicas must acknowledge a
	// write before the primary returns success to the client.
	//   "one"    — primary-only ack; replicas catch up async (default).
	//   "quorum" — primary + ⌊RF/2⌋ replicas must apply.
	//   "all"    — every replica must apply.
	// Anything else is parsed as "quorum" by replication.ParseConsistency.
	WriteConsistency string

	// FailureDetectorEnabled controls whether the leader-only failure
	// detector pings peers and auto-promotes replicas on dead primaries.
	// Disable for tests or maintenance windows where flapping liveness
	// would cause unwanted shard moves.
	FailureDetectorEnabled bool

	// FailureDetectorIntervalMs is the gap between liveness pings.
	FailureDetectorIntervalMs int

	// FailureDetectorThreshold is the consecutive failed-ping count that
	// trips a peer to Alive=false in the shard map.
	FailureDetectorThreshold int

	// DNS discovery configuration.
	DiscoverMode     string // "dns" to enable DNS-based peer discovery, empty to disable
	DiscoverAddr     string // DNS name to resolve for peer discovery (e.g., "loveliness.internal")
	DiscoverInterval int    // interval in seconds between discovery attempts (default: 5)
	ExpectedNodes    int    // expected number of nodes for quorum-gated auto-bootstrap (0 = no expectation)
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
		TLSMode:              "off",
		TLSClientAuth:        "require",
		ReplicationFactor:    1,
		WriteConsistency:     "one",

		FailureDetectorEnabled:    true,
		FailureDetectorIntervalMs: 2000,
		FailureDetectorThreshold:  3,
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
	// Bolt address has bespoke handling: an explicit empty value
	// (LOVELINESS_BOLT_ADDR=) is the documented way to *disable* the
	// Bolt listener — useful in tests that don't want to fight the
	// fixed :7687 port. The other env vars treat unset and empty
	// the same way, but Bolt's default of ":7687" makes that
	// behaviour user-hostile here.
	if v, ok := os.LookupEnv("LOVELINESS_BOLT_ADDR"); ok {
		c.BoltAddr = v
	}
	if v := os.Getenv("LOVELINESS_AUTH_TOKEN"); v != "" {
		c.AuthToken = v
	}
	if v := os.Getenv("LOVELINESS_TLS_CERT"); v != "" {
		c.TLSCert = v
	}
	if v := os.Getenv("LOVELINESS_TLS_KEY"); v != "" {
		c.TLSKey = v
	}
	if v := os.Getenv("LOVELINESS_TLS_CA"); v != "" {
		c.TLSCA = v
	}
	if v := os.Getenv("LOVELINESS_TLS_MODE"); v != "" {
		c.TLSMode = v
	}
	if v := os.Getenv("LOVELINESS_TLS_CLIENT_AUTH"); v != "" {
		c.TLSClientAuth = v
	}
	if v := os.Getenv("LOVELINESS_DISCOVER"); v != "" {
		c.DiscoverMode = v
	}
	if v := os.Getenv("LOVELINESS_DISCOVER_ADDR"); v != "" {
		c.DiscoverAddr = v
	}
	if v := os.Getenv("LOVELINESS_DISCOVER_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.DiscoverInterval = n
		}
	}
	if v := os.Getenv("LOVELINESS_EXPECTED_NODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ExpectedNodes = n
		}
	}
	if v := os.Getenv("LOVELINESS_SHARD_BUFFER_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ShardBufferMB = n
		}
	}
	if v := os.Getenv("LOVELINESS_ALLOW_ALL_SHORTEST_UNSAFE"); v == "true" || v == "1" {
		c.AllowAllShortestUnsafe = true
	}
	if v := os.Getenv("LOVELINESS_REPLICATION_FACTOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.ReplicationFactor = n
		}
	}
	if v := os.Getenv("LOVELINESS_WRITE_CONSISTENCY"); v != "" {
		c.WriteConsistency = v
	}
	if v := os.Getenv("LOVELINESS_FAILURE_DETECTOR_ENABLED"); v != "" {
		c.FailureDetectorEnabled = v != "false" && v != "0"
	}
	if v := os.Getenv("LOVELINESS_FAILURE_DETECTOR_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.FailureDetectorIntervalMs = n
		}
	}
	if v := os.Getenv("LOVELINESS_FAILURE_DETECTOR_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.FailureDetectorThreshold = n
		}
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
