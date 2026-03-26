package replication

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

// ReadPreference controls where read queries are routed.
type ReadPreference int

const (
	// ReadPrimary always reads from the primary (strongest consistency).
	ReadPrimary ReadPreference = iota
	// ReadPreferReplica reads from a replica if available, falls back to primary.
	ReadPreferReplica
	// ReadNearest reads from the node with lowest perceived latency.
	ReadNearest
)

// ParseReadPreference converts a string to a ReadPreference.
func ParseReadPreference(s string) ReadPreference {
	switch s {
	case "primary", "PRIMARY":
		return ReadPrimary
	case "replica", "REPLICA", "prefer_replica", "PREFER_REPLICA":
		return ReadPreferReplica
	case "nearest", "NEAREST":
		return ReadNearest
	default:
		return ReadPrimary
	}
}

func (rp ReadPreference) String() string {
	switch rp {
	case ReadPrimary:
		return "PRIMARY"
	case ReadPreferReplica:
		return "PREFER_REPLICA"
	case ReadNearest:
		return "NEAREST"
	default:
		return "UNKNOWN"
	}
}

// ReadRouter routes read queries to the appropriate node based on read preference,
// replica state, and shard ownership.
type ReadRouter struct {
	localShards []*shard.Shard
	client      *transport.Client
	state       *ReplicaState
	wal         *WAL
	maxLag      uint64 // max acceptable WAL lag for replica reads
}

// NewReadRouter creates a read router.
func NewReadRouter(
	localShards []*shard.Shard,
	client *transport.Client,
	state *ReplicaState,
	wal *WAL,
	maxLag uint64,
) *ReadRouter {
	return &ReadRouter{
		localShards: localShards,
		client:      client,
		state:       state,
		wal:         wal,
		maxLag:      maxLag,
	}
}

// QueryResult wraps a shard query response with routing metadata.
type QueryResult struct {
	Response *shard.QueryResponse
	FromNode string // "local" or the node ID
	IsStale  bool   // true if read from a replica that may be behind
}

// RouteRead executes a read query on the best available node for a shard.
func (rr *ReadRouter) RouteRead(
	ctx context.Context,
	shardID int,
	cypher string,
	pref ReadPreference,
	ownership ShardOwnership,
	localNodeID string,
) (*QueryResult, error) {
	switch pref {
	case ReadPrimary:
		return rr.readFromPrimary(ctx, shardID, cypher, ownership, localNodeID)

	case ReadPreferReplica:
		result, err := rr.readFromReplica(ctx, shardID, cypher, ownership, localNodeID)
		if err != nil {
			slog.Debug("replica read failed, falling back to primary",
				"shard", shardID, "err", err)
			return rr.readFromPrimary(ctx, shardID, cypher, ownership, localNodeID)
		}
		return result, nil

	case ReadNearest:
		// Try local first (whether primary or replica), then remote.
		if shardID < len(rr.localShards) {
			resp, err := rr.localShards[shardID].Query(cypher)
			if err == nil {
				isStale := ownership.Primary != localNodeID
				return &QueryResult{Response: resp, FromNode: "local", IsStale: isStale}, nil
			}
		}
		return rr.readFromPrimary(ctx, shardID, cypher, ownership, localNodeID)

	default:
		return rr.readFromPrimary(ctx, shardID, cypher, ownership, localNodeID)
	}
}

func (rr *ReadRouter) readFromPrimary(
	ctx context.Context,
	shardID int,
	cypher string,
	ownership ShardOwnership,
	localNodeID string,
) (*QueryResult, error) {
	if ownership.Primary == localNodeID && shardID < len(rr.localShards) {
		resp, err := rr.localShards[shardID].Query(cypher)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Response: resp, FromNode: "local"}, nil
	}

	// Forward to primary node.
	resp, err := rr.client.QueryRemote(ownership.Primary, shardID, cypher)
	if err != nil {
		return nil, fmt.Errorf("primary read on %s: %w", ownership.Primary, err)
	}
	return &QueryResult{
		Response: &shard.QueryResponse{
			Columns: resp.Columns,
			Rows:    resp.Rows,
		},
		FromNode: ownership.Primary,
	}, nil
}

func (rr *ReadRouter) readFromReplica(
	ctx context.Context,
	shardID int,
	cypher string,
	ownership ShardOwnership,
	localNodeID string,
) (*QueryResult, error) {
	if len(ownership.Replicas) == 0 {
		return nil, fmt.Errorf("no replicas for shard %d", shardID)
	}

	// Check WAL lag — only read from replicas that are caught up enough.
	walHead := rr.wal.ShardSequence(shardID)
	var eligible []string
	for _, nodeID := range ownership.Replicas {
		lag := rr.state.Lag(shardID, nodeID, walHead)
		if lag <= rr.maxLag {
			eligible = append(eligible, nodeID)
		}
	}

	if len(eligible) == 0 {
		return nil, fmt.Errorf("no replicas within max lag %d for shard %d", rr.maxLag, shardID)
	}

	// Pick a random eligible replica.
	target := eligible[rand.Intn(len(eligible))]

	// If the target is local, read directly.
	if target == localNodeID && shardID < len(rr.localShards) {
		resp, err := rr.localShards[shardID].Query(cypher)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Response: resp, FromNode: "local", IsStale: true}, nil
	}

	// Forward to replica node.
	resp, err := rr.client.QueryRemote(target, shardID, cypher)
	if err != nil {
		return nil, fmt.Errorf("replica read on %s: %w", target, err)
	}
	return &QueryResult{
		Response: &shard.QueryResponse{
			Columns: resp.Columns,
			Rows:    resp.Rows,
		},
		FromNode: target,
		IsStale:  true,
	}, nil
}
