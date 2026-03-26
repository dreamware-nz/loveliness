package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/transport"
)

// Consistency defines how many replicas must acknowledge a write.
type Consistency int

const (
	// ConsistencyOne acknowledges after the primary write (fastest, small loss window).
	ConsistencyOne Consistency = iota
	// ConsistencyQuorum acknowledges after primary + at least 1 replica.
	ConsistencyQuorum
	// ConsistencyAll acknowledges after all replicas confirm.
	ConsistencyAll
)

// ParseConsistency converts a string to a Consistency level.
func ParseConsistency(s string) Consistency {
	switch s {
	case "one", "ONE", "1":
		return ConsistencyOne
	case "quorum", "QUORUM":
		return ConsistencyQuorum
	case "all", "ALL":
		return ConsistencyAll
	default:
		return ConsistencyQuorum // safe default
	}
}

func (c Consistency) String() string {
	switch c {
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// ShardOwnership describes who owns a shard and where replicas are.
type ShardOwnership struct {
	ShardID  int
	Primary  string
	Replicas []string // node IDs of replica holders
}

// ShardResolver looks up shard ownership from the cluster state.
type ShardResolver interface {
	GetShardOwner(shardID int) ShardOwnership
}

// Replicator handles write fan-out to replica shards after primary write.
type Replicator struct {
	client  *transport.Client
	timeout time.Duration
}

// NewReplicator creates a write replicator.
func NewReplicator(client *transport.Client, timeout time.Duration) *Replicator {
	return &Replicator{
		client:  client,
		timeout: timeout,
	}
}

// ReplicateResult contains the outcome of write replication.
type ReplicateResult struct {
	PrimaryOK  bool
	ReplicaACK int    // how many replicas acknowledged
	ReplicaErr []string // errors from failed replicas
}

// Replicate sends a write to replicas and waits according to the consistency level.
// The primary write has already been done. This handles replica fan-out.
func (r *Replicator) Replicate(ctx context.Context, ownership ShardOwnership, cypher string, consistency Consistency) *ReplicateResult {
	result := &ReplicateResult{PrimaryOK: true}

	if len(ownership.Replicas) == 0 || consistency == ConsistencyOne {
		// No replicas or ONE consistency — nothing to wait for.
		// Fire-and-forget replication if replicas exist.
		if len(ownership.Replicas) > 0 {
			go r.replicateAsync(ownership, cypher)
		}
		return result
	}

	// Determine how many acks we need.
	needed := 1 // at least 1 for QUORUM
	if consistency == ConsistencyAll {
		needed = len(ownership.Replicas)
	}

	// Fan out to replicas.
	type replicaResult struct {
		nodeID string
		err    error
	}

	ch := make(chan replicaResult, len(ownership.Replicas))
	replicaCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	for _, nodeID := range ownership.Replicas {
		go func(nodeID string) {
			_, err := r.client.QueryRemote(nodeID, ownership.ShardID, cypher)
			ch <- replicaResult{nodeID: nodeID, err: err}
		}(nodeID)
	}

	// Collect results until we have enough acks or timeout.
	acks := 0
	collected := 0
	for collected < len(ownership.Replicas) {
		select {
		case <-replicaCtx.Done():
			result.ReplicaErr = append(result.ReplicaErr, "replication timeout")
			return result
		case rr := <-ch:
			collected++
			if rr.err != nil {
				result.ReplicaErr = append(result.ReplicaErr, fmt.Sprintf("%s: %v", rr.nodeID, rr.err))
				slog.Warn("replica write failed", "node", rr.nodeID, "shard", ownership.ShardID, "err", rr.err)
			} else {
				acks++
				result.ReplicaACK = acks
			}
			if acks >= needed {
				return result
			}
		}
	}

	return result
}

// replicateAsync fires and forgets replication to all replicas.
func (r *Replicator) replicateAsync(ownership ShardOwnership, cypher string) {
	var wg sync.WaitGroup
	for _, nodeID := range ownership.Replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if _, err := r.client.QueryRemote(nodeID, ownership.ShardID, cypher); err != nil {
				slog.Warn("async replica write failed", "node", nodeID, "shard", ownership.ShardID, "err", err)
			}
		}(nodeID)
	}
	wg.Wait()
}
