package replication

import (
	"context"
	"fmt"
	"strings"
)

// RouterReplicator adapts the existing fan-out Replicator to the simple
// (shardID, cypher) call signature the router uses. It owns the consistency
// level and the ShardResolver so the router never has to know about either.
type RouterReplicator struct {
	repl        *Replicator
	resolver    ShardResolver
	consistency Consistency
}

// NewRouterReplicator wires a Replicator + ShardResolver + Consistency level
// into something the router can call as `WriteReplicator`. The resolver is
// queried per-write so primary/replica reassignments take effect immediately.
func NewRouterReplicator(repl *Replicator, resolver ShardResolver, consistency Consistency) *RouterReplicator {
	return &RouterReplicator{
		repl:        repl,
		resolver:    resolver,
		consistency: consistency,
	}
}

// ReplicateWrite fans `cypher` out to the shard's replicas. For
// ConsistencyOne this returns nil immediately and the underlying call
// fires-and-forgets. For Quorum / All it blocks until enough replicas
// have acknowledged or the inner replicator's timeout elapses; if the
// required ack count is not reached, a non-nil error is returned and the
// router surfaces it to the client as a write failure.
func (rr *RouterReplicator) ReplicateWrite(ctx context.Context, shardID int, cypher string) error {
	if rr.repl == nil || rr.resolver == nil {
		return nil
	}
	ownership := rr.resolver.GetShardOwner(shardID)
	if len(ownership.Replicas) == 0 {
		return nil
	}
	result := rr.repl.Replicate(ctx, ownership, cypher, rr.consistency)
	if rr.consistency == ConsistencyOne {
		return nil
	}
	needed := 1
	if rr.consistency == ConsistencyAll {
		needed = len(ownership.Replicas)
	}
	if result.ReplicaACK < needed {
		return fmt.Errorf("consistency=%s wanted %d acks, got %d (%s)",
			rr.consistency, needed, result.ReplicaACK, strings.Join(result.ReplicaErr, "; "))
	}
	return nil
}
