package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// EdgeCutReplicator manages full-property replication of border nodes.
// A "border node" is a node that has edges crossing shard boundaries.
// Instead of PK-only reference stubs, border nodes get full property
// replication — enabling the local shard to answer traversal queries
// without cross-shard resolution hops.
//
// Architecture:
//
//	Shard 0: Alice(full) ──KNOWS──> Bob(full replica)
//	Shard 1: Bob(full)   ──KNOWS──> Alice(full replica)
//
// When a traversal from Alice→Bob occurs, shard 0 has Bob's full
// properties locally — no need for a second hop to shard 1.
type EdgeCutReplicator struct {
	shards     []*shard.Shard
	shardCount int
	router     *Router

	// borderNodes tracks which nodes are replicated where.
	// borderNodes[shardID][table][keyValue] = true
	borderNodes map[int]map[string]map[string]bool
	mu          sync.RWMutex
}

// NewEdgeCutReplicator creates a replicator for border node management.
func NewEdgeCutReplicator(shards []*shard.Shard, r *Router) *EdgeCutReplicator {
	ecr := &EdgeCutReplicator{
		shards:      shards,
		shardCount:  len(shards),
		router:      r,
		borderNodes: make(map[int]map[string]map[string]bool),
	}
	for i := 0; i < len(shards); i++ {
		ecr.borderNodes[i] = make(map[string]map[string]bool)
	}
	return ecr
}

// ReplicateBorderNode copies full properties of a node from its owning shard
// to a target shard. This upgrades a PK-only reference to a full replica.
func (ecr *EdgeCutReplicator) ReplicateBorderNode(ctx context.Context, table, shardKeyProp, keyValue string, targetShardID int) error {
	ownerShardID := ecr.router.resolveShard(keyValue)
	if ownerShardID == targetShardID {
		return nil // already on correct shard
	}

	// Fetch full properties from the owning shard.
	escaped := strings.ReplaceAll(keyValue, "'", "\\'")
	fetchCypher := fmt.Sprintf("MATCH (n:%s {%s: '%s'}) RETURN n", table, shardKeyProp, escaped)
	resp, err := ecr.shards[ownerShardID].Query(fetchCypher)
	if err != nil {
		return fmt.Errorf("fetch from owner shard %d: %w", ownerShardID, err)
	}
	if len(resp.Rows) == 0 {
		return fmt.Errorf("node %s=%s not found on owner shard %d", shardKeyProp, keyValue, ownerShardID)
	}

	// Build a MERGE + SET to upsert full properties on the target shard.
	props := resp.Rows[0]
	setClauses := buildSetClauses("n", props, shardKeyProp)

	var cypher string
	if len(setClauses) > 0 {
		cypher = fmt.Sprintf("MERGE (n:%s {%s: '%s'}) SET %s",
			table, shardKeyProp, escaped, strings.Join(setClauses, ", "))
	} else {
		cypher = fmt.Sprintf("MERGE (n:%s {%s: '%s'})", table, shardKeyProp, escaped)
	}

	if _, err := ecr.shards[targetShardID].Query(cypher); err != nil {
		return fmt.Errorf("replicate to shard %d: %w", targetShardID, err)
	}

	// Track the replication.
	ecr.mu.Lock()
	if ecr.borderNodes[targetShardID][table] == nil {
		ecr.borderNodes[targetShardID][table] = make(map[string]bool)
	}
	ecr.borderNodes[targetShardID][table][keyValue] = true
	ecr.mu.Unlock()

	return nil
}

// RefreshBorderNodes re-syncs all border node replicas on a given shard
// with their owning shards. Called periodically or after bulk writes.
func (ecr *EdgeCutReplicator) RefreshBorderNodes(ctx context.Context, shardID int, shardKeyProp string) (updated int, errs []error) {
	ecr.mu.RLock()
	tables := ecr.borderNodes[shardID]
	ecr.mu.RUnlock()

	for table, keys := range tables {
		for keyValue := range keys {
			if err := ecr.ReplicateBorderNode(ctx, table, shardKeyProp, keyValue, shardID); err != nil {
				errs = append(errs, err)
			} else {
				updated++
			}
		}
	}
	return updated, errs
}

// RefreshAll refreshes border nodes across all shards.
func (ecr *EdgeCutReplicator) RefreshAll(ctx context.Context, shardKeyProp string) {
	var wg sync.WaitGroup
	for i := 0; i < ecr.shardCount; i++ {
		wg.Add(1)
		go func(sid int) {
			defer wg.Done()
			updated, errs := ecr.RefreshBorderNodes(ctx, sid, shardKeyProp)
			if len(errs) > 0 {
				slog.Warn("border node refresh errors", "shard", sid, "errors", len(errs))
			}
			if updated > 0 {
				slog.Info("border nodes refreshed", "shard", sid, "updated", updated)
			}
		}(i)
	}
	wg.Wait()
}

// IsBorderNode returns true if a node is replicated on the given shard.
func (ecr *EdgeCutReplicator) IsBorderNode(shardID int, table, keyValue string) bool {
	ecr.mu.RLock()
	defer ecr.mu.RUnlock()
	if ecr.borderNodes[shardID][table] == nil {
		return false
	}
	return ecr.borderNodes[shardID][table][keyValue]
}

// BorderNodeCount returns the number of replicated border nodes per shard.
func (ecr *EdgeCutReplicator) BorderNodeCount() map[int]int {
	ecr.mu.RLock()
	defer ecr.mu.RUnlock()
	counts := make(map[int]int, ecr.shardCount)
	for sid, tables := range ecr.borderNodes {
		for _, keys := range tables {
			counts[sid] += len(keys)
		}
	}
	return counts
}

// buildSetClauses creates SET clause fragments from a property map.
// Skips the shard key property (it's in the MERGE pattern) and internal props.
func buildSetClauses(variable string, props map[string]any, skipProp string) []string {
	var clauses []string
	for key, val := range props {
		// Handle nested node properties like "n.age" or just "age".
		prop := key
		if dotIdx := strings.LastIndexByte(key, '.'); dotIdx != -1 {
			prop = key[dotIdx+1:]
		}
		// Skip internal columns and the shard key (already in MERGE).
		if strings.HasPrefix(prop, "_") {
			continue
		}
		if strings.EqualFold(prop, skipProp) {
			continue
		}
		if val == nil {
			continue
		}

		clause := fmt.Sprintf("%s.%s = %s", variable, prop, cypherLiteral(val))
		clauses = append(clauses, clause)
	}
	return clauses
}

// cypherLiteral converts a Go value to a Cypher literal string.
func cypherLiteral(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "\\'"))
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%f", val)
	case float32:
		return fmt.Sprintf("%f", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}
