package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// ReplicaState tracks the replication position for each replica of each shard.
type ReplicaState struct {
	mu    sync.RWMutex
	state map[replicaKey]uint64 // (shardID, nodeID) → last acked sequence
}

type replicaKey struct {
	shardID int
	nodeID  string
}

// NewReplicaState creates an empty replica state tracker.
func NewReplicaState() *ReplicaState {
	return &ReplicaState{state: make(map[replicaKey]uint64)}
}

// SetPosition records that a replica has caught up to a given sequence.
func (rs *ReplicaState) SetPosition(shardID int, nodeID string, seq uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	key := replicaKey{shardID, nodeID}
	if seq > rs.state[key] {
		rs.state[key] = seq
	}
}

// GetPosition returns the last acked sequence for a replica.
func (rs *ReplicaState) GetPosition(shardID int, nodeID string) uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state[replicaKey{shardID, nodeID}]
}

// Lag returns how far behind a replica is relative to the WAL head.
func (rs *ReplicaState) Lag(shardID int, nodeID string, walHead uint64) uint64 {
	pos := rs.GetPosition(shardID, nodeID)
	if walHead > pos {
		return walHead - pos
	}
	return 0
}

// CatchupManager handles replica catch-up from the WAL.
type CatchupManager struct {
	wal        *WAL
	state      *ReplicaState
	shards     []*shard.Shard
	interval   time.Duration
	maxBatch   int
}

// NewCatchupManager creates a catch-up manager that periodically replays
// missed WAL entries to local replica shards.
func NewCatchupManager(wal *WAL, state *ReplicaState, shards []*shard.Shard) *CatchupManager {
	return &CatchupManager{
		wal:      wal,
		state:    state,
		shards:   shards,
		interval: 1 * time.Second,
		maxBatch: 1000,
	}
}

// CatchupShard replays missed WAL entries for a shard on the local node.
// Returns the number of entries replayed.
func (cm *CatchupManager) CatchupShard(ctx context.Context, shardID int, nodeID string) (int, error) {
	lastAcked := cm.state.GetPosition(shardID, nodeID)
	entries, err := cm.wal.ReadFrom(shardID, lastAcked)
	if err != nil {
		return 0, fmt.Errorf("read WAL for shard %d from seq %d: %w", shardID, lastAcked, err)
	}
	if len(entries) == 0 {
		return 0, nil
	}

	// Limit batch size.
	if len(entries) > cm.maxBatch {
		entries = entries[:cm.maxBatch]
	}

	if shardID >= len(cm.shards) {
		return 0, fmt.Errorf("shard %d not available locally", shardID)
	}
	s := cm.shards[shardID]

	replayed := 0
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return replayed, ctx.Err()
		default:
		}

		if _, err := s.Query(entry.Cypher); err != nil {
			slog.Warn("catchup: replay failed",
				"shard", shardID, "seq", entry.Sequence, "err", err)
			// Continue — some queries may fail on replay (e.g., already applied).
			// Record position up to this point so we don't re-replay.
		}
		cm.state.SetPosition(shardID, nodeID, entry.Sequence)
		replayed++
	}

	slog.Info("catchup: replayed entries",
		"shard", shardID, "count", replayed, "from_seq", lastAcked,
		"to_seq", entries[len(entries)-1].Sequence)
	return replayed, nil
}

// Run starts the background catch-up loop for all shards on this node.
// It periodically checks for missed WAL entries and replays them.
func (cm *CatchupManager) Run(ctx context.Context, nodeID string, shardIDs []int) {
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, sid := range shardIDs {
				if _, err := cm.CatchupShard(ctx, sid, nodeID); err != nil {
					slog.Warn("catchup: error", "shard", sid, "err", err)
				}
			}
		}
	}
}
