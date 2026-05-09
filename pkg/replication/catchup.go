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
	state map[replicaKey]replicaPos
}

type replicaKey struct {
	shardID int
	nodeID  string
}

type replicaPos struct {
	seq uint64    // last acked sequence
	ts  time.Time // timestamp of the WAL entry at `seq` (origin time, not ack time)
}

// NewReplicaState creates an empty replica state tracker.
func NewReplicaState() *ReplicaState {
	return &ReplicaState{state: make(map[replicaKey]replicaPos)}
}

// SetPosition records that a replica has caught up to a given sequence.
// The position only advances — older sequences are ignored.
func (rs *ReplicaState) SetPosition(shardID int, nodeID string, seq uint64) {
	rs.SetPositionAt(shardID, nodeID, seq, time.Time{})
}

// SetPositionAt records that a replica has caught up to a given sequence
// whose origin timestamp at the primary was ts. Used so callers can later
// derive time-lag without re-reading the WAL. Position only advances.
func (rs *ReplicaState) SetPositionAt(shardID int, nodeID string, seq uint64, ts time.Time) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	key := replicaKey{shardID, nodeID}
	cur := rs.state[key]
	if seq > cur.seq {
		rs.state[key] = replicaPos{seq: seq, ts: ts}
	}
}

// GetPosition returns the last acked sequence for a replica.
func (rs *ReplicaState) GetPosition(shardID int, nodeID string) uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state[replicaKey{shardID, nodeID}].seq
}

// GetTimestamp returns the WAL-origin timestamp of the replica's last
// applied entry. Zero time if none recorded.
func (rs *ReplicaState) GetTimestamp(shardID int, nodeID string) time.Time {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state[replicaKey{shardID, nodeID}].ts
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
		cm.state.SetPositionAt(shardID, nodeID, entry.Sequence, entry.Timestamp)
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
