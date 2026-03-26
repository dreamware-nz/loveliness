package shard

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Assignment mirrors cluster.ShardAssignment to avoid circular imports.
type Assignment struct {
	Primary string
	Replica string
}

// StoreFactory creates a Store for a given shard path.
type StoreFactory func(path string, maxThreads uint64) (Store, error)

// DefaultStoreFactory creates LbugStore instances.
func DefaultStoreFactory(path string, maxThreads uint64) (Store, error) {
	return NewLbugStore(path, maxThreads)
}

// Manager manages the lifecycle of local shards based on the cluster's shard map.
// It opens shards assigned to this node and closes shards that are reassigned away.
type Manager struct {
	mu            sync.RWMutex
	shards        map[int]*Shard // shard ID → local Shard
	nodeID        string
	dataDir       string
	maxThreads    uint64
	maxConcurrent int
	storeFactory  StoreFactory
}

// NewManager creates a shard manager for the given node.
func NewManager(nodeID, dataDir string, maxThreads uint64, maxConcurrent int) *Manager {
	return &Manager{
		shards:        make(map[int]*Shard),
		nodeID:        nodeID,
		dataDir:       dataDir,
		maxThreads:    maxThreads,
		maxConcurrent: maxConcurrent,
		storeFactory:  DefaultStoreFactory,
	}
}

// NewTestManager creates a manager that uses MemoryStore for testing.
func NewTestManager(nodeID string) *Manager {
	return &Manager{
		shards:        make(map[int]*Shard),
		nodeID:        nodeID,
		dataDir:       "",
		maxThreads:    1,
		maxConcurrent: 4,
		storeFactory: func(path string, maxThreads uint64) (Store, error) {
			return NewMemoryStore(), nil
		},
	}
}

// UpdateAssignments reconciles local shards with the cluster shard map.
// It opens shards newly assigned to this node and closes shards reassigned away.
func (m *Manager) UpdateAssignments(assignments map[int]Assignment) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Determine which shards this node should host.
	want := make(map[int]bool)
	for id, a := range assignments {
		if a.Primary == m.nodeID || a.Replica == m.nodeID {
			want[id] = true
		}
	}

	// Close shards no longer assigned to this node.
	for id, s := range m.shards {
		if !want[id] {
			slog.Info("closing shard", "shard", id, "reason", "reassigned")
			s.Close()
			delete(m.shards, id)
		}
	}

	// Open shards newly assigned to this node.
	for id := range want {
		if _, exists := m.shards[id]; exists {
			continue
		}
		if err := m.openShard(id); err != nil {
			slog.Error("failed to open shard", "shard", id, "err", err)
		}
	}
}

func (m *Manager) openShard(id int) error {
	var path string
	if m.dataDir != "" {
		// Only create the parent directory — LadybugDB must create
		// its own database directory or it fails with status 1.
		if err := os.MkdirAll(m.dataDir, 0755); err != nil {
			return fmt.Errorf("create data dir: %w", err)
		}
		path = filepath.Join(m.dataDir, fmt.Sprintf("shard-%d", id))
	}

	store, err := m.storeFactory(path, m.maxThreads)
	if err != nil {
		return fmt.Errorf("open store for shard %d: %w", id, err)
	}

	m.shards[id] = NewShard(id, store, m.maxConcurrent)
	slog.Info("opened shard", "shard", id, "node", m.nodeID)
	return nil
}

// GetShard returns the local shard for the given ID, or nil if not local.
func (m *Manager) GetShard(id int) *Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shards[id]
}

// GetLocalShards returns all locally opened shards.
func (m *Manager) GetLocalShards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		out = append(out, s)
	}
	return out
}

// LocalShardIDs returns the IDs of all locally opened shards.
func (m *Manager) LocalShardIDs() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]int, 0, len(m.shards))
	for id := range m.shards {
		out = append(out, id)
	}
	return out
}

// IsLocal returns true if this node hosts the given shard.
func (m *Manager) IsLocal(shardID int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.shards[shardID]
	return ok
}

// ShardCount returns the number of locally opened shards.
func (m *Manager) ShardCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.shards)
}

// Close shuts down all local shards.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for id, s := range m.shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		slog.Info("closed shard", "shard", id)
		delete(m.shards, id)
	}
	return firstErr
}
