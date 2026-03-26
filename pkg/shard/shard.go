package shard

import (
	"fmt"
	"sync"
)

// Shard wraps a Store with concurrency control (semaphore) and crash recovery.
//
//	                ┌──────────────┐
//	  Query() ────►│  semaphore   │──► SafeQuery() ──► Store.Query()
//	                │  (bounded)   │
//	                └──────────────┘
type Shard struct {
	ID        int
	Store     Store
	semaphore chan struct{}
	healthy   bool
	mu        sync.RWMutex
}

// NewShard creates a Shard wrapping the given Store with bounded concurrency.
func NewShard(id int, store Store, maxConcurrent int) *Shard {
	sem := make(chan struct{}, maxConcurrent)
	return &Shard{
		ID:        id,
		Store:     store,
		semaphore: sem,
		healthy:   true,
	}
}

// IsHealthy returns whether this shard is currently healthy.
func (s *Shard) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

func (s *Shard) markUnhealthy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = false
}

// Query executes a Cypher query with concurrency control and panic recovery.
func (s *Shard) Query(cypher string) (resp *QueryResponse, err error) {
	if !s.IsHealthy() {
		return nil, fmt.Errorf("shard %d is unhealthy", s.ID)
	}

	// Acquire semaphore slot.
	s.semaphore <- struct{}{}
	defer func() { <-s.semaphore }()

	// Recover from panics in CGo calls.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SHARD_CRASH: shard %d panicked: %v", s.ID, r)
			s.markUnhealthy()
		}
	}()

	return s.Store.Query(cypher)
}

// Close releases all resources held by the shard.
func (s *Shard) Close() error {
	return s.Store.Close()
}
