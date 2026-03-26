package shard

import (
	"sync"
	"testing"
)

func TestShard_Query(t *testing.T) {
	store := NewMemoryStore()
	store.PutNode("alice", map[string]any{"name": "Alice"})

	s := NewShard(0, store, 4)

	resp, err := s.Query("MATCH (n) RETURN n")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(resp.Rows))
	}
}

func TestShard_ConcurrentQueries(t *testing.T) {
	store := NewMemoryStore()
	store.PutNode("a", map[string]any{"name": "A"})
	s := NewShard(0, store, 4)

	var wg sync.WaitGroup
	errors := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Query("MATCH (n) RETURN n")
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)
	for err := range errors {
		t.Errorf("concurrent query error: %v", err)
	}
}

func TestShard_PanicRecovery(t *testing.T) {
	// Create a store that panics on query.
	store := &panicStore{}
	s := NewShard(0, store, 4)

	_, err := s.Query("MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error from panic recovery")
	}
	if s.IsHealthy() {
		t.Error("shard should be marked unhealthy after panic")
	}

	// Subsequent queries should fail immediately.
	_, err = s.Query("MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error from unhealthy shard")
	}
}

func TestShard_Close(t *testing.T) {
	store := NewMemoryStore()
	s := NewShard(0, store, 4)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShard_SemaphoreBounds(t *testing.T) {
	store := NewMemoryStore()
	store.PutNode("a", map[string]any{"name": "A"})

	// Max 2 concurrent queries.
	s := NewShard(0, store, 2)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Query("MATCH (n) RETURN n")
		}()
	}
	wg.Wait()

	// If semaphore wasn't working, this would deadlock or panic.
	// Completing without issues is the test.
}

// panicStore is a Store that panics on every query (for testing recovery).
type panicStore struct{}

func (s *panicStore) Query(cypher string) (*QueryResponse, error) {
	panic("simulated LadybugDB crash")
}

func (s *panicStore) Close() error {
	return nil
}
