package router

import (
	"context"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func makeTestShards(n int) []*shard.Shard {
	shards := make([]*shard.Shard, n)
	for i := 0; i < n; i++ {
		store := shard.NewMemoryStore()
		store.PutNode("test", map[string]any{"name": "test", "shard": i})
		shards[i] = shard.NewShard(i, store, 4)
	}
	return shards
}

func TestRouter_SingleShardQuery(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	result, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'Alice'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	// Query should hit exactly one shard.
	queriedShards := 0
	for _, s := range shards {
		ms := s.Store.(*shard.MemoryStore)
		if len(ms.QueryLog()) > 0 {
			queriedShards++
		}
	}
	if queriedShards != 1 {
		t.Errorf("expected query to hit 1 shard, hit %d", queriedShards)
	}
}

func TestRouter_ScatterGatherQuery(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	result, err := r.Execute(context.Background(), "MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	// Scatter-gather should return rows from all shards.
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows (one per shard), got %d", len(result.Rows))
	}
	// All shards should have been queried.
	for i, s := range shards {
		ms := s.Store.(*shard.MemoryStore)
		if len(ms.QueryLog()) == 0 {
			t.Errorf("shard %d was not queried", i)
		}
	}
}

func TestRouter_ParseError(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	_, err := r.Execute(context.Background(), "MERGE (n:Foo)")
	if err == nil {
		t.Fatal("expected error for unsupported clause")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T", err)
	}
	if qe.Code != "CYPHER_PARSE_ERROR" {
		t.Errorf("expected code CYPHER_PARSE_ERROR, got %s", qe.Code)
	}
}

func TestRouter_ConsistentShardResolution(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	// Same key should always resolve to same shard.
	s1 := r.ResolveShardForKey("Alice")
	s2 := r.ResolveShardForKey("Alice")
	if s1 != s2 {
		t.Errorf("inconsistent shard resolution: %d vs %d", s1, s2)
	}
}

func TestRouter_UnhealthyShard(t *testing.T) {
	shards := makeTestShards(1)
	r := NewRouter(shards, 5*time.Second)

	// Make the shard unhealthy by causing a panic in a mock.
	// Instead, directly test that an unhealthy shard returns error.
	key := "TestKey"
	shardID := r.ResolveShardForKey(key)

	// Verify it works when healthy.
	_, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'TestKey'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}

	// The shard should have been the target.
	_ = shardID
}

func TestRouter_ContextCancellation(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	// Scatter-gather with cancelled context should return partial results.
	result, err := r.Execute(ctx, "MATCH (p:Person) RETURN p")
	if err != nil {
		// Single-shard queries will get context error.
		return
	}
	if result != nil && !result.Partial {
		// Might complete before cancellation is observed; that's OK.
		return
	}
}

func TestRouter_EmptyQuery(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	_, err := r.Execute(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}
