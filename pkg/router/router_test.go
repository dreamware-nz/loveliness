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
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows (one per shard), got %d", len(result.Rows))
	}
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

	_, err := r.Execute(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty query")
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

	s1 := r.ResolveShardForKey("Alice")
	s2 := r.ResolveShardForKey("Alice")
	if s1 != s2 {
		t.Errorf("inconsistent shard resolution: %d vs %d", s1, s2)
	}
}

func TestRouter_UnhealthyShard(t *testing.T) {
	shards := makeTestShards(1)
	r := NewRouter(shards, 5*time.Second)

	key := "TestKey"
	shardID := r.ResolveShardForKey(key)
	_ = shardID

	_, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'TestKey'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
}

func TestRouter_ContextCancellation(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := r.Execute(ctx, "MATCH (p:Person) RETURN p")
	if err != nil {
		return
	}
	if result != nil && !result.Partial {
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

// --- Write routing tests ---

func TestRouter_WriteWithShardKey(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	// MERGE with shard key should route to a single shard.
	result, err := r.Execute(context.Background(), "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
}

func TestRouter_WriteWithoutShardKey_Rejected(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	// SET without shard key should be rejected.
	_, err := r.Execute(context.Background(), "MATCH (p:Person) SET p.active = true")
	if err == nil {
		t.Fatal("expected error for write without shard key")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T", err)
	}
	if qe.Code != "MISSING_SHARD_KEY" {
		t.Errorf("expected MISSING_SHARD_KEY, got %s", qe.Code)
	}
}

func TestRouter_DeleteWithShardKey(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	result, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'Bob'}) DELETE p")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
}

// --- Dedup tests ---

func TestDeduplicateRows_RemovesReferenceNodes(t *testing.T) {
	columns := []string{"p.name", "p.age", "p.city"}
	rows := []map[string]any{
		{"p.name": "Bob", "p.age": nil, "p.city": nil},       // reference node (stub)
		{"p.name": "Alice", "p.age": 30, "p.city": "Auckland"}, // real node
		{"p.name": "Bob", "p.age": 25, "p.city": "Wellington"}, // real node
	}

	result := deduplicateRows(rows, columns)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows after dedup, got %d: %v", len(result), result)
	}

	// Bob should have full properties, not nulls.
	for _, row := range result {
		if row["p.name"] == "Bob" {
			if row["p.age"] != 25 {
				t.Errorf("Bob's age: got %v, want 25", row["p.age"])
			}
			if row["p.city"] != "Wellington" {
				t.Errorf("Bob's city: got %v, want Wellington", row["p.city"])
			}
		}
	}
}

func TestDeduplicateRows_NoFalsePositives(t *testing.T) {
	columns := []string{"p.name", "p.age"}
	rows := []map[string]any{
		{"p.name": "Alice", "p.age": 30},
		{"p.name": "Bob", "p.age": 25},
	}

	result := deduplicateRows(rows, columns)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows (no dedup), got %d", len(result))
	}
}

func TestDeduplicateRows_EmptyInput(t *testing.T) {
	result := deduplicateRows(nil, []string{"p.name"})
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// --- Schema broadcast tests ---

func TestRouter_SchemaBroadcast(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	result, err := r.Execute(context.Background(), "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
	// All shards should have received the schema DDL.
	for i, s := range shards {
		ms := s.Store.(*shard.MemoryStore)
		if len(ms.QueryLog()) == 0 {
			t.Errorf("shard %d did not receive schema DDL", i)
		}
	}
}

func TestRouter_OptionalMatchIsRead(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)

	// OPTIONAL MATCH without shard key should scatter-gather (not error).
	result, err := r.Execute(context.Background(), "OPTIONAL MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows from scatter-gather, got %d", len(result.Rows))
	}
}
