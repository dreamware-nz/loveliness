package router

import (
	"context"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupResolverTest(shardCount int) (*Router, *schema.Registry) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	shards := make([]*shard.Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = shard.NewShard(i, shard.NewMemoryStore(), 4)
	}
	r := NewRouterWithSchema(shards, 5*time.Second, reg)
	return r, reg
}

func TestBuildResolveQuery(t *testing.T) {
	got := buildResolveQuery("Person", "name", "Bob", "b", []string{"name", "age", "city"})
	want := "MATCH (b:Person {name: 'Bob'}) RETURN b.name, b.age, b.city"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestBuildResolveQuery_NoProps(t *testing.T) {
	got := buildResolveQuery("Person", "name", "Bob", "b", nil)
	want := "MATCH (b:Person {name: 'Bob'}) RETURN b"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestBuildResolveQuery_EscapesQuotes(t *testing.T) {
	got := buildResolveQuery("Person", "name", "O'Brien", "b", []string{"name"})
	want := "MATCH (b:Person {name: 'O\\'Brien'}) RETURN b.name"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestIsCrossShard(t *testing.T) {
	r, _ := setupResolverTest(4)

	// Same key hashes to same shard.
	if r.isCrossShard([]string{"Alice", "Alice"}) {
		t.Error("same key should not be cross-shard")
	}

	// Single key is never cross-shard.
	if r.isCrossShard([]string{"Alice"}) {
		t.Error("single key should not be cross-shard")
	}
}

func TestResolverCreated(t *testing.T) {
	r, _ := setupResolverTest(4)
	if r.resolver == nil {
		t.Fatal("resolver should be set when using NewRouterWithSchema")
	}
}

func TestTraversalDetectedInParsedQuery(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	parsed, err := ParseWithSchema("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name", reg)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if parsed.Traversal == nil {
		t.Fatal("expected traversal info to be set")
	}
	if parsed.Traversal.SourceLabel != "Person" {
		t.Errorf("source label: got %q, want Person", parsed.Traversal.SourceLabel)
	}
	if parsed.Traversal.TargetLabel != "Person" {
		t.Errorf("target label: got %q, want Person", parsed.Traversal.TargetLabel)
	}
	if parsed.Traversal.RelType != "KNOWS" {
		t.Errorf("rel type: got %q, want KNOWS", parsed.Traversal.RelType)
	}
}

func TestEnsureReferenceNode(t *testing.T) {
	r, _ := setupResolverTest(4)
	ctx := context.Background()

	// First call should create.
	err := r.resolver.EnsureReferenceNode(ctx, 0, "Person", "name", "Bob")
	if err != nil {
		t.Fatalf("ensure reference: %v", err)
	}

	// Second call should be a no-op (already exists).
	err = r.resolver.EnsureReferenceNode(ctx, 0, "Person", "name", "Bob")
	if err != nil {
		t.Fatalf("ensure reference (idempotent): %v", err)
	}
}

func TestDedupeStrings(t *testing.T) {
	got := dedupeStrings([]string{"a", "b", "a", "c", "b"})
	if len(got) != 3 {
		t.Errorf("expected 3 unique, got %d: %v", len(got), got)
	}
}
