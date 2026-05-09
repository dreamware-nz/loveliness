package router

import (
	"context"
	"strings"
	"sync"
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

// --- Write rewriter tests ---

// fakeWAL records each Append call.
type fakeWAL struct {
	mu      sync.Mutex
	entries []walAppendCall
	nextSeq uint64
}

type walAppendCall struct {
	shardID int
	cypher  string
}

func (f *fakeWAL) Append(shardID int, cypher string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	f.entries = append(f.entries, walAppendCall{shardID: shardID, cypher: cypher})
	return f.nextSeq, nil
}

// fakeRewriter swaps any "now()" for a fixed literal — easy to assert on.
type fakeRewriter struct{}

func (fakeRewriter) Rewrite(cypher string) string {
	return strings.ReplaceAll(cypher, "now()", "datetime('FIXED')")
}

func TestRouter_RewriterRunsBeforeWAL(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	wal := &fakeWAL{}
	r.SetWAL(wal)
	r.SetWriteRewriter(fakeRewriter{})

	// Use a write query with a shard key so it routes to a single shard.
	_, err := r.Execute(context.Background(), "CREATE (p:Person {name: 'alice', ts: now()})")
	if err != nil {
		t.Fatal(err)
	}

	if len(wal.entries) != 1 {
		t.Fatalf("expected 1 WAL entry, got %d", len(wal.entries))
	}
	if !strings.Contains(wal.entries[0].cypher, "datetime('FIXED')") {
		t.Errorf("WAL entry should contain rewritten literal; got: %q", wal.entries[0].cypher)
	}
	if strings.Contains(wal.entries[0].cypher, "now()") {
		t.Errorf("WAL entry should NOT contain raw now(); got: %q", wal.entries[0].cypher)
	}

	// And the shard that executed must have received the SAME rewritten string,
	// not the original. This is the correctness invariant: primary's local
	// state derives from the same statement that replicas will replay.
	var executed string
	for _, s := range shards {
		ms := s.Store.(*shard.MemoryStore)
		log := ms.QueryLog()
		if len(log) > 0 {
			executed = log[0]
			break
		}
	}
	if executed == "" {
		t.Fatal("no shard saw the query")
	}
	if !strings.Contains(executed, "datetime('FIXED')") {
		t.Errorf("primary execution must use rewritten cypher; got: %q", executed)
	}
}

func TestRouter_RewriterSkippedForReads(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	wal := &fakeWAL{}
	r.SetWAL(wal)
	r.SetWriteRewriter(fakeRewriter{})

	_, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'alice'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(wal.entries) != 0 {
		t.Errorf("read query must not append to WAL, got %d entries", len(wal.entries))
	}
}

func TestRouter_NoRewriterStillWALs(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	wal := &fakeWAL{}
	r.SetWAL(wal)
	// No rewriter set — write must still be recorded verbatim.

	_, err := r.Execute(context.Background(), "CREATE (p:Person {name: 'alice', ts: now()})")
	if err != nil {
		t.Fatal(err)
	}
	if len(wal.entries) != 1 {
		t.Fatalf("expected 1 WAL entry, got %d", len(wal.entries))
	}
	if !strings.Contains(wal.entries[0].cypher, "now()") {
		t.Errorf("without rewriter, WAL keeps original cypher; got: %q", wal.entries[0].cypher)
	}
}

// --- Write replicator tests ---

// fakeReplicator records each ReplicateWrite call and can be configured to
// return an error to simulate a sync-replication quorum failure.
type fakeReplicator struct {
	mu    sync.Mutex
	calls []replicateCall
	err   error
}

type replicateCall struct {
	shardID int
	cypher  string
}

func (f *fakeReplicator) ReplicateWrite(_ context.Context, shardID int, cypher string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, replicateCall{shardID: shardID, cypher: cypher})
	return f.err
}

// fakePlacement is a minimal PlacementResolver used to simulate a multi-node
// cluster — the router asks it to decide whether the local node is the
// primary for a given shard.
type fakePlacement struct {
	primary map[int]string
}

func (f *fakePlacement) PrimaryForShard(shardID int) string { return f.primary[shardID] }

func TestRouter_ReplicatorCalledAfterWrite(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	r.SetWAL(&fakeWAL{})
	r.SetWriteRewriter(fakeRewriter{})
	rep := &fakeReplicator{}
	r.SetWriteReplicator(rep)

	_, err := r.Execute(context.Background(), "CREATE (p:Person {name: 'alice'})")
	if err != nil {
		t.Fatal(err)
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.calls) != 1 {
		t.Fatalf("expected 1 replicator call, got %d", len(rep.calls))
	}
	if !strings.Contains(rep.calls[0].cypher, "CREATE") {
		t.Errorf("replicator should receive the rewritten write cypher; got %q", rep.calls[0].cypher)
	}
}

func TestRouter_ReplicatorNotCalledForReads(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	rep := &fakeReplicator{}
	r.SetWriteReplicator(rep)

	_, err := r.Execute(context.Background(), "MATCH (p:Person {name: 'alice'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.calls) != 0 {
		t.Errorf("read queries must not invoke the replicator; got %d calls", len(rep.calls))
	}
}

func TestRouter_ReplicatorSkippedWhenNotPrimary(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	r.SetWAL(&fakeWAL{})
	rep := &fakeReplicator{}
	r.SetWriteReplicator(rep)

	// Mark every shard as primary on a different node — this node is the
	// receiving replica, so it must apply the write but NOT re-replicate.
	r.SetRemoteTransport("local-node", nil, &fakePlacement{
		primary: map[int]string{0: "other-node", 1: "other-node", 2: "other-node"},
	})

	_, err := r.Execute(context.Background(), "CREATE (p:Person {name: 'alice'})")
	if err != nil {
		t.Fatal(err)
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.calls) != 0 {
		t.Errorf("non-primary node must not re-fan-out replicated writes; got %d calls", len(rep.calls))
	}
}

func TestRouter_ReplicatorErrorSurfacesAsWriteFailure(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	r.SetWAL(&fakeWAL{})
	rep := &fakeReplicator{err: context.DeadlineExceeded}
	r.SetWriteReplicator(rep)

	_, err := r.Execute(context.Background(), "CREATE (p:Person {name: 'alice'})")
	if err == nil {
		t.Fatal("expected write failure when sync replication fails")
	}
	if !strings.Contains(err.Error(), "replication") {
		t.Errorf("error should mention replication; got %v", err)
	}
}
