package router

import (
	"testing"

	"github.com/johnjansen/loveliness/pkg/schema"
)

func TestParse_EmptyQuery(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestParse_MatchWithWhereEquality(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryRead {
		t.Errorf("expected QueryRead, got %d", pq.Type)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Alice" {
		t.Errorf("expected shard key [Alice], got %v", pq.ShardKeys)
	}
	if pq.NeedsScatterGather() {
		t.Error("should not need scatter-gather")
	}
}

func TestParse_MatchWithInlineProperty(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Bob'}) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Bob" {
		t.Errorf("expected shard key [Bob], got %v", pq.ShardKeys)
	}
}

func TestParse_MatchWithDoubleQuotes(t *testing.T) {
	pq, err := Parse(`MATCH (p:Person) WHERE p.name = "Charlie" RETURN p`)
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Charlie" {
		t.Errorf("expected shard key [Charlie], got %v", pq.ShardKeys)
	}
}

func TestParse_MatchWithoutFilter_ScatterGather(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if !pq.NeedsScatterGather() {
		t.Error("should need scatter-gather with no filter")
	}
}

func TestParse_MatchWithMultipleConditions(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Alice' AND p.city = 'Auckland' RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 2 {
		t.Errorf("expected 2 shard keys, got %d: %v", len(pq.ShardKeys), pq.ShardKeys)
	}
}

func TestParse_CreateNode(t *testing.T) {
	pq, err := Parse("CREATE (p:Person {name: 'Dave', age: 30})")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Dave" {
		t.Errorf("expected shard key [Dave], got %v", pq.ShardKeys)
	}
}

func TestParse_Relationship(t *testing.T) {
	pq, err := Parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Alice" {
		t.Errorf("expected shard key [Alice], got %v", pq.ShardKeys)
	}
}

func TestParse_DeduplicatesKeys(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Alice'}) WHERE p.name = 'Alice' RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 {
		t.Errorf("expected 1 deduplicated shard key, got %d: %v", len(pq.ShardKeys), pq.ShardKeys)
	}
}

func TestParse_PreservesRawQuery(t *testing.T) {
	raw := "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p LIMIT 10"
	pq, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pq.Raw != raw {
		t.Errorf("expected raw query preserved, got %q", pq.Raw)
	}
}

func TestParse_WhereWithReturnTerminator(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Eve' RETURN p.name, p.age")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Eve" {
		t.Errorf("expected [Eve], got %v", pq.ShardKeys)
	}
}

func TestParse_WhereWithOR(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Alice' OR p.name = 'Bob' RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 2 {
		t.Errorf("expected 2 shard keys, got %d: %v", len(pq.ShardKeys), pq.ShardKeys)
	}
}

// --- New tests for expanded dialect ---

func TestParse_MergeIsWrite(t *testing.T) {
	pq, err := Parse("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Alice" {
		t.Errorf("expected shard key [Alice], got %v", pq.ShardKeys)
	}
}

func TestParse_MatchSetIsWrite(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Bob'}) SET p.age = 30 RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Bob" {
		t.Errorf("expected shard key [Bob], got %v", pq.ShardKeys)
	}
}

func TestParse_MatchDeleteIsWrite(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Charlie'}) DELETE p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
}

func TestParse_DetachDeleteIsWrite(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Dave'}) DETACH DELETE p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
}

func TestParse_RemoveIsWrite(t *testing.T) {
	pq, err := Parse("MATCH (p:Person {name: 'Eve'}) REMOVE p.age RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
}

func TestParse_OptionalMatchIsRead(t *testing.T) {
	pq, err := Parse("OPTIONAL MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryRead {
		t.Errorf("expected QueryRead, got %d", pq.Type)
	}
}

func TestParse_WithClauseIsRead(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WITH p.name AS name RETURN name")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryRead {
		t.Errorf("expected QueryRead, got %d", pq.Type)
	}
}

func TestParse_UnwindIsRead(t *testing.T) {
	pq, err := Parse("UNWIND [1,2,3] AS x RETURN x")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryRead {
		t.Errorf("expected QueryRead, got %d", pq.Type)
	}
}

func TestParse_CallIsRead(t *testing.T) {
	pq, err := Parse("CALL db.labels() YIELD label RETURN label")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryRead {
		t.Errorf("expected QueryRead, got %d", pq.Type)
	}
}

func TestParse_CreateNodeTableIsSchema(t *testing.T) {
	pq, err := Parse("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QuerySchema {
		t.Errorf("expected QuerySchema, got %d", pq.Type)
	}
}

func TestParse_CreateRelTableIsSchema(t *testing.T) {
	pq, err := Parse("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QuerySchema {
		t.Errorf("expected QuerySchema, got %d", pq.Type)
	}
}

func TestParse_DropTableIsSchema(t *testing.T) {
	pq, err := Parse("DROP TABLE Person")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QuerySchema {
		t.Errorf("expected QuerySchema, got %d", pq.Type)
	}
}

func TestParse_WhereBeforeSetTerminator(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 30")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryWrite {
		t.Errorf("expected QueryWrite, got %d", pq.Type)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Alice" {
		t.Errorf("expected [Alice], got %v", pq.ShardKeys)
	}
}

// --- Schema-aware shard key tests ---

func TestParseWithSchema_UsesShardKeyOnly(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	// city is NOT the shard key — should be ignored.
	pq, err := ParseWithSchema("MATCH (p:Person {city: 'Auckland', name: 'Alice'}) RETURN p", reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Alice" {
		t.Errorf("expected shard key [Alice] (only name, not city), got %v", pq.ShardKeys)
	}
}

func TestParseWithSchema_WhereShardKey(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	pq, err := ParseWithSchema("MATCH (p:Person) WHERE p.name = 'Bob' AND p.age = 30 RETURN p", reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "Bob" {
		t.Errorf("expected [Bob], got %v", pq.ShardKeys)
	}
}

func TestParseWithSchema_UnknownTableFallsBack(t *testing.T) {
	reg := schema.NewRegistry()
	// No tables registered — should fall back to legacy extraction.

	pq, err := ParseWithSchema("MATCH (p:Unknown {foo: 'bar'}) RETURN p", reg)
	if err != nil {
		t.Fatal(err)
	}
	// Legacy fallback grabs all string literals.
	if len(pq.ShardKeys) != 1 || pq.ShardKeys[0] != "bar" {
		t.Errorf("expected fallback key [bar], got %v", pq.ShardKeys)
	}
}

func TestParseWithSchema_CreateNodeTableRegisters(t *testing.T) {
	reg := schema.NewRegistry()

	_, err := ParseWithSchema("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))", reg)
	if err != nil {
		t.Fatal(err)
	}

	// The registry should now have Person registered.
	if key := reg.GetShardKey("Person"); key != "name" {
		t.Errorf("expected Person shard key 'name', got %q", key)
	}
}

func TestParseWithSchema_DropTableUnregisters(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	_, err := ParseWithSchema("DROP TABLE Person", reg)
	if err != nil {
		t.Fatal(err)
	}

	if key := reg.GetShardKey("Person"); key != "" {
		t.Errorf("expected Person removed from registry, got %q", key)
	}
}

func TestParseWithSchema_WrongPropertyScatterGathers(t *testing.T) {
	reg := schema.NewRegistry()
	reg.Register("Person", "name")

	// Query has city but not name — should NOT route based on city.
	// Person is a registered table, so no legacy fallback. Scatter-gather instead.
	pq, err := ParseWithSchema("MATCH (p:Person {city: 'Auckland'}) RETURN p", reg)
	if err != nil {
		t.Fatal(err)
	}
	if !pq.NeedsScatterGather() {
		t.Errorf("expected scatter-gather when shard key property is missing, got keys %v", pq.ShardKeys)
	}
}
