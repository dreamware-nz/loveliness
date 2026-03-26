package router

import (
	"testing"
)

func TestParse_EmptyQuery(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestParse_UnsupportedClause(t *testing.T) {
	clauses := []string{
		"MERGE (n:Person {name: 'Alice'})",
		"MATCH (n) SET n.age = 30",
		"MATCH (n) DELETE n",
		"MATCH (n) WITH n RETURN n",
		"UNWIND [1,2,3] AS x RETURN x",
		"CALL db.labels() YIELD label",
		"OPTIONAL MATCH (n) RETURN n",
	}
	for _, q := range clauses {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("expected error for unsupported clause: %s", q)
		}
	}
}

func TestParse_UnsupportedStatement(t *testing.T) {
	_, err := Parse("DROP TABLE foo")
	if err == nil {
		t.Fatal("expected error for unsupported statement")
	}
}

func TestParse_MatchWithWhereEquality(t *testing.T) {
	pq, err := Parse("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryMatch {
		t.Errorf("expected QueryMatch, got %d", pq.Type)
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
	if pq.Type != QueryCreate {
		t.Errorf("expected QueryCreate, got %d", pq.Type)
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
	// Same value in both inline prop and WHERE.
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
