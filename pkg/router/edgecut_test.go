package router

import (
	"testing"
)

func TestBuildSetClauses(t *testing.T) {
	props := map[string]any{
		"n.name": "Alice",
		"n.age":  float64(30),
		"n.city": "Auckland",
	}
	clauses := buildSetClauses("n", props, "name")
	// Should skip "name" (shard key) and include age and city.
	if len(clauses) != 2 {
		t.Errorf("expected 2 clauses, got %d: %v", len(clauses), clauses)
	}
}

func TestBuildSetClauses_SkipsNilAndInternal(t *testing.T) {
	props := map[string]any{
		"n.name":  "Bob",
		"n.age":   nil,
		"n._ID":   int64(42),
		"n.city":  "Wellington",
	}
	clauses := buildSetClauses("n", props, "name")
	// Should skip: name (shard key), age (nil), _ID (internal).
	if len(clauses) != 1 {
		t.Errorf("expected 1 clause (city), got %d: %v", len(clauses), clauses)
	}
}

func TestCypherLiteral(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{"hello", "'hello'"},
		{"it's", "'it\\'s'"},
		{float64(42), "42"},
		{float64(3.14), "3.140000"},
		{int64(99), "99"},
		{true, "true"},
		{false, "false"},
	}
	for _, tc := range tests {
		got := cypherLiteral(tc.input)
		if got != tc.expected {
			t.Errorf("cypherLiteral(%v) = %s, want %s", tc.input, got, tc.expected)
		}
	}
}

func TestEdgeCutReplicator_BorderNodeTracking(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5e9)
	ecr := NewEdgeCutReplicator(shards, r)

	if ecr.IsBorderNode(0, "Person", "Alice") {
		t.Error("expected Alice not to be a border node initially")
	}

	counts := ecr.BorderNodeCount()
	for sid, c := range counts {
		if c != 0 {
			t.Errorf("shard %d: expected 0 border nodes, got %d", sid, c)
		}
	}
}
