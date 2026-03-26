package router

import (
	"context"
	"testing"
	"time"
)

func TestPipelineExecutor_ScatterAll(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	pe := NewPipelineExecutor(shards, r, 5*time.Second)

	rows, cols, errs := pe.scatterAll(context.Background(), "MATCH (n:test) RETURN n.name")
	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows (one per shard), got %d", len(rows))
	}
	if len(cols) == 0 {
		t.Error("expected columns")
	}
}

func TestPipelineExecutor_Execute_NoHops(t *testing.T) {
	shards := makeTestShards(3)
	r := NewRouter(shards, 5*time.Second)
	pe := NewPipelineExecutor(shards, r, 5*time.Second)

	result, err := pe.Execute(
		context.Background(),
		"MATCH (n:test) RETURN n.name",
		func(row map[string]any) string {
			if v, ok := row["name"]; ok {
				return v.(string)
			}
			return ""
		},
		nil, // no hops
		"name",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestEscapeForCypher(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{"Alice", "Alice"},
		{"it's", "it\\'s"},
		{"no'quotes'here", "no\\'quotes\\'here"},
	}
	for _, tc := range tests {
		got := escapeForCypher(tc.input)
		if got != tc.expected {
			t.Errorf("escapeForCypher(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}
