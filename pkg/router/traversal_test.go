package router

import (
	"testing"
)

func TestExtractTraversal_Outgoing(t *testing.T) {
	info := extractTraversal("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name")
	if info == nil {
		t.Fatal("expected traversal info")
	}
	if info.SourceVar != "a" {
		t.Errorf("source var: got %q, want %q", info.SourceVar, "a")
	}
	if info.SourceLabel != "Person" {
		t.Errorf("source label: got %q, want %q", info.SourceLabel, "Person")
	}
	if info.RelType != "KNOWS" {
		t.Errorf("rel type: got %q, want %q", info.RelType, "KNOWS")
	}
	if info.TargetVar != "b" {
		t.Errorf("target var: got %q, want %q", info.TargetVar, "b")
	}
	if info.TargetLabel != "Person" {
		t.Errorf("target label: got %q, want %q", info.TargetLabel, "Person")
	}
}

func TestExtractTraversal_NoTargetLabel(t *testing.T) {
	info := extractTraversal("MATCH (a:Person)-[:FOLLOWS]->(b) RETURN b.name")
	if info == nil {
		t.Fatal("expected traversal info")
	}
	if info.TargetVar != "b" {
		t.Errorf("target var: got %q, want %q", info.TargetVar, "b")
	}
	if info.TargetLabel != "" {
		t.Errorf("target label: got %q, want empty", info.TargetLabel)
	}
	if info.RelType != "FOLLOWS" {
		t.Errorf("rel type: got %q, want %q", info.RelType, "FOLLOWS")
	}
}

func TestExtractTraversal_WithRelVariable(t *testing.T) {
	info := extractTraversal("MATCH (a:Person)-[r:KNOWS {since: 2024}]->(b:Person) RETURN b.name, r.since")
	if info == nil {
		t.Fatal("expected traversal info")
	}
	if info.RelType != "KNOWS" {
		t.Errorf("rel type: got %q, want %q", info.RelType, "KNOWS")
	}
}

func TestExtractTraversal_NoTraversal(t *testing.T) {
	info := extractTraversal("MATCH (p:Person {name: 'Alice'}) RETURN p")
	if info != nil {
		t.Errorf("expected nil, got %+v", info)
	}
}

func TestExtractReturnColumns(t *testing.T) {
	cols := extractReturnColumns("MATCH (a)-[:KNOWS]->(b) RETURN b.name, b.age, a.city ORDER BY b.name")
	if len(cols) != 2 {
		t.Fatalf("expected 2 variables, got %d", len(cols))
	}
	if len(cols["b"]) != 2 {
		t.Errorf("expected 2 columns for b, got %d", len(cols["b"]))
	}
	if len(cols["a"]) != 1 {
		t.Errorf("expected 1 column for a, got %d", len(cols["a"]))
	}
}

func TestAddToReturn(t *testing.T) {
	tests := []struct {
		cypher string
		col    string
		want   string
	}{
		{
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age",
			"b.name",
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age, b.name",
		},
		{
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age ORDER BY b.age",
			"b.name",
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age, b.name ORDER BY b.age",
		},
		{
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age LIMIT 10",
			"b.name",
			"MATCH (a)-[:KNOWS]->(b) RETURN b.age, b.name LIMIT 10",
		},
	}

	for _, tt := range tests {
		got := addToReturn(tt.cypher, tt.col)
		if got != tt.want {
			t.Errorf("addToReturn(%q, %q)\n  got:  %q\n  want: %q", tt.cypher, tt.col, got, tt.want)
		}
	}
}
