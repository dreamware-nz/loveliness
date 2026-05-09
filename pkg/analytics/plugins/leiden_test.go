package plugins

import (
	"context"
	"testing"

	"github.com/johnjansen/loveliness/pkg/router"
)

// edgeRow constructs a result row with src/dst columns.
func edgeRow(s, d any) map[string]any {
	return map[string]any{"src": s, "dst": d}
}

func TestLeiden_TwoTriangles(t *testing.T) {
	r := &router.Result{
		Columns: []string{"src", "dst"},
		Rows: []map[string]any{
			edgeRow("a", "b"), edgeRow("b", "c"), edgeRow("c", "a"),
			edgeRow("x", "y"), edgeRow("y", "z"), edgeRow("z", "x"),
		},
	}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"include_assignments": true,
		"seed":                float64(7),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_communities"].(int) != 2 {
		t.Errorf("num_communities: %v (full=%v)", got["num_communities"], got)
	}
	assigns := got["assignments"].(map[string]int)
	if assigns["a"] != assigns["b"] || assigns["b"] != assigns["c"] {
		t.Errorf("triangle 1 split: %v", assigns)
	}
	if assigns["x"] != assigns["y"] || assigns["y"] != assigns["z"] {
		t.Errorf("triangle 2 split: %v", assigns)
	}
	if assigns["a"] == assigns["x"] {
		t.Errorf("triangles should be different communities: %v", assigns)
	}
}

func TestLeiden_MissingSrcColumn(t *testing.T) {
	r := &router.Result{Columns: []string{"a", "dst"}, Rows: []map[string]any{}}
	if _, err := (Leiden{}).Compute(context.Background(), r, nil); err == nil {
		t.Fatal("expected error for missing src column")
	}
}

func TestLeiden_NegativeGammaRejected(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{"gamma": float64(-1)})
	if err == nil {
		t.Fatal("expected error for negative gamma")
	}
}

func TestLeiden_WeightedEdges(t *testing.T) {
	// Two near-cliques with a weak bridge. With weight=1 on bridge but
	// weight=10 on internal edges, the partition should clearly split.
	r := &router.Result{
		Columns: []string{"src", "dst", "w"},
		Rows: []map[string]any{
			{"src": "a", "dst": "b", "w": float64(10)},
			{"src": "b", "dst": "c", "w": float64(10)},
			{"src": "c", "dst": "a", "w": float64(10)},
			{"src": "d", "dst": "e", "w": float64(10)},
			{"src": "e", "dst": "f", "w": float64(10)},
			{"src": "f", "dst": "d", "w": float64(10)},
			{"src": "c", "dst": "d", "w": float64(1)}, // bridge
		},
	}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"weight":              "w",
		"include_assignments": true,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_communities"].(int) != 2 {
		t.Errorf("expected 2 communities with weighted bridge, got %v", got)
	}
}

func TestLeiden_Determinism(t *testing.T) {
	build := func() *router.Result {
		return &router.Result{
			Columns: []string{"src", "dst"},
			Rows: []map[string]any{
				edgeRow("a", "b"), edgeRow("b", "c"), edgeRow("c", "d"),
				edgeRow("d", "a"), edgeRow("a", "c"), edgeRow("e", "f"),
				edgeRow("f", "g"), edgeRow("g", "e"), edgeRow("d", "e"),
			},
		}
	}
	params := map[string]any{"seed": float64(42), "include_assignments": true}
	a, _ := Leiden{}.Compute(context.Background(), build(), params)
	b, _ := Leiden{}.Compute(context.Background(), build(), params)
	aa := a.(map[string]any)["assignments"].(map[string]int)
	bb := b.(map[string]any)["assignments"].(map[string]int)
	for k, v := range aa {
		if bb[k] != v {
			t.Errorf("non-deterministic at %q: %d vs %d", k, v, bb[k])
		}
	}
}

func TestLeiden_AssignmentsHiddenByDefault(t *testing.T) {
	r := &router.Result{
		Columns: []string{"src", "dst"},
		Rows:    []map[string]any{edgeRow("a", "b")},
	}
	out, err := Leiden{}.Compute(context.Background(), r, nil)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if _, present := got["assignments"]; present {
		t.Error("assignments should be omitted when not requested")
	}
}

func TestLeiden_EmptyResult(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: []map[string]any{}}
	out, err := Leiden{}.Compute(context.Background(), r, nil)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_communities"].(int) != 0 {
		t.Errorf("empty graph: num_communities=%v", got["num_communities"])
	}
}
