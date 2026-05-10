package plugins

import (
	"context"
	"testing"

	"github.com/johnjansen/loveliness/pkg/router"
)

func TestCountByLabel_Compute(t *testing.T) {
	r := &router.Result{
		Columns: []string{"label"},
		Rows: []map[string]any{
			{"label": "User"}, {"label": "User"}, {"label": "User"},
			{"label": "Post"}, {"label": "Post"},
			{"label": "Tag"},
		},
	}
	out, err := CountByLabel{}.Compute(context.Background(), r, map[string]any{"column": "label"})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	counts := got["counts"].(map[string]int)
	if counts["User"] != 3 || counts["Post"] != 2 || counts["Tag"] != 1 {
		t.Errorf("counts: %+v", counts)
	}
	if got["total"].(int) != 6 {
		t.Errorf("total: %v", got["total"])
	}
}

func TestCountByLabel_MissingParam(t *testing.T) {
	r := &router.Result{Columns: []string{"x"}, Rows: []map[string]any{{"x": 1}}}
	if _, err := (CountByLabel{}).Compute(context.Background(), r, nil); err == nil {
		t.Fatal("expected error on missing column param")
	}
}

func TestCountByLabel_UnknownColumn(t *testing.T) {
	r := &router.Result{Columns: []string{"label"}, Rows: []map[string]any{{"label": "x"}}}
	if _, err := (CountByLabel{}).Compute(context.Background(), r, map[string]any{"column": "nope"}); err == nil {
		t.Fatal("expected error on unknown column")
	}
}

func TestConnectedComponents_TwoTriangles(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "x", "dst": "y"}, {"src": "y", "dst": "z"}, {"src": "z", "dst": "x"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := ConnectedComponents{}.Compute(context.Background(), r, nil)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_components"].(int) != 2 {
		t.Errorf("num_components: %v", got["num_components"])
	}
	if got["num_nodes"].(int) != 6 {
		t.Errorf("num_nodes: %v", got["num_nodes"])
	}
	if got["largest"].(int) != 3 {
		t.Errorf("largest: %v", got["largest"])
	}
}

func TestConnectedComponents_CustomColumns(t *testing.T) {
	rows := []map[string]any{
		{"from": 1, "to": 2}, {"from": 2, "to": 3},
	}
	r := &router.Result{Columns: []string{"from", "to"}, Rows: rows}
	out, err := ConnectedComponents{}.Compute(
		context.Background(), r,
		map[string]any{"src": "from", "dst": "to"},
	)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_components"].(int) != 1 {
		t.Errorf("num_components: want 1, got %v", got["num_components"])
	}
	if got["num_nodes"].(int) != 3 {
		t.Errorf("num_nodes: want 3, got %v", got["num_nodes"])
	}
}

func TestConnectedComponents_MissingColumn(t *testing.T) {
	r := &router.Result{Columns: []string{"a"}, Rows: []map[string]any{{"a": 1}}}
	if _, err := (ConnectedComponents{}).Compute(context.Background(), r, nil); err == nil {
		t.Fatal("expected error when src column missing")
	}
}

// TestLeiden_Hierarchical_TwoCliques verifies that hierarchical mode
// on two cliques connected by a bridge recovers the cliques at level 1.
func TestLeiden_Hierarchical_TwoCliques(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "d", "dst": "e"}, {"src": "e", "dst": "f"}, {"src": "f", "dst": "d"},
		{"src": "c", "dst": "d"}, // bridge
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 3, "gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	levels, ok := got["levels"].([]map[string]any)
	if !ok {
		t.Fatalf("expected levels array, got %T", got["levels"])
	}
	if len(levels) < 1 {
		t.Fatal("expected at least 1 level")
	}
	// Level 1 should find 2 communities (the two cliques).
	l1 := levels[0]
	if l1["num_communities"].(int) != 2 {
		t.Errorf("level 1: expected 2 communities, got %v", l1["num_communities"])
	}
}

// TestLeiden_Hierarchical_Depth1_matchesSingleGamma verifies that
// depth=1 in hierarchical mode produces the same result as single-gamma mode.
func TestLeiden_Hierarchical_Depth1_matchesSingleGamma(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "d", "dst": "e"}, {"src": "e", "dst": "f"}, {"src": "f", "dst": "d"},
		{"src": "c", "dst": "d"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}

	// Single-gamma result.
	singleOut, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("single-gamma compute: %v", err)
	}

	// Hierarchical depth=1 result.
	hierOut, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 1, "gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("hierarchical compute: %v", err)
	}

	single := singleOut.(map[string]any)
	hier := hierOut.(map[string]any)
	levels := hier["levels"].([]map[string]any)

	if single["num_communities"] != levels[0]["num_communities"] {
		t.Errorf("num_communities mismatch: single=%v hier_level0=%v",
			single["num_communities"], levels[0]["num_communities"])
	}
	if single["modularity"] != levels[0]["modularity"] {
		t.Errorf("modularity mismatch: single=%v hier_level0=%v",
			single["modularity"], levels[0]["modularity"])
	}
}

// TestLeiden_Hierarchical_Depth_configurable verifies depth param works.
func TestLeiden_Hierarchical_Depth_configurable(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "d", "dst": "e"}, {"src": "e", "dst": "f"}, {"src": "f", "dst": "d"},
		{"src": "c", "dst": "d"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 5, "gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	levels := got["levels"].([]map[string]any)
	// May stop early if communities collapse to 1.
	if len(levels) < 1 {
		t.Fatal("expected at least 1 level")
	}
	// depth=5 should produce at most 5 levels.
	if len(levels) > 5 {
		t.Errorf("expected ≤5 levels, got %d", len(levels))
	}
}

// TestLeiden_Hierarchical_IncludesAssignments verifies include_assignments
// produces assignments on every level.
func TestLeiden_Hierarchical_IncludesAssignments(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "d", "dst": "e"}, {"src": "e", "dst": "f"}, {"src": "f", "dst": "d"},
		{"src": "c", "dst": "d"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 3, "gamma": 1.0, "seed": 42,
		"include_assignments": true,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	levels := got["levels"].([]map[string]any)
	for i, level := range levels {
		_, ok := level["assignments"].(map[string]int)
		if !ok {
			t.Errorf("level %d: expected assignments map, got %T", i, level["assignments"])
		}
	}
}

// TestLeiden_Hierarchical_ExcludesAssignments verifies assignments are
// omitted by default.
func TestLeiden_Hierarchical_ExcludesAssignments(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "d", "dst": "e"}, {"src": "e", "dst": "f"}, {"src": "f", "dst": "d"},
		{"src": "c", "dst": "d"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 3, "gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	levels := got["levels"].([]map[string]any)
	for i, level := range levels {
		if _, ok := level["assignments"]; ok {
			t.Errorf("level %d: expected no assignments, got present", i)
		}
	}
}

// TestLeiden_Hierarchical_IncompatibleWithSweep verifies that hierarchical
// mode returns an error when combined with gammas sweep.
func TestLeiden_Hierarchical_IncompatibleWithSweep(t *testing.T) {
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "gammas": []float64{0.5, 1.0},
	})
	if err == nil {
		t.Fatal("expected error when hierarchical + gammas sweep combined")
	}
}

// TestLeiden_Hierarchical_EarlyTermination verifies that a graph that
// collapses to a single community stops early.
func TestLeiden_Hierarchical_EarlyTermination(t *testing.T) {
	// A single clique should collapse to one community at level 1.
	rows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "a", "dst": "c"}, {"src": "b", "dst": "c"},
	}
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	out, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"hierarchical": true, "depth": 5, "gamma": 1.0, "seed": 42,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	levels := got["levels"].([]map[string]any)
	// Should have exactly 1 level (the clique is one community).
	if len(levels) != 1 {
		t.Errorf("expected 1 level for a clique, got %d", len(levels))
	}
	if levels[0]["num_communities"].(int) != 1 {
		t.Errorf("level 0: expected 1 community for clique, got %v", levels[0]["num_communities"])
	}
}
