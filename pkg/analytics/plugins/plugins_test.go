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
