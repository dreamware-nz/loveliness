package plugins

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

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

// twoCliquesPlusBridge: 8-node graph used as a multi-resolution probe.
// Two 4-cliques joined by a single weak edge — at low γ everything
// merges, at γ≈1 we see the 2-clique partition, and at high γ the
// cliques shatter.
func twoCliquesPlusBridge() *router.Result {
	rows := []map[string]any{}
	// clique A on {a0..a3}
	for _, e := range [][2]string{{"a0", "a1"}, {"a0", "a2"}, {"a0", "a3"}, {"a1", "a2"}, {"a1", "a3"}, {"a2", "a3"}} {
		rows = append(rows, edgeRow(e[0], e[1]))
	}
	// clique B on {b0..b3}
	for _, e := range [][2]string{{"b0", "b1"}, {"b0", "b2"}, {"b0", "b3"}, {"b1", "b2"}, {"b1", "b3"}, {"b2", "b3"}} {
		rows = append(rows, edgeRow(e[0], e[1]))
	}
	rows = append(rows, edgeRow("a3", "b0")) // weak bridge
	return &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
}

// TestLeiden_GammaSweep_Shape: passing `gammas` returns a partitions
// list of the right length, with one entry per γ, in order.
func TestLeiden_GammaSweep_Shape(t *testing.T) {
	gammas := []any{float64(0.5), float64(1.0), float64(2.0)}
	out, err := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gammas": gammas,
		"seed":   float64(11),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["num_nodes"].(int) != 8 {
		t.Errorf("num_nodes: %v", got["num_nodes"])
	}
	if _, ok := got["partitions"]; !ok {
		t.Fatalf("expected partitions key, got %v", got)
	}
	parts := got["partitions"].([]map[string]any)
	if len(parts) != len(gammas) {
		t.Fatalf("expected %d partitions, got %d", len(gammas), len(parts))
	}
	for i, p := range parts {
		want := gammas[i].(float64)
		if p["gamma"].(float64) != want {
			t.Errorf("partition[%d]: gamma=%v, want %v", i, p["gamma"], want)
		}
		// Required per-partition fields.
		for _, k := range []string{"num_communities", "modularity", "iterations", "size_histogram"} {
			if _, ok := p[k]; !ok {
				t.Errorf("partition[%d] missing %q (%v)", i, k, p)
			}
		}
		// Sweep mode without include_assignments must omit them.
		if _, present := p["assignments"]; present {
			t.Errorf("partition[%d]: assignments should be omitted when not requested", i)
		}
	}
}

// TestLeiden_GammaSweep_ResolvesByGamma: the same graph at γ=0.1
// (very coarse) should yield ≤ communities than at γ=5 (very fine).
// Specifically: at γ=5 the 4-cliques shatter (≥4 communities); at
// γ=0.1 the bridge wins and everything collapses (1 community).
func TestLeiden_GammaSweep_ResolvesByGamma(t *testing.T) {
	out, err := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gammas": []any{float64(0.1), float64(5.0)},
		"seed":   float64(3),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	parts := out.(map[string]any)["partitions"].([]map[string]any)
	low := parts[0]["num_communities"].(int)
	high := parts[1]["num_communities"].(int)
	if low > high {
		t.Errorf("low γ should yield ≤ communities than high γ, got %d vs %d", low, high)
	}
	if low != 1 {
		t.Errorf("γ=0.1 should collapse to 1 community, got %d", low)
	}
	if high < 8 {
		t.Errorf("γ=5 should fragment into at least 8 communities (singletons), got %d", high)
	}
}

// TestLeiden_GammaSweep_Determinism: same gammas + seed → same per-γ
// partitions. Locks that parallel goroutine execution doesn't introduce
// nondeterminism (each Run owns its RNG seeded from the shared seed).
func TestLeiden_GammaSweep_Determinism(t *testing.T) {
	params := map[string]any{
		"gammas":              []any{float64(0.5), float64(1.0), float64(2.0)},
		"seed":                float64(42),
		"include_assignments": true,
	}
	a, _ := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), params)
	b, _ := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), params)
	aps := a.(map[string]any)["partitions"].([]map[string]any)
	bps := b.(map[string]any)["partitions"].([]map[string]any)
	if len(aps) != len(bps) {
		t.Fatalf("partition count differs: %d vs %d", len(aps), len(bps))
	}
	for i := range aps {
		aa := aps[i]["assignments"].(map[string]int)
		bb := bps[i]["assignments"].(map[string]int)
		for k, v := range aa {
			if bb[k] != v {
				t.Errorf("partition[%d] non-deterministic at %q: %d vs %d", i, k, v, bb[k])
			}
		}
	}
}

// TestLeiden_GammaSweep_AssignmentsPerPartition: include_assignments
// applies to every entry in the sweep, not just one.
func TestLeiden_GammaSweep_AssignmentsPerPartition(t *testing.T) {
	out, err := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gammas":              []any{float64(0.5), float64(1.0)},
		"include_assignments": true,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	parts := out.(map[string]any)["partitions"].([]map[string]any)
	for i, p := range parts {
		a, ok := p["assignments"].(map[string]int)
		if !ok {
			t.Fatalf("partition[%d]: missing assignments map", i)
		}
		if len(a) != 8 {
			t.Errorf("partition[%d]: expected 8 assignments, got %d", i, len(a))
		}
	}
}

// TestLeiden_GammaSweep_RejectsBoth: passing both gamma and gammas is
// ambiguous and must error rather than silently picking one.
func TestLeiden_GammaSweep_RejectsBoth(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"gamma":  float64(1),
		"gammas": []any{float64(0.5), float64(1.0)},
	})
	if err == nil {
		t.Fatal("expected error when both gamma and gammas are set")
	}
}

// TestLeiden_GammaSweep_RejectsEmpty: an empty gammas list is
// nonsensical and must error.
func TestLeiden_GammaSweep_RejectsEmpty(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"gammas": []any{},
	})
	if err == nil {
		t.Fatal("expected error for empty gammas")
	}
}

// TestLeiden_GammaSweep_RejectsNegative: any negative γ in the list
// invalidates the whole sweep.
func TestLeiden_GammaSweep_RejectsNegative(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"gammas": []any{float64(0.5), float64(-1)},
	})
	if err == nil {
		t.Fatal("expected error for negative gamma in sweep")
	}
}

// TestLeiden_GammaSweep_RejectsBadType: gammas must be a list, not a
// scalar or other shape.
func TestLeiden_GammaSweep_RejectsBadType(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	_, err := Leiden{}.Compute(context.Background(), r, map[string]any{
		"gammas": float64(1.0),
	})
	if err == nil {
		t.Fatal("expected error for scalar gammas")
	}
}

// TestLeiden_RejectsNaNGamma: NaN must error (otherwise it silently
// produces an identity partition because NaN comparisons are false).
func TestLeiden_RejectsNaNGamma(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	if _, err := (Leiden{}).Compute(context.Background(), r, map[string]any{
		"gamma": math.NaN(),
	}); err == nil {
		t.Fatal("expected error for NaN gamma")
	}
	if _, err := (Leiden{}).Compute(context.Background(), r, map[string]any{
		"gammas": []any{math.NaN()},
	}); err == nil {
		t.Fatal("expected error for NaN in gammas")
	}
}

// TestLeiden_RejectsInfGamma: both ±Inf must error in either path.
// Covered as a table to ensure both signs hit both single and sweep
// modes (validateGamma is shared, but the test's job is to lock that
// in).
func TestLeiden_RejectsInfGamma(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	cases := []struct {
		name   string
		params map[string]any
	}{
		{"single +Inf", map[string]any{"gamma": math.Inf(1)}},
		{"single -Inf", map[string]any{"gamma": math.Inf(-1)}},
		{"sweep +Inf", map[string]any{"gammas": []any{float64(1.0), math.Inf(1)}}},
		{"sweep -Inf", map[string]any{"gammas": []any{math.Inf(-1)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := (Leiden{}).Compute(context.Background(), r, c.params); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

// TestLeiden_GammaSweep_SingleElement: a one-element gammas list still
// routes to sweep mode and returns a partitions array (length 1) — this
// keeps the response shape stable for callers iterating over partitions
// regardless of how many γ values they passed.
func TestLeiden_GammaSweep_SingleElement(t *testing.T) {
	out, err := Leiden{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gammas": []any{float64(1.0)},
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	parts, ok := got["partitions"].([]map[string]any)
	if !ok {
		t.Fatalf("expected partitions array, got %T (%v)", got["partitions"], got)
	}
	if len(parts) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(parts))
	}
	if parts[0]["gamma"].(float64) != 1.0 {
		t.Errorf("partition[0].gamma = %v, want 1.0", parts[0]["gamma"])
	}
	// Single-element sweep must NOT also produce the flat single-mode keys.
	if _, present := got["num_communities"]; present {
		t.Errorf("sweep response must not carry top-level num_communities, got %v", got)
	}
}

// TestLeiden_GammaSweep_HonorsCancelledContext: a context already
// cancelled at entry must error out without running any Leiden work.
// Exercises the entry-guard ctx check.
func TestLeiden_GammaSweep_HonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	_, err := Leiden{}.Compute(ctx, twoCliquesPlusBridge(), map[string]any{
		"gammas": []any{float64(0.5), float64(1.0), float64(2.0)},
	})
	if err == nil {
		t.Fatal("expected error from pre-cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestLeiden_GammaSweep_CancelMidSweep: cancelling ctx while a sweep is
// in flight must (a) eventually return ctx.Err() and (b) not leak
// goroutines or race on partitions[i] writes. The wg.Wait() drain in
// the cancellation branch is what we're really exercising — combined
// with -race it locks the goroutine-cleanup contract.
//
// We use a deeply parallel sweep (32 γ values) on a non-trivial graph
// and cancel after a short delay. Because sweepConcurrency caps to
// NumCPU(), most γs are queued behind the semaphore — cancellation
// short-circuits those, and any in-flight workers finish naturally
// before wg.Wait returns.
func TestLeiden_GammaSweep_CancelMidSweep(t *testing.T) {
	r := twoCliquesPlusBridge()
	gammas := make([]any, 32)
	for i := range gammas {
		gammas[i] = float64(0.5 + 0.1*float64(i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel almost immediately; the exact moment doesn't matter —
		// we just need ctx to fire while the sweep is mid-loop.
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	_, err := Leiden{}.Compute(ctx, r, map[string]any{
		"gammas": gammas,
		"seed":   float64(1),
	})

	// Either: cancellation hit mid-loop and we got context.Canceled,
	// or all 32 γs finished before cancel fired (ultra-fast machine)
	// and we got a normal result. Both are valid; the test's job is to
	// exercise the cancellation path race-free, not to force it to
	// always win.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("expected nil or context.Canceled, got %v", err)
	}
}
