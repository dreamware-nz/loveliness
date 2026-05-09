package plugins

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
)

// TestResolutionPlateau_TwoCliquesYieldsPlateaus: a two-clique-with-bridge
// fixture should produce at least two plateaus across a wide γ range — a
// low-γ "all together" plateau, a mid-γ "two clusters" plateau, and a
// high-γ "all singletons" plateau.
func TestResolutionPlateau_TwoCliquesYieldsPlateaus(t *testing.T) {
	out, err := ResolutionPlateau{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gamma_min":   float64(0.05),
		"gamma_max":   float64(5.0),
		"gamma_steps": float64(20),
		"seed":        float64(7),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	plateaus := got["plateaus"].([]map[string]any)
	if len(plateaus) < 2 {
		t.Errorf("expected at least 2 plateaus across [0.05, 5.0], got %d (%+v)", len(plateaus), plateaus)
	}
	// Plateau community counts should be monotonically non-decreasing
	// in γ — coarser at low γ, finer at high γ.
	for i := 1; i < len(plateaus); i++ {
		prev := plateaus[i-1]["num_communities"].(int)
		cur := plateaus[i]["num_communities"].(int)
		if cur < prev {
			t.Errorf("plateau community count decreased: %d → %d (%+v)", prev, cur, plateaus)
		}
	}
}

// TestResolutionPlateau_ExplicitGammas: passing an explicit `gammas`
// list overrides the default range and is sorted internally for plateau
// detection.
func TestResolutionPlateau_ExplicitGammas(t *testing.T) {
	out, err := ResolutionPlateau{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		// Intentionally out-of-order — the plugin must sort.
		"gammas": []any{float64(2.0), float64(0.5), float64(1.0)},
		"seed":   float64(3),
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	plateaus := got["plateaus"].([]map[string]any)
	if len(plateaus) == 0 {
		t.Fatalf("expected ≥1 plateau, got none")
	}
	// Plateau ranges must be in ascending γ order.
	for i := 1; i < len(plateaus); i++ {
		prev := plateaus[i-1]["gamma_max"].(float64)
		cur := plateaus[i]["gamma_min"].(float64)
		if cur <= prev {
			t.Errorf("plateaus[%d].gamma_min (%v) ≤ plateaus[%d].gamma_max (%v)", i, cur, i-1, prev)
		}
	}
}

// TestResolutionPlateau_IncludePartitions: the per-γ summaries are
// hidden by default and surfaced when include_partitions=true.
func TestResolutionPlateau_IncludePartitions(t *testing.T) {
	r := twoCliquesPlusBridge()

	a, err := ResolutionPlateau{}.Compute(context.Background(), r, map[string]any{
		"gammas": []any{float64(0.5), float64(1.0), float64(2.0)},
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if _, present := a.(map[string]any)["partitions"]; present {
		t.Error("partitions should be hidden by default")
	}

	b, err := ResolutionPlateau{}.Compute(context.Background(), r, map[string]any{
		"gammas":             []any{float64(0.5), float64(1.0), float64(2.0)},
		"include_partitions": true,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	parts, ok := b.(map[string]any)["partitions"].([]map[string]any)
	if !ok {
		t.Fatalf("partitions missing or wrong type: %v", b)
	}
	if len(parts) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(parts))
	}
}

// TestResolutionPlateau_RepresentativePartition: include_assignments=true
// puts a representative partition map on every plateau.
func TestResolutionPlateau_RepresentativePartition(t *testing.T) {
	out, err := ResolutionPlateau{}.Compute(context.Background(), twoCliquesPlusBridge(), map[string]any{
		"gammas":              []any{float64(0.5), float64(1.0), float64(2.0)},
		"include_assignments": true,
	})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	plateaus := out.(map[string]any)["plateaus"].([]map[string]any)
	for i, p := range plateaus {
		rep, ok := p["representative_partition"].(map[string]int)
		if !ok {
			t.Errorf("plateau[%d] missing representative_partition: %v", i, p)
			continue
		}
		if len(rep) != 8 {
			t.Errorf("plateau[%d] representative_partition has %d nodes, want 8", i, len(rep))
		}
	}
}

// TestResolutionPlateau_RejectsBadThreshold: NMI thresholds must be in [0,1].
func TestResolutionPlateau_RejectsBadThreshold(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	for _, bad := range []float64{-0.1, 1.1, 2.0, math.NaN()} {
		_, err := ResolutionPlateau{}.Compute(context.Background(), r, map[string]any{
			"gammas":        []any{float64(1.0)},
			"nmi_threshold": bad,
		})
		if err == nil {
			t.Errorf("expected error for nmi_threshold=%v", bad)
		}
	}
}

// TestResolutionPlateau_RejectsBadGammaRange: gamma_max ≤ gamma_min,
// gamma_steps < 2, and NaN/Inf bounds must all error.
func TestResolutionPlateau_RejectsBadGammaRange(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	cases := []struct {
		name   string
		params map[string]any
	}{
		{"max ≤ min", map[string]any{"gamma_min": float64(2), "gamma_max": float64(1)}},
		{"max == min", map[string]any{"gamma_min": float64(1), "gamma_max": float64(1)}},
		{"steps=1", map[string]any{"gamma_steps": float64(1)}},
		{"NaN min", map[string]any{"gamma_min": math.NaN()}},
		{"+Inf max", map[string]any{"gamma_max": math.Inf(1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := (ResolutionPlateau{}).Compute(context.Background(), r, c.params); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

// TestResolutionPlateau_RejectsEmptyGammas: explicit empty gammas list.
func TestResolutionPlateau_RejectsEmptyGammas(t *testing.T) {
	r := &router.Result{Columns: []string{"src", "dst"}, Rows: nil}
	if _, err := (ResolutionPlateau{}).Compute(context.Background(), r, map[string]any{
		"gammas": []any{},
	}); err == nil {
		t.Fatal("expected error for empty gammas")
	}
}

// TestResolutionPlateau_HonorsCancelledContext: a pre-cancelled ctx
// returns context.Canceled without running any work.
func TestResolutionPlateau_HonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ResolutionPlateau{}.Compute(ctx, twoCliquesPlusBridge(), map[string]any{
		"gammas": []any{float64(0.5), float64(1.0)},
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestResolutionPlateau_CancelMidSweep: cancelling ctx while runSweep
// is in flight must (a) eventually return ctx.Err() and (b) not leak
// goroutines or race on partitions[i] writes. Mirrors the leiden
// plugin's CancelMidSweep test — combined with -race it locks the
// goroutine-cleanup contract for the resolution_plateau sweep.
func TestResolutionPlateau_CancelMidSweep(t *testing.T) {
	r := twoCliquesPlusBridge()
	gammas := make([]any, 32)
	for i := range gammas {
		gammas[i] = float64(0.5 + 0.1*float64(i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	_, err := ResolutionPlateau{}.Compute(ctx, r, map[string]any{
		"gammas": gammas,
		"seed":   float64(1),
	})

	// Either cancellation hit mid-loop (context.Canceled) or all 32 γs
	// finished first (nil). Both are valid; we just need the cancellation
	// path to be race-free when it does fire.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("expected nil or context.Canceled, got %v", err)
	}
}

// TestResolutionPlateau_RejectsMissingColumns: missing src/dst columns
// surface as plugin errors, not panics.
func TestResolutionPlateau_RejectsMissingColumns(t *testing.T) {
	r := &router.Result{Columns: []string{"a", "b"}, Rows: nil}
	if _, err := (ResolutionPlateau{}).Compute(context.Background(), r, nil); err == nil {
		t.Fatal("expected error for missing src column")
	}
}

// TestResolutionPlateau_DefaultRange: with no gamma params, uses the
// default linear range and produces a non-empty plateau list.
func TestResolutionPlateau_DefaultRange(t *testing.T) {
	out, err := ResolutionPlateau{}.Compute(context.Background(), twoCliquesPlusBridge(), nil)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	got := out.(map[string]any)
	if got["nmi_threshold"].(float64) != 0.95 {
		t.Errorf("default threshold not 0.95: %v", got["nmi_threshold"])
	}
	if got["num_nodes"].(int) != 8 {
		t.Errorf("num_nodes: %v", got["num_nodes"])
	}
	plateaus := got["plateaus"].([]map[string]any)
	if len(plateaus) == 0 {
		t.Errorf("expected non-empty plateaus from default range")
	}
}
