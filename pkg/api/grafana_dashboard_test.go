package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGrafanaOverviewDashboard_ParsesAndCoversCoreMetrics validates the
// shipped Grafana JSON. The point isn't to lint Grafana's full schema —
// that's Grafana's job at import time. The point is to catch drift:
// when a metric name changes, this test fails until the dashboard is
// updated (or vice-versa).
func TestGrafanaOverviewDashboard_ParsesAndCoversCoreMetrics(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "grafana", "loveliness-overview.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v", path, err)
	}

	var dash struct {
		Title  string `json:"title"`
		UID    string `json:"uid"`
		Panels []struct {
			Title   string `json:"title"`
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
		Templating struct {
			List []struct {
				Name string `json:"name"`
			} `json:"list"`
		} `json:"templating"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("invalid JSON in dashboard: %v", err)
	}

	if dash.Title == "" {
		t.Error("dashboard must have a title")
	}
	if dash.UID == "" {
		t.Error("dashboard must have a uid (so re-imports overwrite the same dashboard)")
	}
	if len(dash.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}

	hasDS := false
	for _, v := range dash.Templating.List {
		if v.Name == "DS_PROMETHEUS" {
			hasDS = true
			break
		}
	}
	if !hasDS {
		t.Error("dashboard must define a DS_PROMETHEUS templating variable")
	}

	// Every panel must have at least one target with a non-empty expr.
	for i, p := range dash.Panels {
		if len(p.Targets) == 0 {
			t.Errorf("panel %d (%q) has no targets", i, p.Title)
			continue
		}
		anyExpr := false
		for _, tgt := range p.Targets {
			if strings.TrimSpace(tgt.Expr) != "" {
				anyExpr = true
				break
			}
		}
		if !anyExpr {
			t.Errorf("panel %d (%q) has no non-empty expr", i, p.Title)
		}
	}

	// Every metric we emit and consider operationally important must
	// appear in at least one panel expr. Adding a series here forces
	// the dashboard to display it, or the test fails.
	expectedMetrics := []string{
		"loveliness_raft_state",
		"loveliness_shard_healthy",
		"loveliness_query_total",
		"loveliness_query_duration_seconds_bucket",
		"loveliness_replication_lag_seconds",
		"loveliness_replication_lag_entries",
		"loveliness_bulk_load_rows_total",
		"loveliness_go_goroutines",
		"loveliness_go_memstats_alloc_bytes",
		"loveliness_go_memstats_heap_inuse_bytes",
		"loveliness_go_memstats_sys_bytes",
		"loveliness_go_gc_pause_seconds_total",
	}

	allExprs := make([]string, 0)
	for _, p := range dash.Panels {
		for _, tgt := range p.Targets {
			allExprs = append(allExprs, tgt.Expr)
		}
	}
	joined := strings.Join(allExprs, "\n")

	for _, m := range expectedMetrics {
		if !strings.Contains(joined, m) {
			t.Errorf("metric %q is not referenced by any panel — dashboard out of date with emitted series", m)
		}
	}
}
