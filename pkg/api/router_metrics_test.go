package api

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/johnjansen/loveliness/pkg/router"
)

// The exposition writer must produce a Prometheus-shaped block for
// each non-empty section of the snapshot. An empty snapshot must
// produce no output at all — series should only appear once they've
// been observed at least once, matching the lazy-creation semantics
// the rest of /metrics already uses.
func TestWriteRouterMetrics_EmptySnapshotEmitsNothing(t *testing.T) {
	var buf bytes.Buffer
	writeRouterMetrics(&buf, router.RouterMetricsSnapshot{})
	if buf.Len() != 0 {
		t.Errorf("empty snapshot produced %d bytes; want 0\noutput:\n%s", buf.Len(), buf.String())
	}
}

// Full snapshot: every primitive populated. Verifies the exact
// Prometheus shape — HELP + TYPE headers, _bucket lines per (shard,
// le), _sum / _count, +Inf bucket, error counters, bloom skip
// counters — so a future writer change can't silently corrupt the
// scrape format.
func TestWriteRouterMetrics_FullShape(t *testing.T) {
	m := router.NewRouterMetrics()

	m.ObserveRemoteRTT(0, 0.0005)
	m.ObserveRemoteRTT(0, 0.05)
	m.ObserveRemoteRTT(3, 0.001)

	m.IncRemoteError("timeout")
	m.IncRemoteError("conn_refused")
	m.IncRemoteError("conn_refused")

	m.IncBloomSkip(0)
	m.IncBloomSkip(3)
	m.IncBloomSkip(3)

	var buf bytes.Buffer
	writeRouterMetrics(&buf, m.Snapshot())
	out := buf.String()

	wants := []string{
		"# HELP loveliness_router_remote_rtt_seconds",
		"# TYPE loveliness_router_remote_rtt_seconds histogram",
		"loveliness_router_remote_rtt_seconds_bucket{shard_id=\"0\",le=\"0.0005\"}",
		"loveliness_router_remote_rtt_seconds_bucket{shard_id=\"0\",le=\"+Inf\"} 2",
		"loveliness_router_remote_rtt_seconds_count{shard_id=\"0\"} 2",
		"loveliness_router_remote_rtt_seconds_count{shard_id=\"3\"} 1",
		"# HELP loveliness_router_remote_errors_total",
		"# TYPE loveliness_router_remote_errors_total counter",
		"loveliness_router_remote_errors_total{code=\"conn_refused\"} 2",
		"loveliness_router_remote_errors_total{code=\"timeout\"} 1",
		"# HELP loveliness_router_bloom_skip_total",
		"# TYPE loveliness_router_bloom_skip_total counter",
		"loveliness_router_bloom_skip_total{shard_id=\"0\"} 1",
		"loveliness_router_bloom_skip_total{shard_id=\"3\"} 2",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("output missing %q\nfull output:\n%s", w, out)
		}
	}
}

// Series ordering: every metric block must list its samples in the
// stable, sorted order that the snapshot promises (shard_id ascending
// for histograms + bloom skips, code alphabetical for errors). Diff-
// friendly scrapes depend on this not drifting.
func TestWriteRouterMetrics_DeterministicOrder(t *testing.T) {
	m := router.NewRouterMetrics()
	for _, sid := range []int{7, 2, 5} {
		m.ObserveRemoteRTT(sid, 0.001)
	}
	for _, code := range []string{"timeout", "conn_refused", "eof"} {
		m.IncRemoteError(code)
	}
	for _, sid := range []int{9, 1, 4} {
		m.IncBloomSkip(sid)
	}

	var buf bytes.Buffer
	writeRouterMetrics(&buf, m.Snapshot())
	out := buf.String()

	// Helper: each value should appear before the next in stable order.
	// Use _count lines (one per shard) for the histogram check.
	mustOrder := func(label string, parts []string) {
		t.Helper()
		positions := make([]int, len(parts))
		for i, p := range parts {
			positions[i] = strings.Index(out, p)
			if positions[i] < 0 {
				t.Errorf("%s: missing %q", label, p)
				return
			}
		}
		for i := 1; i < len(positions); i++ {
			if positions[i] <= positions[i-1] {
				t.Errorf("%s: %q (pos %d) must come after %q (pos %d)",
					label, parts[i], positions[i], parts[i-1], positions[i-1])
			}
		}
	}

	mustOrder("RTT shard order", []string{
		`loveliness_router_remote_rtt_seconds_count{shard_id="2"}`,
		`loveliness_router_remote_rtt_seconds_count{shard_id="5"}`,
		`loveliness_router_remote_rtt_seconds_count{shard_id="7"}`,
	})
	mustOrder("Errors alphabetical", []string{
		`loveliness_router_remote_errors_total{code="conn_refused"}`,
		`loveliness_router_remote_errors_total{code="eof"}`,
		`loveliness_router_remote_errors_total{code="timeout"}`,
	})
	mustOrder("Bloom skip shard order", []string{
		`loveliness_router_bloom_skip_total{shard_id="1"}`,
		`loveliness_router_bloom_skip_total{shard_id="4"}`,
		`loveliness_router_bloom_skip_total{shard_id="9"}`,
	})
}

// Partial-population sanity: only one of the three sections present.
// The other two must stay completely absent — no stray HELP lines,
// no zero-valued series.
func TestWriteRouterMetrics_PartialSection(t *testing.T) {
	m := router.NewRouterMetrics()
	m.IncRemoteError("timeout")

	var buf bytes.Buffer
	writeRouterMetrics(&buf, m.Snapshot())
	out := buf.String()

	if !strings.Contains(out, "loveliness_router_remote_errors_total") {
		t.Errorf("errors section missing")
	}
	if strings.Contains(out, "loveliness_router_remote_rtt_seconds") {
		t.Errorf("RTT section should be absent in partial snapshot")
	}
	if strings.Contains(out, "loveliness_router_bloom_skip_total") {
		t.Errorf("bloom-skip section should be absent in partial snapshot")
	}
}

// Sum + count consistency: with N observations summing to S, the
// emitted _sum and _count must round-trip exactly.
func TestWriteRouterMetrics_SumAndCountConsistent(t *testing.T) {
	m := router.NewRouterMetrics()
	durations := []float64{0.001, 0.002, 0.003, 0.005}
	for _, d := range durations {
		m.ObserveRemoteRTT(0, d)
	}

	var buf bytes.Buffer
	writeRouterMetrics(&buf, m.Snapshot())
	out := buf.String()

	wantCount := fmt.Sprintf("loveliness_router_remote_rtt_seconds_count{shard_id=\"0\"} %d", len(durations))
	if !strings.Contains(out, wantCount) {
		t.Errorf("count line missing or wrong; expected %q\nfull:\n%s", wantCount, out)
	}
	// Sum is 0.011s; formatGaugeFloat trims trailing zeros so the
	// exact rendering is "0.011".
	if !strings.Contains(out, `loveliness_router_remote_rtt_seconds_sum{shard_id="0"} 0.011`) {
		t.Errorf("sum line missing the expected 0.011 value\nfull:\n%s", out)
	}
}
