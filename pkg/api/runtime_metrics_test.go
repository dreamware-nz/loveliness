package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteRuntimeMetrics_EmitsAllSeries(t *testing.T) {
	var buf bytes.Buffer
	writeRuntimeMetrics(&buf)
	out := buf.String()

	mustContain := []string{
		"# HELP loveliness_go_goroutines",
		"# TYPE loveliness_go_goroutines gauge",
		"loveliness_go_goroutines ",

		"# HELP loveliness_go_memstats_alloc_bytes",
		"# TYPE loveliness_go_memstats_alloc_bytes gauge",
		"loveliness_go_memstats_alloc_bytes ",

		"# HELP loveliness_go_memstats_sys_bytes",
		"# TYPE loveliness_go_memstats_sys_bytes gauge",
		"loveliness_go_memstats_sys_bytes ",

		"# HELP loveliness_go_memstats_heap_inuse_bytes",
		"loveliness_go_memstats_heap_inuse_bytes ",

		"# HELP loveliness_go_memstats_heap_objects",
		"loveliness_go_memstats_heap_objects ",

		"# HELP loveliness_go_gc_count_total",
		"# TYPE loveliness_go_gc_count_total counter",
		"loveliness_go_gc_count_total ",

		"# HELP loveliness_go_gc_pause_seconds_total",
		"# TYPE loveliness_go_gc_pause_seconds_total counter",
		"loveliness_go_gc_pause_seconds_total ",
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in:\n%s", s, out)
		}
	}
}

func TestWriteRuntimeMetrics_NonZeroValues(t *testing.T) {
	var buf bytes.Buffer
	writeRuntimeMetrics(&buf)
	out := buf.String()

	// At least one goroutine (the test runner) and some heap allocation
	// must always be present in a live Go process — confirms we're
	// reading actual values, not zeros.
	if strings.Contains(out, "loveliness_go_goroutines 0\n") {
		t.Errorf("goroutine count should be > 0:\n%s", out)
	}
	if strings.Contains(out, "loveliness_go_memstats_alloc_bytes 0\n") {
		t.Errorf("heap alloc should be > 0:\n%s", out)
	}
}

func TestRuntimeMetrics_ScrapedViaEndpoint(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	mustContain := []string{
		"loveliness_go_goroutines ",
		"loveliness_go_memstats_alloc_bytes ",
		"loveliness_go_gc_count_total ",
	}
	for _, s := range mustContain {
		if !strings.Contains(body, s) {
			t.Errorf("missing %q in /metrics body:\n%s", s, body)
		}
	}
}
