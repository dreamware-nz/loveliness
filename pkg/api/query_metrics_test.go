package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestClassifyQueryType(t *testing.T) {
	cases := []struct {
		cypher string
		want   string
	}{
		{"MATCH (n) RETURN n", "read"},
		{"  match (n:Person) return n  ", "read"},
		{"OPTIONAL MATCH (n)-[r]->(m) RETURN m", "read"},
		{"WITH 1 AS x RETURN x", "read"},
		{"UNWIND [1,2,3] AS i RETURN i", "read"},
		{"CALL db.schema()", "read"},
		{"SHOW DATABASES", "read"},

		{"CREATE (p:Person {name: 'Alice'})", "write"},
		{"MERGE (n {id: 1})", "write"},
		{"SET n.foo = 1", "write"},
		{"DELETE n", "write"},
		{"REMOVE n.foo", "write"},
		{"DETACH DELETE n", "write"},

		{"CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))", "schema"},
		{"CREATE REL TABLE FOLLOWS(FROM Person TO Person)", "schema"},
		{"DROP TABLE Person", "schema"},
		{"ALTER TABLE Person ADD age INT64", "schema"},

		{"", "unknown"},
		{"   ", "unknown"},
		{"BLAH BLAH", "unknown"},
	}
	for _, tc := range cases {
		got := classifyQueryType(tc.cypher)
		if got != tc.want {
			t.Errorf("classifyQueryType(%q) = %q, want %q", tc.cypher, got, tc.want)
		}
	}
}

func TestStatusBucket(t *testing.T) {
	cases := map[int]string{
		200: "ok",
		201: "ok",
		299: "ok",
		400: "client_error",
		404: "client_error",
		499: "client_error",
		500: "server_error",
		503: "server_error",
		599: "server_error",
		100: "unknown",
		301: "unknown",
	}
	for code, want := range cases {
		if got := statusBucket(code); got != want {
			t.Errorf("statusBucket(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestQueryCounters_IncAndSnapshot(t *testing.T) {
	q := newQueryCounters()
	q.Inc("read", "ok")
	q.Inc("read", "ok")
	q.Inc("write", "server_error")
	q.Inc("read", "client_error")

	snap := q.Snapshot()
	got := map[string]uint64{}
	for _, s := range snap {
		got[s.QueryType+"/"+s.Status] = s.Count
	}
	if got["read/ok"] != 2 {
		t.Errorf("read/ok = %d, want 2", got["read/ok"])
	}
	if got["read/client_error"] != 1 {
		t.Errorf("read/client_error = %d, want 1", got["read/client_error"])
	}
	if got["write/server_error"] != 1 {
		t.Errorf("write/server_error = %d, want 1", got["write/server_error"])
	}
}

func TestQueryCounters_SnapshotIsSorted(t *testing.T) {
	q := newQueryCounters()
	q.Inc("write", "ok")
	q.Inc("read", "ok")
	q.Inc("read", "client_error")
	q.Inc("schema", "ok")

	snap := q.Snapshot()
	// Expected sort: read/client_error, read/ok, schema/ok, write/ok.
	want := []queryCounterSample{
		{QueryType: "read", Status: "client_error", Count: 1},
		{QueryType: "read", Status: "ok", Count: 1},
		{QueryType: "schema", Status: "ok", Count: 1},
		{QueryType: "write", Status: "ok", Count: 1},
	}
	if len(snap) != len(want) {
		t.Fatalf("expected %d samples, got %d", len(want), len(snap))
	}
	for i := range snap {
		if snap[i] != want[i] {
			t.Errorf("snap[%d] = %+v, want %+v", i, snap[i], want[i])
		}
	}
}

func TestQueryCounters_NilSafe(t *testing.T) {
	var q *queryCounters
	q.Inc("read", "ok")            // must not panic
	if got := q.Snapshot(); got != nil {
		t.Errorf("nil snapshot must be nil, got %v", got)
	}
}

func TestQueryCounters_ConcurrentInc(t *testing.T) {
	q := newQueryCounters()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				q.Inc("read", "ok")
			}
		}()
	}
	wg.Wait()
	snap := q.Snapshot()
	if len(snap) != 1 || snap[0].Count != 10000 {
		t.Errorf("expected 1 series with count=10000, got %+v", snap)
	}
}

func TestWriteQueryCounterMetrics_EmptyEmitsNothing(t *testing.T) {
	var buf bytes.Buffer
	writeQueryCounterMetrics(&buf, nil)
	if buf.Len() != 0 {
		t.Errorf("empty samples must emit nothing, got:\n%s", buf.String())
	}
}

func TestWriteQueryCounterMetrics_Format(t *testing.T) {
	var buf bytes.Buffer
	writeQueryCounterMetrics(&buf, []queryCounterSample{
		{QueryType: "read", Status: "ok", Count: 7},
		{QueryType: "write", Status: "server_error", Count: 1},
	})
	out := buf.String()
	mustContain := []string{
		"# HELP loveliness_query_total",
		"# TYPE loveliness_query_total counter",
		`loveliness_query_total{query_type="read",status="ok"} 7`,
		`loveliness_query_total{query_type="write",status="server_error"} 1`,
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in:\n%s", s, out)
		}
	}
}

func TestQueryHistogram_ObserveAndSnapshot(t *testing.T) {
	h := newQueryHistogram()
	h.Observe("read", "ok", 0.001)   // ≤ 0.001
	h.Observe("read", "ok", 0.003)   // ≤ 0.005
	h.Observe("read", "ok", 0.4)     // ≤ 0.5
	h.Observe("read", "ok", 5.0)     // > 2.5 → only +Inf
	h.Observe("write", "ok", 0.0001) // ≤ 0.00025

	snaps := h.Snapshot()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 series, got %d", len(snaps))
	}

	read := snaps[0]
	if read.QueryType != "read" || read.Status != "ok" {
		t.Fatalf("first should be read/ok, got %+v", read)
	}
	if read.Count != 4 {
		t.Errorf("read count = %d, want 4", read.Count)
	}
	wantSum := 0.001 + 0.003 + 0.4 + 5.0
	if read.Sum < wantSum-1e-9 || read.Sum > wantSum+1e-9 {
		t.Errorf("read sum = %v, want %v", read.Sum, wantSum)
	}

	// +Inf bucket must equal total count.
	last := read.Buckets[len(read.Buckets)-1]
	if last.UpperBound == 0 {
		t.Fatal("expected +Inf bucket at the end")
	}
	if last.Count != read.Count {
		t.Errorf("+Inf bucket count %d != total count %d", last.Count, read.Count)
	}

	// le=0.001 bucket: 0.001 falls into it (≤), 0.003 doesn't.
	bucket0p001 := findBucket(read.Buckets, 0.001)
	if bucket0p001 != 1 {
		t.Errorf("le=0.001 bucket count = %d, want 1", bucket0p001)
	}
	// le=0.005 bucket: 0.001 and 0.003 both fall in.
	bucket0p005 := findBucket(read.Buckets, 0.005)
	if bucket0p005 != 2 {
		t.Errorf("le=0.005 bucket count = %d, want 2", bucket0p005)
	}
	// le=0.5 bucket: 0.001, 0.003, and 0.4 all fall in.
	bucket0p5 := findBucket(read.Buckets, 0.5)
	if bucket0p5 != 3 {
		t.Errorf("le=0.5 bucket count = %d, want 3", bucket0p5)
	}
	// le=2.5 bucket: 5.0 doesn't fall in, so still 3.
	bucket2p5 := findBucket(read.Buckets, 2.5)
	if bucket2p5 != 3 {
		t.Errorf("le=2.5 bucket count = %d, want 3", bucket2p5)
	}
}

func findBucket(points []bucketPoint, le float64) uint64 {
	for _, p := range points {
		if p.UpperBound == le {
			return p.Count
		}
	}
	return 0
}

func TestQueryHistogram_NilSafe(t *testing.T) {
	var h *queryHistogram
	h.Observe("read", "ok", 0.1) // must not panic
	if got := h.Snapshot(); got != nil {
		t.Errorf("nil snapshot must be nil, got %v", got)
	}
}

func TestQueryHistogram_ConcurrentObserve(t *testing.T) {
	h := newQueryHistogram()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.Observe("read", "ok", 0.001)
			}
		}()
	}
	wg.Wait()
	snap := h.Snapshot()
	if len(snap) != 1 || snap[0].Count != 5000 {
		t.Errorf("expected count=5000, got %+v", snap)
	}
}

func TestWriteQueryHistogramMetrics_EmitsAllSeries(t *testing.T) {
	h := newQueryHistogram()
	h.Observe("read", "ok", 0.0009) // ≤ 0.001 bucket
	h.Observe("read", "ok", 1.5)    // ≤ 2.5 bucket

	var buf bytes.Buffer
	writeQueryHistogramMetrics(&buf, h.Snapshot())
	out := buf.String()

	mustContain := []string{
		"# HELP loveliness_query_duration_seconds",
		"# TYPE loveliness_query_duration_seconds histogram",
		`loveliness_query_duration_seconds_bucket{query_type="read",status="ok",le="0.001"} 1`,
		`loveliness_query_duration_seconds_bucket{query_type="read",status="ok",le="2.5"} 2`,
		`loveliness_query_duration_seconds_bucket{query_type="read",status="ok",le="+Inf"} 2`,
		`loveliness_query_duration_seconds_count{query_type="read",status="ok"} 2`,
		`loveliness_query_duration_seconds_sum{query_type="read",status="ok"}`,
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in:\n%s", s, out)
		}
	}
}

func TestBulkLoadCounters_AddAndSnapshot(t *testing.T) {
	b := newBulkLoadCounters()
	b.Add("Person", 100)
	b.Add("Person", 50)
	b.Add("Movie", 7)

	snap := b.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 series, got %d", len(snap))
	}
	// Sorted alphabetically: Movie, Person.
	if snap[0].Table != "Movie" || snap[0].Count != 7 {
		t.Errorf("snap[0] = %+v, want {Movie 7}", snap[0])
	}
	if snap[1].Table != "Person" || snap[1].Count != 150 {
		t.Errorf("snap[1] = %+v, want {Person 150}", snap[1])
	}
}

func TestBulkLoadCounters_DropsZeroAndNegative(t *testing.T) {
	b := newBulkLoadCounters()
	b.Add("Person", 0)
	b.Add("Person", -5)
	b.Add("", 10)
	if got := b.Snapshot(); len(got) != 0 {
		t.Errorf("expected no samples, got %+v", got)
	}
}

func TestBulkLoadCounters_NilSafe(t *testing.T) {
	var b *bulkLoadCounters
	b.Add("Person", 5) // must not panic
	if got := b.Snapshot(); got != nil {
		t.Errorf("nil snapshot must be nil, got %v", got)
	}
}

func TestBulkLoadCounters_ConcurrentAdd(t *testing.T) {
	b := newBulkLoadCounters()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Add("Person", 1)
			}
		}()
	}
	wg.Wait()
	snap := b.Snapshot()
	if len(snap) != 1 || snap[0].Count != 5000 {
		t.Errorf("expected 1 series with count=5000, got %+v", snap)
	}
}

func TestWriteBulkLoadMetrics_EmptyEmitsNothing(t *testing.T) {
	var buf bytes.Buffer
	writeBulkLoadMetrics(&buf, nil)
	if buf.Len() != 0 {
		t.Errorf("empty samples must emit nothing, got:\n%s", buf.String())
	}
}

func TestWriteBulkLoadMetrics_Format(t *testing.T) {
	var buf bytes.Buffer
	writeBulkLoadMetrics(&buf, []bulkLoadSample{
		{Table: "Movie", Count: 7},
		{Table: "Person", Count: 150},
	})
	out := buf.String()
	mustContain := []string{
		"# HELP loveliness_bulk_load_rows_total",
		"# TYPE loveliness_bulk_load_rows_total counter",
		`loveliness_bulk_load_rows_total{table="Movie"} 7`,
		`loveliness_bulk_load_rows_total{table="Person"} 150`,
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("missing %q in:\n%s", s, out)
		}
	}
}

func TestBulkNodes_RecordsBulkLoadCounter(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "name,age,city\nAlice,30,Auckland\nBob,25,Wellington\nCharlie,28,Hamilton\n"
	req := httptest.NewRequest("POST", "/bulk/nodes", bytes.NewBufferString(body))
	req.Header.Set("X-Table", "Person")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("bulk/nodes returned %d: %s", w.Code, w.Body.String())
	}

	mreq := httptest.NewRequest("GET", "/metrics", nil)
	mw := httptest.NewRecorder()
	srv.Handler().ServeHTTP(mw, mreq)
	if mw.Code != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d: %s", mw.Code, mw.Body.String())
	}
	body2 := mw.Body.String()
	if !strings.Contains(body2, `loveliness_bulk_load_rows_total{table="Person"} 3`) {
		t.Errorf("expected Person counter at 3 in /metrics:\n%s", body2)
	}
}

func TestCypher_RecordsQueryCounter(t *testing.T) {
	srv := setupTestServer()

	// A read.
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// A write.
	req2 := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("CREATE (p:Person {name:'x'})"))
	w2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w2, req2)

	// An empty body — should record (unknown, client_error).
	req3 := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString(""))
	w3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w3, req3)

	// Now scrape /metrics.
	mreq := httptest.NewRequest("GET", "/metrics", nil)
	mw := httptest.NewRecorder()
	srv.Handler().ServeHTTP(mw, mreq)

	if mw.Code != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d: %s", mw.Code, mw.Body.String())
	}
	body := mw.Body.String()
	mustContain := []string{
		`loveliness_query_total{query_type="read",status="ok"} 1`,
		`loveliness_query_total{query_type="write",status="ok"} 1`,
		`loveliness_query_total{query_type="unknown",status="client_error"} 1`,
	}
	for _, s := range mustContain {
		if !strings.Contains(body, s) {
			t.Errorf("missing %q in /metrics body:\n%s", s, body)
		}
	}
}
