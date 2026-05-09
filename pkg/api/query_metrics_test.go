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
