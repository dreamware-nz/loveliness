package mcp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeBackend lets tests register per-path handlers.
func fakeBackend(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.HandleFunc(path, h)
	}
	return httptest.NewServer(mux)
}

func TestClientCypher(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if got := string(body); !strings.Contains(got, "MATCH") {
				t.Fatalf("unexpected body %q", got)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer t1" {
				t.Fatalf("missing bearer token, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"columns":["n"],"rows":[{"n":1}],"stats":{"ms":1}}`))
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "t1", 2*time.Second)
	res, err := c.Cypher(context.Background(), "MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("Cypher: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "n" {
		t.Fatalf("columns: %v", res.Columns)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows: %v", res.Rows)
	}
}

func TestClientCypherHTTPError(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"CYPHER_PARSE_ERROR","message":"syntax error at line 1"}}`))
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", time.Second)
	_, err := c.Cypher(context.Background(), "bogus query", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T", err)
	}
	if he.Status != http.StatusBadRequest || he.Code != "CYPHER_PARSE_ERROR" {
		t.Fatalf("unexpected error: %+v", he)
	}
}

func TestClientCluster(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/cluster": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"leader":"127.0.0.1:9000","node_id":"n1","is_leader":true,"shard_map":{},"nodes":[],"schema":{}}`))
		},
	})
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", time.Second)
	st, err := c.Cluster(context.Background())
	if err != nil {
		t.Fatalf("Cluster: %v", err)
	}
	if !st.IsLeader || st.NodeID != "n1" {
		t.Fatalf("bad cluster status: %+v", st)
	}
}

func TestClientBulkNodes(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/bulk/nodes": func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Table") != "Person" {
				t.Fatalf("missing X-Table header, got %q", r.Header.Get("X-Table"))
			}
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), "Alice") {
				t.Fatalf("missing CSV body, got %q", body)
			}
			_, _ = w.Write([]byte(`{"table":"Person","loaded":3}`))
		},
	})
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", time.Second)
	res, err := c.BulkNodes(context.Background(), "Person", "name,age\nAlice,30\n")
	if err != nil {
		t.Fatalf("BulkNodes: %v", err)
	}
	if res.Loaded != 3 || res.Table != "Person" {
		t.Fatalf("bad result: %+v", res)
	}
}

func TestClientIngestNodes(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/ingest/nodes": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"job_id":"abc","status":"pending"}`))
		},
	})
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", time.Second)
	res, err := c.IngestNodes(context.Background(), "Person", "name\nAlice\n")
	if err != nil {
		t.Fatalf("IngestNodes: %v", err)
	}
	if res.JobID != "abc" || res.Status != "pending" {
		t.Fatalf("bad result: %+v", res)
	}
}

func TestClientIngestStatus(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/ingest/jobs/abc": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":"abc","status":"running","loaded":100}`))
		},
	})
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", time.Second)
	job, err := c.IngestStatus(context.Background(), "abc")
	if err != nil {
		t.Fatalf("IngestStatus: %v", err)
	}
	if job["status"] != "running" {
		t.Fatalf("bad job: %+v", job)
	}
}
