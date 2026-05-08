package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientAdminCypher checks that the AdminCypher client method posts
// the raw command body to /admin/cypher and decodes the JSON response.
func TestClientAdminCypher(t *testing.T) {
	var gotBody atomic.Value
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/admin/cypher": func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			gotBody.Store(string(b))
			writeJSON(w, map[string]any{
				"columns": []string{"name", "state", "shard_count", "created_at"},
				"rows": []map[string]any{
					{"name": "default", "state": "running", "shard_count": float64(2), "created_at": "2026-05-01"},
					{"name": "scratch", "state": "stopped", "shard_count": float64(1), "created_at": "2026-05-02"},
				},
			})
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, err := c.AdminCypher(context.Background(), "SHOW DATABASES")
	if err != nil {
		t.Fatalf("AdminCypher: %v", err)
	}
	if got := gotBody.Load().(string); got != "SHOW DATABASES" {
		t.Fatalf("body: %q", got)
	}
	rows, _ := res["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows: %+v", res)
	}
}

// TestAdminCypherErrorPropagation verifies that a 400 from /admin/cypher
// surfaces as an HTTPError so toolError can classify it.
func TestAdminCypherErrorPropagation(t *testing.T) {
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/admin/cypher": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"BAD_REQUEST","message":"only admin commands accepted"}}`))
		},
	})
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", 2*time.Second)
	_, err := c.AdminCypher(context.Background(), "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "BAD_REQUEST") {
		t.Fatalf("error: %v", err)
	}
}

// TestSimpleStatusDecoder is a unit check on the decoder that pulls
// status/name/shard_count out of /admin/cypher responses, including
// when shard_count is absent.
func TestSimpleStatusDecoder(t *testing.T) {
	cases := []struct {
		in   map[string]any
		want SimpleStatusOutput
	}{
		{
			in:   map[string]any{"status": "created", "name": "foo", "shard_count": float64(4)},
			want: SimpleStatusOutput{Status: "created", Name: "foo", ShardCount: 4},
		},
		{
			in:   map[string]any{"status": "deleted", "name": "foo"},
			want: SimpleStatusOutput{Status: "deleted", Name: "foo"},
		},
		{
			in:   map[string]any{},
			want: SimpleStatusOutput{},
		},
	}
	for i, tc := range cases {
		got := simpleStatus(tc.in)
		if got != tc.want {
			t.Fatalf("case %d: got %+v want %+v", i, got, tc.want)
		}
	}
}
