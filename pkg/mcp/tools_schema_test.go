package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// TestSchemaCache verifies that schema() is cached for 30s.
func TestSchemaCache(t *testing.T) {
	var calls atomic.Int32
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			// Respond to show_tables() first, then table_info calls.
			// Differentiate by a cheap body-sniff.
			buf := make([]byte, 256)
			n, _ := r.Body.Read(buf)
			body := string(buf[:n])
			switch {
			case containsAll(body, "show_tables"):
				writeJSON(w, map[string]any{
					"columns": []string{"name", "type"},
					"rows": []map[string]any{
						{"name": "Person", "type": "NODE"},
					},
				})
			case containsAll(body, "table_info"):
				writeJSON(w, map[string]any{
					"columns": []string{"name", "type", "primary key"},
					"rows": []map[string]any{
						{"name": "name", "type": "STRING", "primary key": true},
						{"name": "age", "type": "INT64", "primary key": false},
					},
				})
			default:
				http.Error(w, "unexpected body: "+body, 400)
			}
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	cache := newSchemaCache(30 * time.Second)

	for i := 0; i < 3; i++ {
		out, err := cache.get(context.Background(), c)
		if err != nil {
			t.Fatalf("cache.get: %v", err)
		}
		if len(out.NodeTables) != 1 || out.NodeTables[0].Name != "Person" {
			t.Fatalf("bad schema: %+v", out)
		}
		if out.NodeTables[0].PrimaryKey != "name" {
			t.Fatalf("primary key not captured: %+v", out.NodeTables[0])
		}
		if len(out.NodeTables[0].Properties) != 2 {
			t.Fatalf("properties: %+v", out.NodeTables[0].Properties)
		}
	}

	// show_tables + one table_info per table = 2 HTTP calls, only on the
	// first fetch. Subsequent cached fetches should add none.
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 backend calls, got %d", got)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(v)
	_, _ = w.Write(b)
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !stringContains(s, sub) {
			return false
		}
	}
	return true
}

func stringContains(haystack, needle string) bool {
	return len(needle) == 0 || indexOf(haystack, needle) >= 0
}

func indexOf(h, n string) int {
	hb, nb := []byte(h), []byte(n)
	if len(nb) == 0 {
		return 0
	}
	for i := 0; i+len(nb) <= len(hb); i++ {
		match := true
		for j := 0; j < len(nb); j++ {
			if hb[i+j] != nb[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
