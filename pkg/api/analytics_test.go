package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/catalog"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// setupAnalyticsServer wires a multi-database router with a single
// "main" database, seeded with rows that have a `name` shard-key column
// so MATCH (n) RETURN n returns deterministic data across shards.
func setupAnalyticsServer(t *testing.T) *Server {
	t.Helper()
	cat := catalog.NewCatalog()
	db, err := cat.CreateDatabase("main", 2)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}

	shards := make([]*shard.Shard, len(db.ShardIDs))
	for i, id := range db.ShardIDs {
		store := shard.NewMemoryStore()
		store.PutNode("alice", map[string]any{"name": "alice", "label": "User"})
		store.PutNode("bob", map[string]any{"name": "bob", "label": "User"})
		store.PutNode("post", map[string]any{"name": "post", "label": "Post"})
		shards[i] = shard.NewShard(id, store, 4)
	}

	dr := router.NewDatabaseRouter(cat, 5*time.Second)
	dr.RegisterDatabase("main", shards)

	r := router.NewRouter(shards, 5*time.Second)
	srv := NewServer(r, nil, shards, nil, 5*time.Second)
	srv.SetDatabaseRouter(dr)
	return srv
}

func postQuery(t *testing.T, srv *Server, body string) (*httptest.ResponseRecorder, queryResponse) {
	t.Helper()
	req := httptest.NewRequest("POST", "/db/main/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	var resp queryResponse
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v\nbody=%s", err, w.Body.String())
		}
	}
	return w, resp
}

func TestQuery_NoAnalyticsIsCypherSuperset(t *testing.T) {
	srv := setupAnalyticsServer(t)
	w, resp := postQuery(t, srv, `{"cypher":"MATCH (n) RETURN n"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if resp.Analytics != nil || resp.AnalyticsErrors != nil {
		t.Errorf("did not request analytics, got analytics=%v errors=%v",
			resp.Analytics, resp.AnalyticsErrors)
	}
	if len(resp.Rows) == 0 {
		t.Errorf("expected rows from underlying cypher")
	}
}

func TestQuery_UnknownPluginIsolated(t *testing.T) {
	srv := setupAnalyticsServer(t)
	body := `{"cypher":"MATCH (n) RETURN n","analytics":[{"name":"nope"}]}`
	w, resp := postQuery(t, srv, body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (unknown plugin shouldn't kill request), got %d: %s",
			w.Code, w.Body.String())
	}
	if resp.AnalyticsErrors["nope"] != "unknown plugin" {
		t.Errorf("expected unknown plugin error, got %q", resp.AnalyticsErrors["nope"])
	}
}

func TestQuery_PluginErrorIsolation(t *testing.T) {
	srv := setupAnalyticsServer(t)
	// missing 'column' param → count_by_label fails, request still 200.
	body := `{"cypher":"MATCH (n) RETURN n","analytics":[{"name":"count_by_label"}]}`
	w, resp := postQuery(t, srv, body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if _, ok := resp.AnalyticsErrors["count_by_label"]; !ok {
		t.Errorf("expected count_by_label to surface in analytics_errors, got %v",
			resp.AnalyticsErrors)
	}
	if resp.Analytics["count_by_label"] != nil {
		t.Errorf("failed plugin should not appear in analytics")
	}
}

func TestQuery_BadJSON(t *testing.T) {
	srv := setupAnalyticsServer(t)
	w, _ := postQuery(t, srv, `{not json}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestQuery_MissingCypher(t *testing.T) {
	srv := setupAnalyticsServer(t)
	w, _ := postQuery(t, srv, `{"analytics":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestQuery_AdminCommandRejected(t *testing.T) {
	srv := setupAnalyticsServer(t)
	w, _ := postQuery(t, srv, `{"cypher":"CREATE DATABASE foo"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for admin command on /db/{name}/query, got %d", w.Code)
	}
}

func TestQuery_WithoutDBRouter(t *testing.T) {
	// No SetDatabaseRouter — should report NO_MULTI_DB.
	cat := catalog.NewCatalog()
	_, _ = cat.CreateDatabase("main", 1)
	shards := []*shard.Shard{shard.NewShard(0, shard.NewMemoryStore(), 4)}
	r := router.NewRouter(shards, 5*time.Second)
	srv := NewServer(r, nil, shards, nil, 5*time.Second)

	req := httptest.NewRequest("POST", "/db/main/query",
		bytes.NewBufferString(`{"cypher":"MATCH (n) RETURN n"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestAnalyticsList(t *testing.T) {
	srv := setupAnalyticsServer(t)
	req := httptest.NewRequest("GET", "/analytics", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var got map[string][]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	plugins := got["plugins"]
	if len(plugins) != 2 {
		t.Errorf("expected 2 built-in plugins, got %v", plugins)
	}
	have := map[string]bool{}
	for _, n := range plugins {
		have[n] = true
	}
	if !have["count_by_label"] || !have["connected_components"] {
		t.Errorf("missing built-in plugin: %v", plugins)
	}
}
