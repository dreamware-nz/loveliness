package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/analytics"
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

func TestQuery_CountByLabelHappyPath(t *testing.T) {
	srv := setupAnalyticsServer(t)
	body := `{
		"cypher": "MATCH (n) RETURN n",
		"analytics": [{"name": "count_by_label", "params": {"column": "label"}}]
	}`
	w, resp := postQuery(t, srv, body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(resp.AnalyticsErrors) != 0 {
		t.Fatalf("unexpected analytics errors: %v", resp.AnalyticsErrors)
	}
	got, ok := resp.Analytics["count_by_label"].(map[string]any)
	if !ok {
		t.Fatalf("missing count_by_label in analytics: %+v", resp.Analytics)
	}
	counts, _ := got["counts"].(map[string]any)
	// Two shards each seed 2 Users + 1 Post → 4 Users, 2 Posts.
	if counts["User"].(float64) != 4 {
		t.Errorf("User count: want 4, got %v", counts["User"])
	}
	if counts["Post"].(float64) != 2 {
		t.Errorf("Post count: want 2, got %v", counts["Post"])
	}
	if got["total"].(float64) != 6 {
		t.Errorf("total: want 6, got %v", got["total"])
	}
}

func TestQuery_MultiplePluginsAtOnce(t *testing.T) {
	// Both plugins requested; only count_by_label can run on this shape.
	// connected_components needs src/dst columns it won't find — should
	// surface in analytics_errors while count_by_label still succeeds.
	srv := setupAnalyticsServer(t)
	body := `{
		"cypher": "MATCH (n) RETURN n",
		"analytics": [
			{"name": "count_by_label", "params": {"column": "label"}},
			{"name": "connected_components"}
		]
	}`
	w, resp := postQuery(t, srv, body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if _, ok := resp.Analytics["count_by_label"]; !ok {
		t.Errorf("count_by_label should have succeeded: %+v", resp.Analytics)
	}
	if _, ok := resp.AnalyticsErrors["connected_components"]; !ok {
		t.Errorf("connected_components should have failed (no src column): %+v", resp.AnalyticsErrors)
	}
}

func TestQuery_WireFormatCarriesPartialAndErrors(t *testing.T) {
	// Direct marshalling test: queryResponse must round-trip Partial,
	// Errors, and Stats from router.Result so the new endpoint is a
	// strict superset of /db/{name}/cypher. We can't easily induce a
	// real partial scatter-gather in a unit test, so we construct the
	// state we want to see and assert it survives the wire.
	resp := queryResponse{
		Result: &router.Result{
			Columns: []string{"n"},
			Rows:    []map[string]any{{"n": 1}},
			Partial: true,
			Errors: []router.ShardError{
				{ShardID: 7, Error: "shard unavailable"},
			},
		},
		Analytics: map[string]any{"x": 1},
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["partial"] != true {
		t.Errorf("partial dropped: %s", raw)
	}
	gotErrs, ok := got["errors"].([]any)
	if !ok || len(gotErrs) != 1 {
		t.Fatalf("errors dropped: %s", raw)
	}
	first := gotErrs[0].(map[string]any)
	if first["shard_id"].(float64) != 7 || first["error"] != "shard unavailable" {
		t.Errorf("errors content corrupted: %+v", first)
	}
	if _, ok := got["analytics"]; !ok {
		t.Errorf("analytics block missing: %s", raw)
	}
}

func TestQuery_DuplicatePluginRejected(t *testing.T) {
	// Duplicate plugin names in one request must surface as an error,
	// not silently last-write-wins. Otherwise a client can't tell why
	// only one result came back, and a malicious client can amplify
	// expensive plugin runs under one body limit.
	srv := setupAnalyticsServer(t)
	body := `{
		"cypher": "MATCH (n) RETURN n",
		"analytics": [
			{"name": "count_by_label", "params": {"column": "label"}},
			{"name": "count_by_label", "params": {"column": "name"}}
		]
	}`
	w, resp := postQuery(t, srv, body)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if resp.AnalyticsErrors["count_by_label"] != "duplicate plugin in request" {
		t.Errorf("expected duplicate error, got %q", resp.AnalyticsErrors["count_by_label"])
	}
	// The first occurrence still ran successfully.
	if _, ok := resp.Analytics["count_by_label"]; !ok {
		t.Errorf("first occurrence should still be in analytics: %+v", resp.Analytics)
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

// stubFreezePlugin is a minimal Plugin used to drive RegisterAnalyticsPlugin.
type stubFreezePlugin struct{ name string }

func (s stubFreezePlugin) Name() string { return s.name }
func (s stubFreezePlugin) Compute(_ context.Context, _ *router.Result, _ map[string]any) (any, error) {
	return nil, nil
}

func TestRegisterAnalyticsPlugin_FrozenAfterHandler(t *testing.T) {
	srv := setupAnalyticsServer(t)
	if err := srv.RegisterAnalyticsPlugin(stubFreezePlugin{name: "early"}); err != nil {
		t.Fatalf("pre-Handler register: %v", err)
	}
	_ = srv.Handler() // freezes the registry
	err := srv.RegisterAnalyticsPlugin(stubFreezePlugin{name: "late"})
	if !errors.Is(err, analytics.ErrRegistryFrozen) {
		t.Errorf("expected ErrRegistryFrozen after Handler(), got %v", err)
	}
	// Calling Handler() again must remain safe (idempotent freeze).
	_ = srv.Handler()
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
