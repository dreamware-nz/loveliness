package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/analytics"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/plugins"
)

func stubRunner(result *router.Result) CypherRunner {
	return func(_ context.Context, _ string) (*router.Result, error) {
		return result, nil
	}
}

func newTestServer(t *testing.T, runner CypherRunner) *httptest.Server {
	t.Helper()
	reg := analytics.NewRegistry()
	if err := reg.Register(plugins.CountByLabel{}); err != nil {
		t.Fatalf("register count_by_label: %v", err)
	}
	if err := reg.Register(plugins.ConnectedComponents{}); err != nil {
		t.Fatalf("register connected_components: %v", err)
	}
	s := New(runner, reg, time.Second)
	return httptest.NewServer(s.Handler())
}

func TestQueryWithoutAnalytics(t *testing.T) {
	result := &router.Result{
		Columns: []string{"label"},
		Rows: []map[string]any{
			{"label": "User"}, {"label": "User"}, {"label": "Post"},
		},
	}
	srv := newTestServer(t, stubRunner(result))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/db/main/query", `{"cypher":"MATCH (n) RETURN n.label AS label"}`)
	if resp.Analytics != nil {
		t.Errorf("expected no analytics, got %v", resp.Analytics)
	}
	if len(resp.Rows) != 3 {
		t.Errorf("rows: want 3, got %d", len(resp.Rows))
	}
}

func TestCountByLabelPlugin(t *testing.T) {
	result := &router.Result{
		Columns: []string{"label"},
		Rows: []map[string]any{
			{"label": "User"}, {"label": "User"}, {"label": "User"},
			{"label": "Post"}, {"label": "Post"},
			{"label": "Tag"},
		},
	}
	srv := newTestServer(t, stubRunner(result))
	defer srv.Close()

	body := `{
		"cypher": "MATCH (n) RETURN n.label AS label",
		"analytics": [{"name": "count_by_label", "params": {"column": "label"}}]
	}`
	resp := postJSON(t, srv.URL+"/db/main/query", body)
	got, ok := resp.Analytics["count_by_label"].(map[string]any)
	if !ok {
		t.Fatalf("missing count_by_label: %+v", resp.Analytics)
	}
	counts := got["counts"].(map[string]any)
	if counts["User"].(float64) != 3 {
		t.Errorf("User count: want 3, got %v", counts["User"])
	}
	if counts["Post"].(float64) != 2 {
		t.Errorf("Post count: want 2, got %v", counts["Post"])
	}
	if counts["Tag"].(float64) != 1 {
		t.Errorf("Tag count: want 1, got %v", counts["Tag"])
	}
}

func TestConnectedComponentsPlugin(t *testing.T) {
	rows := []map[string]any{}
	for _, e := range [][2]string{
		{"a", "b"}, {"b", "c"}, {"c", "a"},
		{"x", "y"}, {"y", "z"}, {"z", "x"},
	} {
		rows = append(rows, map[string]any{"src": e[0], "dst": e[1]})
	}
	result := &router.Result{Columns: []string{"src", "dst"}, Rows: rows}
	srv := newTestServer(t, stubRunner(result))
	defer srv.Close()

	body := `{
		"cypher": "MATCH (s)-[:KNOWS]->(d) RETURN s.id AS src, d.id AS dst",
		"analytics": [{"name": "connected_components"}]
	}`
	resp := postJSON(t, srv.URL+"/db/main/query", body)
	got, ok := resp.Analytics["connected_components"].(map[string]any)
	if !ok {
		t.Fatalf("missing connected_components: %+v", resp.Analytics)
	}
	if got["num_components"].(float64) != 2 {
		t.Errorf("components: want 2, got %v", got["num_components"])
	}
	if got["num_nodes"].(float64) != 6 {
		t.Errorf("nodes: want 6, got %v", got["num_nodes"])
	}
	if got["largest"].(float64) != 3 {
		t.Errorf("largest: want 3, got %v", got["largest"])
	}
}

func TestUnknownPlugin(t *testing.T) {
	result := &router.Result{Columns: []string{"x"}, Rows: []map[string]any{{"x": 1}}}
	srv := newTestServer(t, stubRunner(result))
	defer srv.Close()

	body := `{"cypher":"RETURN 1 AS x","analytics":[{"name":"nonexistent"}]}`
	resp := postJSON(t, srv.URL+"/db/main/query", body)
	if got := resp.AnalyticsErrors["nonexistent"]; got != "unknown plugin" {
		t.Errorf("want 'unknown plugin', got %q", got)
	}
}

func TestPluginErrorIsolation(t *testing.T) {
	result := &router.Result{Columns: []string{"label"}, Rows: []map[string]any{{"label": "A"}}}
	srv := newTestServer(t, stubRunner(result))
	defer srv.Close()

	body := `{
		"cypher": "MATCH (n) RETURN n.label AS label",
		"analytics": [{"name": "count_by_label", "params": {"column": "missing"}}]
	}`
	resp := postJSON(t, srv.URL+"/db/main/query", body)
	if _, hasErr := resp.AnalyticsErrors["count_by_label"]; !hasErr {
		t.Errorf("expected count_by_label to error on missing column")
	}
}

func BenchmarkPluginOverhead10K(b *testing.B) {
	rows := make([]map[string]any, 10_000)
	for i := range rows {
		rows[i] = map[string]any{"label": fmt.Sprintf("L%d", i%50)}
	}
	result := &router.Result{Columns: []string{"label"}, Rows: rows}
	reg := analytics.NewRegistry()
	_ = reg.Register(plugins.CountByLabel{})
	reqs := []analytics.Request{{Name: "count_by_label", Params: map[string]any{"column": "label"}}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, errs := reg.Run(context.Background(), result, reqs)
		if len(errs) > 0 {
			b.Fatalf("errs: %v", errs)
		}
		_ = out
	}
}

func postJSON(t *testing.T, url, body string) queryResponse {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var out queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}
