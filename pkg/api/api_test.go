package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupTestServer() *Server {
	shards := make([]*shard.Shard, 3)
	for i := 0; i < 3; i++ {
		store := shard.NewMemoryStore()
		store.PutNode("test", map[string]any{"name": "test", "shard": i})
		shards[i] = shard.NewShard(i, store, 4)
	}
	r := router.NewRouter(shards, 5*time.Second)
	return NewServer(r, nil, shards, nil, 5*time.Second)
}

func TestQuery_ValidCypher(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": "MATCH (p:Person {name: 'test'}) RETURN p"}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result router.Result
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) == 0 {
		t.Error("expected rows in result")
	}
}

func TestQuery_EmptyBody(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestQuery_InvalidJSON(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestQuery_EmptyCypher(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": ""}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestQuery_MergeWithShardKey(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": "MERGE (n:Person {name: 'Alice'})"}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestQuery_WriteWithoutShardKey(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": "MATCH (p:Person) SET p.active = true"}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	// Writes without shard key should be rejected.
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestQuery_ScatterGather(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": "MATCH (p:Person) RETURN p"}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result router.Result
	json.Unmarshal(w.Body.Bytes(), &result)
	// Scatter-gather across 3 shards, each with 1 node.
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows from scatter-gather, got %d", len(result.Rows))
	}
}

func TestHealth_Standalone(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
	// Should include shard health.
	shards, ok := resp["shards"].(map[string]any)
	if !ok {
		t.Fatal("expected shards in health response")
	}
	if len(shards) != 3 {
		t.Errorf("expected 3 shards, got %d", len(shards))
	}
	for id, status := range shards {
		if status != "healthy" {
			t.Errorf("shard %s expected healthy, got %v", id, status)
		}
	}
}

func TestHealth_RequestID(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	reqID := w.Header().Get("X-Request-ID")
	if reqID == "" {
		t.Error("expected X-Request-ID header")
	}
	if len(reqID) != 16 { // 8 bytes = 16 hex chars
		t.Errorf("expected 16-char request ID, got %d: %s", len(reqID), reqID)
	}
}

func TestHealth_DegradedWithUnhealthyShard(t *testing.T) {
	// Create shards, one of which panics (becomes unhealthy).
	shards := make([]*shard.Shard, 2)
	shards[0] = shard.NewShard(0, shard.NewMemoryStore(), 4)
	shards[1] = shard.NewShard(1, &panicStore{}, 4)

	// Trigger the panic to mark shard 1 unhealthy.
	shards[1].Query("MATCH (n) RETURN n")

	r := router.NewRouter(shards, 5*time.Second)
	srv := NewServer(r, nil, shards, nil, 5*time.Second)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "degraded" {
		t.Errorf("expected status degraded, got %v", resp["status"])
	}
}

// panicStore panics on every query (for testing degraded health).
type panicStore struct{}

func (s *panicStore) Query(string) (*shard.QueryResponse, error) { panic("boom") }
func (s *panicStore) Close() error                               { return nil }

func TestCluster_NoCluster(t *testing.T) {
	srv := setupTestServer() // no cluster configured
	req := httptest.NewRequest("GET", "/cluster", nil)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}
