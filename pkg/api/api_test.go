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
	return NewServer(r, nil, 5*time.Second)
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

func TestQuery_UnsupportedClause(t *testing.T) {
	srv := setupTestServer()
	body := `{"cypher": "MERGE (n:Person {name: 'Alice'})"}`
	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	var resp errorResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error.Code != "CYPHER_PARSE_ERROR" {
		t.Errorf("expected CYPHER_PARSE_ERROR, got %s", resp.Error.Code)
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

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %s", resp["status"])
	}
}

func TestCluster_NoCluster(t *testing.T) {
	srv := setupTestServer() // no cluster configured
	req := httptest.NewRequest("GET", "/cluster", nil)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}
