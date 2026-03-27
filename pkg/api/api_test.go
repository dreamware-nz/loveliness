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

func TestCypher_ValidQuery(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person {name: 'test'}) RETURN p"))
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

func TestCypher_EmptyBody(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString(""))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCypher_WhitespaceOnly(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("   \n\t  "))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCypher_MergeWithShardKey(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MERGE (n:Person {name: 'Alice'})"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCypher_WriteWithoutShardKey(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person) SET p.active = true"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCypher_ScatterGather(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result router.Result
	json.Unmarshal(w.Body.Bytes(), &result)
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
	if len(reqID) != 16 {
		t.Errorf("expected 16-char request ID, got %d: %s", len(reqID), reqID)
	}
}

func TestHealth_DegradedWithUnhealthyShard(t *testing.T) {
	shards := make([]*shard.Shard, 2)
	shards[0] = shard.NewShard(0, shard.NewMemoryStore(), 4)
	shards[1] = shard.NewShard(1, &panicStore{}, 4)

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

type panicStore struct{}

func (s *panicStore) Query(string) (*shard.QueryResponse, error) { panic("boom") }
func (s *panicStore) Close() error                               { return nil }

func TestCluster_NoCluster(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/cluster", nil)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}
