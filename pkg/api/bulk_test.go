package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupTestServerWithSchema() *Server {
	shards := make([]*shard.Shard, 3)
	for i := 0; i < 3; i++ {
		store := shard.NewMemoryStore()
		store.PutNode("test", map[string]any{"name": "test", "shard": i})
		shards[i] = shard.NewShard(i, store, 4)
	}
	reg := schema.NewRegistry()
	reg.Register("Person", "name")
	r := router.NewRouterWithSchema(shards, 5*time.Second, reg)
	return NewServer(r, nil, shards, reg, 5*time.Second)
}

// --- POST /cypher tests ---

func TestCypher_RawTextBody(t *testing.T) {
	srv := setupTestServerWithSchema()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person {name: 'test'}) RETURN p"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCypher_EmptyBody(t *testing.T) {
	srv := setupTestServerWithSchema()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString(""))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCypher_ScatterGather(t *testing.T) {
	srv := setupTestServerWithSchema()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result router.Result
	json.Unmarshal(w.Body.Bytes(), &result)
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

// --- POST /bulk/nodes tests ---

func TestBulkNodes_MissingHeader(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "name,age,city\nAlice,30,Auckland\n"
	req := httptest.NewRequest("POST", "/bulk/nodes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBulkNodes_UnknownTable(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "id,val\n1,test\n"
	req := httptest.NewRequest("POST", "/bulk/nodes", bytes.NewBufferString(body))
	req.Header.Set("X-Table", "UnknownTable")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBulkNodes_Success(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "name,age,city\nAlice,30,Auckland\nBob,25,Wellington\nCharlie,28,Hamilton\n"
	req := httptest.NewRequest("POST", "/bulk/nodes", bytes.NewBufferString(body))
	req.Header.Set("X-Table", "Person")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result bulkResult
	json.Unmarshal(w.Body.Bytes(), &result)
	if result.Loaded != 3 {
		t.Errorf("expected 3 loaded, got %d", result.Loaded)
	}
	if result.Table != "Person" {
		t.Errorf("expected table Person, got %s", result.Table)
	}
}

func TestBulkNodes_MissingShardKeyColumn(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "age,city\n30,Auckland\n"
	req := httptest.NewRequest("POST", "/bulk/nodes", bytes.NewBufferString(body))
	req.Header.Set("X-Table", "Person")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// --- POST /bulk/edges tests ---

func TestBulkEdges_MissingHeaders(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "from,to,since\nAlice,Bob,2020\n"
	req := httptest.NewRequest("POST", "/bulk/edges", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBulkEdges_Success(t *testing.T) {
	srv := setupTestServerWithSchema()
	body := "from,to,since\nAlice,Bob,2020\nCharlie,Dave,2021\n"
	req := httptest.NewRequest("POST", "/bulk/edges", bytes.NewBufferString(body))
	req.Header.Set("X-Rel-Table", "KNOWS")
	req.Header.Set("X-From-Table", "Person")
	req.Header.Set("X-To-Table", "Person")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result bulkResult
	json.Unmarshal(w.Body.Bytes(), &result)
	if result.Loaded != 2 {
		t.Errorf("expected 2 loaded, got %d", result.Loaded)
	}
}
