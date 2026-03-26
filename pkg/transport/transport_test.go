package transport

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupTestHandler() (*Handler, *shard.Manager) {
	m := shard.NewTestManager("node-1")
	m.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-1"},
	})
	s := m.GetShard(0)
	if ms, ok := s.Store.(*shard.MemoryStore); ok {
		ms.PutNode("alice", map[string]any{"name": "Alice"})
	}
	return NewHandler(m), m
}

func TestInternalQuery_Success(t *testing.T) {
	h, _ := setupTestHandler()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/internal/query",
		bytes.NewBufferString(`{"shard_id": 0, "cypher": "MATCH (n) RETURN n"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInternalQuery_ShardNotLocal(t *testing.T) {
	h, _ := setupTestHandler()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/internal/query",
		bytes.NewBufferString(`{"shard_id": 99, "cypher": "MATCH (n) RETURN n"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestInternalQuery_InvalidBody(t *testing.T) {
	h, _ := setupTestHandler()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/internal/query",
		bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestClient_QueryRemote(t *testing.T) {
	h, _ := setupTestHandler()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(5 * time.Second)
	c.SetPeer("node-1", srv.Listener.Addr().String())

	resp, err := c.QueryRemote("node-1", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) == 0 {
		t.Error("expected rows from remote query")
	}
}

func TestClient_QueryRemote_UnknownPeer(t *testing.T) {
	c := NewClient(5 * time.Second)
	_, err := c.QueryRemote("unknown", 0, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for unknown peer")
	}
}

func TestClient_RemovePeer(t *testing.T) {
	c := NewClient(5 * time.Second)
	c.SetPeer("node-1", "localhost:9999")
	c.RemovePeer("node-1")
	_, err := c.QueryRemote("node-1", 0, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error after removing peer")
	}
}

func TestClient_QueryRemote_ShardNotOnPeer(t *testing.T) {
	h, _ := setupTestHandler()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(5 * time.Second)
	c.SetPeer("node-1", srv.Listener.Addr().String())

	_, err := c.QueryRemote("node-1", 99, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for shard not on peer")
	}
}
