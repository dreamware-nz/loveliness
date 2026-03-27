package transport

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupTCPServer(t *testing.T) (*TCPServer, string) {
	t.Helper()
	m := shard.NewTestManager("node-tcp")
	m.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: "node-tcp"},
		1: {Primary: "node-tcp"},
	})
	// Seed shard 0 with test data.
	s := m.GetShard(0)
	if ms, ok := s.Store.(*shard.MemoryStore); ok {
		ms.PutNode("alice", map[string]any{"name": "alice", "age": int64(30)})
		ms.PutNode("bob", map[string]any{"name": "bob", "age": int64(25)})
	}

	srv := NewTCPServer(m)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	return srv, srv.Addr().String()
}

func TestCodecRoundtrip(t *testing.T) {
	// Test encoding and decoding a QueryRequest.
	var buf bytes.Buffer
	req := QueryRequest{ShardID: 3, Cypher: "MATCH (n) RETURN n"}
	if err := WriteFrame(&buf, MsgQuery, req); err != nil {
		t.Fatal(err)
	}

	msgType, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgQuery {
		t.Errorf("expected MsgQuery, got %d", msgType)
	}

	var decoded QueryRequest
	if err := Decode(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ShardID != 3 || decoded.Cypher != "MATCH (n) RETURN n" {
		t.Errorf("decoded mismatch: %+v", decoded)
	}
}

func TestCodecResponseRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	resp := QueryResponse{
		Columns: []string{"name", "age"},
		Rows: []map[string]any{
			{"name": "alice", "age": int64(30)},
			{"name": "bob", "age": int64(25)},
		},
	}
	if err := WriteFrame(&buf, MsgResult, resp); err != nil {
		t.Fatal(err)
	}

	msgType, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgResult {
		t.Errorf("expected MsgResult, got %d", msgType)
	}

	var decoded QueryResponse
	if err := Decode(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(decoded.Rows))
	}
	if decoded.Columns[0] != "name" {
		t.Errorf("expected column 'name', got %s", decoded.Columns[0])
	}
}

func TestTCPServerQuery(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	resp, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) == 0 {
		t.Error("expected rows from TCP query")
	}
}

func TestTCPServerQuery_ShardNotFound(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	_, err := pool.QueryRemoteTCP("node-tcp", 99, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for missing shard")
	}
}

func TestTCPPoolUnknownPeer(t *testing.T) {
	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()

	_, err := pool.QueryRemoteTCP("unknown", 0, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for unknown peer")
	}
}

func TestTCPPoolConnectionReuse(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	// Issue multiple sequential queries — should reuse connections.
	for i := 0; i < 10; i++ {
		resp, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if len(resp.Rows) == 0 {
			t.Errorf("query %d: expected rows", i)
		}
	}

	pool.mu.RLock()
	connCount := len(pool.conns["node-tcp"])
	pool.mu.RUnlock()

	if connCount != 1 {
		t.Errorf("expected 1 pooled connection (reuse), got %d", connCount)
	}
}

func TestTCPPoolConcurrent(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(4, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
			if err != nil {
				errors <- fmt.Errorf("query %d: %v", i, err)
				return
			}
			if len(resp.Rows) == 0 {
				errors <- fmt.Errorf("query %d: no rows", i)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		t.Fatalf("%d errors, first: %v", len(errs), errs[0])
	}

	pool.mu.RLock()
	connCount := len(pool.conns["node-tcp"])
	pool.mu.RUnlock()

	if connCount > 4 {
		t.Errorf("expected at most 4 pooled connections, got %d", connCount)
	}
}

func TestTCPPoolRemovePeer(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	// Establish a connection.
	pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")

	pool.RemovePeer("node-tcp")

	_, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error after peer removal")
	}
}

func TestPingPong(t *testing.T) {
	var buf bytes.Buffer
	WriteFrame(&buf, MsgPing, nil)

	msgType, _, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgPing {
		t.Errorf("expected MsgPing, got %d", msgType)
	}
}
