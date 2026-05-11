package transport

import (
	"bytes"
	"fmt"
	"net"
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

// TestTCPServerStop_FastTeardown_IdleConn covers the #83 fix: an idle client
// connection must not pin Stop() to the 60s read deadline. Before the fix this
// test would block ~60s; after the fix it returns within 1s.
func TestTCPServerStop_FastTeardown_IdleConn(t *testing.T) {
	srv, addr := setupTCPServer(t)

	// Open a client connection but send nothing, so the server's handleConn
	// is parked in ReadFrame against the 60s idle deadline.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Give the server a moment to register the conn.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed > time.Second {
			t.Errorf("Stop() took %v, expected < 1s (idle-conn teardown)", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s — teardown is blocked")
	}
}

// TestTCPServerStop_FastTeardown_PartialFrame covers the in-flight read case:
// a client that sent a header but no body parks the server inside ReadFrame
// reading the payload. Stop() must still wake it within 1s.
func TestTCPServerStop_FastTeardown_PartialFrame(t *testing.T) {
	srv, addr := setupTCPServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a frame header claiming a 1024-byte payload but never deliver the
	// body. ReadFrame on the server side will block reading the payload.
	// Frame format: [length u32 BE][msgType u8][payload...].
	header := []byte{0x00, 0x00, 0x04, 0x00, byte(MsgQuery)}
	if _, err := conn.Write(header); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed > time.Second {
			t.Errorf("Stop() took %v, expected < 1s (partial-frame teardown)", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s — teardown is blocked on partial read")
	}
}

// TestTCPServerStop_Idempotent verifies Stop() can be called multiple times
// without panicking on close-of-closed-channel or double-deregister.
func TestTCPServerStop_Idempotent(t *testing.T) {
	srv, _ := setupTCPServer(t)
	srv.Stop()
	srv.Stop() // must not panic
}

// TestTCPServerStop_AcceptRaceWithStop covers the race where a connection is
// accepted as Stop() is firing. The conn must either be tracked (and woken)
// or rejected immediately, never leaked.
func TestTCPServerStop_AcceptRaceWithStop(t *testing.T) {
	srv, addr := setupTCPServer(t)

	// Fire a burst of dials concurrent with Stop(). At least some should land
	// before/during the Stop().
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err == nil {
				defer c.Close()
			}
		}()
	}

	start := time.Now()
	srv.Stop()
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Stop() under accept race took %v, expected < 2s", elapsed)
	}
}

// TestTCPServerStop_StressTeardownRace stresses the preemption race between
// the handler's stopCh-check and SetReadDeadline(60s). With GOMAXPROCS > 1
// the scheduler can in principle preempt between those two lines, allowing
// Stop()'s SetReadDeadline(now) to be overridden by the handler's 60s deadline.
// The second stopCh-check after SetReadDeadline closes this race; without it,
// over enough iterations this test would eventually see a multi-second Stop().
//
// We can't deterministically trigger the race, so we iterate. Each iteration
// must individually complete its Stop() in < 1s.
func TestTCPServerStop_StressTeardownRace(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	const iterations = 20
	for i := 0; i < iterations; i++ {
		srv, addr := setupTCPServer(t)

		// Two idle client conns increase the chance the scheduler is busy
		// when Stop() fires, raising the odds of catching the race.
		conns := make([]net.Conn, 4)
		for j := range conns {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("iter %d: dial: %v", i, err)
			}
			conns[j] = c
		}
		time.Sleep(10 * time.Millisecond)

		start := time.Now()
		srv.Stop()
		elapsed := time.Since(start)

		for _, c := range conns {
			c.Close()
		}

		if elapsed > time.Second {
			t.Fatalf("iter %d: Stop() took %v, expected < 1s — race may not be plugged", i, elapsed)
		}
	}
}
