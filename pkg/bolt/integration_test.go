package bolt

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"
)

// fullFlowExecutor simulates a real database with schema + data.
type fullFlowExecutor struct {
	data map[string]struct {
		columns []string
		rows    []map[string]any
	}
}

func newFullFlowExecutor() *fullFlowExecutor {
	e := &fullFlowExecutor{
		data: make(map[string]struct {
			columns []string
			rows    []map[string]any
		}),
	}
	// Pre-populate some data.
	e.data["match_person"] = struct {
		columns []string
		rows    []map[string]any
	}{
		columns: []string{"p.name", "p.age", "p.city"},
		rows: []map[string]any{
			{"p.name": "Alice", "p.age": int64(30), "p.city": "Auckland"},
			{"p.name": "Bob", "p.age": int64(25), "p.city": "Wellington"},
			{"p.name": "Charlie", "p.age": int64(35), "p.city": "Christchurch"},
		},
	}
	e.data["count"] = struct {
		columns []string
		rows    []map[string]any
	}{
		columns: []string{"count"},
		rows:    []map[string]any{{"count": int64(15700000)}},
	}
	return e
}

func (e *fullFlowExecutor) Query(cypher string, params map[string]any) ([]string, []map[string]any, error) {
	upper := strings.ToUpper(cypher)
	if strings.Contains(upper, "ERROR") {
		return nil, nil, fmt.Errorf("simulated database error")
	}
	if strings.Contains(upper, "CREATE") {
		return []string{}, nil, nil
	}
	if strings.Contains(upper, "COUNT") {
		d := e.data["count"]
		return d.columns, d.rows, nil
	}
	if strings.Contains(upper, "PERSON") || strings.Contains(upper, "MATCH") {
		d := e.data["match_person"]
		return d.columns, d.rows, nil
	}
	return []string{}, nil, nil
}

// TestFullBoltSession simulates what a real Neo4j driver does:
// connect → handshake → HELLO → RUN → PULL → RUN → PULL → GOODBYE
func TestFullBoltSession(t *testing.T) {
	exec := newFullFlowExecutor()
	srv := startTestServer(t, exec)
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// --- HELLO ---
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(3)
	p.PackString("user_agent")
	p.PackString("neo4j-python/5.x")
	p.PackString("scheme")
	p.PackString("basic")
	p.PackString("routing")
	p.PackMapHeader(0)
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send HELLO:", err)
	}

	resp := recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "HELLO")
	meta := resp.Fields[0].(map[string]any)
	if !strings.Contains(meta["server"].(string), "Loveliness") {
		t.Error("expected Loveliness server identification")
	}

	// --- RUN query 1: MATCH person ---
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (p:Person) RETURN p.name, p.age, p.city")
	p.PackMapHeader(0) // params
	p.PackMapHeader(0) // extra
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send RUN:", err)
	}

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RUN person")
	runMeta := resp.Fields[0].(map[string]any)
	fields := runMeta["fields"].([]any)
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d: %v", len(fields), fields)
	}
	if fields[0].(string) != "p.name" {
		t.Errorf("field 0: expected p.name, got %v", fields[0])
	}

	// --- PULL all records ---
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send PULL:", err)
	}

	// Expect 3 RECORD messages.
	names := []string{}
	for i := 0; i < 3; i++ {
		rec := recvMessage(t, conn)
		assertTag(t, rec, msgRECORD, fmt.Sprintf("RECORD %d", i))
		recFields := rec.Fields[0].([]any)
		if len(recFields) != 3 {
			t.Fatalf("record %d: expected 3 fields, got %d", i, len(recFields))
		}
		names = append(names, recFields[0].(string))
	}

	// Final SUCCESS with has_more=false.
	done := recvMessage(t, conn)
	assertTag(t, done, msgSUCCESS, "PULL done")
	doneMeta := done.Fields[0].(map[string]any)
	if doneMeta["has_more"].(bool) != false {
		t.Error("expected has_more=false")
	}

	t.Logf("Query 1 returned: %v", names)
	if len(names) != 3 || names[0] != "Alice" {
		t.Errorf("unexpected names: %v", names)
	}

	// --- RUN query 2: COUNT ---
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (n) RETURN COUNT(n)")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send RUN count:", err)
	}

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RUN count")

	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send PULL count:", err)
	}

	rec := recvMessage(t, conn)
	assertTag(t, rec, msgRECORD, "count RECORD")
	countFields := rec.Fields[0].([]any)
	count := countFields[0].(int64)
	t.Logf("Query 2 returned count: %d", count)
	if count != 15700000 {
		t.Errorf("expected 15700000, got %d", count)
	}

	done = recvMessage(t, conn)
	assertTag(t, done, msgSUCCESS, "count done")

	// --- RUN with parameters ---
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (p:Person {name: $name}) RETURN p.name, p.age, p.city")
	p.PackMapHeader(1) // params
	p.PackString("name")
	p.PackString("Alice")
	p.PackMapHeader(0) // extra
	if err := sendMessage(conn, p); err != nil {
		t.Fatal("send RUN params:", err)
	}

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RUN with params")

	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	// Should get 3 records (mock doesn't filter, but params were interpolated).
	for i := 0; i < 3; i++ {
		rec = recvMessage(t, conn)
		assertTag(t, rec, msgRECORD, "param RECORD")
	}
	done = recvMessage(t, conn)
	assertTag(t, done, msgSUCCESS, "param done")

	// --- Test error handling ---
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (e:ERROR) RETURN e")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgFAILURE, "error RUN")
	failMeta := resp.Fields[0].(map[string]any)
	if !strings.Contains(failMeta["message"].(string), "simulated") {
		t.Errorf("expected simulated error, got: %v", failMeta["message"])
	}

	// After FAILURE, messages should be IGNORED until RESET.
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgIGNORED, "PULL after failure")

	// RESET recovers the connection.
	p.Reset()
	p.PackStructHeader(0, msgRESET)
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RESET")

	// Can query again after RESET.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (p:Person) RETURN p.name, p.age, p.city")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RUN after RESET")

	// --- GOODBYE ---
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	for i := 0; i < 3; i++ {
		recvMessage(t, conn)
	}
	recvMessage(t, conn) // final SUCCESS

	p.Reset()
	p.PackStructHeader(0, msgGOODBYE)
	sendMessage(conn, p)

	// Connection should be closed by server.
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Error("expected connection closed after GOODBYE")
	}

	t.Log("Full Bolt session completed successfully")
}

// TestBoltExplicitTransaction tests BEGIN → RUN → PULL → COMMIT flow.
func TestBoltExplicitTransaction(t *testing.T) {
	exec := newFullFlowExecutor()
	srv := startTestServer(t, exec)
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// BEGIN.
	p.Reset()
	p.PackStructHeader(1, msgBEGIN)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	resp := recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "BEGIN")

	// RUN inside transaction.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("CREATE (p:Person {name: 'Dave', age: 28})")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "RUN in tx")

	// PULL (CREATE returns 0 rows, so we get SUCCESS immediately
	// but there may be an empty RECORD first if columns exist).
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)
	// Drain any records until we get SUCCESS.
	for {
		resp = recvMessage(t, conn)
		if resp.Tag == msgSUCCESS {
			break
		}
		if resp.Tag != msgRECORD {
			t.Fatalf("PULL in tx: unexpected 0x%02X", resp.Tag)
		}
	}

	// COMMIT.
	p.Reset()
	p.PackStructHeader(0, msgCOMMIT)
	sendMessage(conn, p)
	resp = recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "COMMIT")
}

// TestBoltRouteMessage tests the ROUTE message (driver routing table discovery).
func TestBoltRouteMessage(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// ROUTE.
	p.Reset()
	p.PackStructHeader(3, msgROUTE)
	p.PackMapHeader(0) // routing context
	p.PackListHeader(0) // bookmarks
	p.PackNull()        // database
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	assertTag(t, resp, msgSUCCESS, "ROUTE")
	meta := resp.Fields[0].(map[string]any)
	rt := meta["rt"].(map[string]any)
	if rt["ttl"].(int64) != 300 {
		t.Errorf("expected ttl=300, got %v", rt["ttl"])
	}
	servers := rt["servers"].([]any)
	if len(servers) != 3 {
		t.Fatalf("expected 3 server roles, got %d", len(servers))
	}
	t.Logf("ROUTE returned %d server entries", len(servers))
}

// TestBoltConcurrentConnections tests multiple clients connecting simultaneously.
func TestBoltConcurrentConnections(t *testing.T) {
	exec := newFullFlowExecutor()
	srv := startTestServer(t, exec)

	const numClients = 10
	errs := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", srv.Addr())
			if err != nil {
				errs <- fmt.Errorf("client %d: dial: %w", id, err)
				return
			}
			defer conn.Close()

			// Handshake.
			if _, err := conn.Write(boltMagic); err != nil {
				errs <- fmt.Errorf("client %d: write magic: %w", id, err)
				return
			}
			versions := make([]byte, 16)
			versions[2] = 4
			versions[3] = 4
			if _, err := conn.Write(versions); err != nil {
				errs <- fmt.Errorf("client %d: write versions: %w", id, err)
				return
			}
			agreed := make([]byte, 4)
			if _, err := conn.Read(agreed); err != nil {
				errs <- fmt.Errorf("client %d: version: %w", id, err)
				return
			}

			// HELLO.
			p := NewPacker()
			p.PackStructHeader(1, msgHELLO)
			p.PackMapHeader(0)
			WriteMessage(conn, p.Bytes())
			data, err := ReadMessage(conn)
			if err != nil {
				errs <- fmt.Errorf("client %d: hello: %w", id, err)
				return
			}
			u := NewUnpacker(bytes.NewReader(data))
			v, _ := u.Unpack()
			s := v.(*BoltStruct)
			if s.Tag != msgSUCCESS {
				errs <- fmt.Errorf("client %d: expected SUCCESS, got 0x%02X", id, s.Tag)
				return
			}

			// RUN + PULL.
			p.Reset()
			p.PackStructHeader(3, msgRUN)
			p.PackString("MATCH (p:Person) RETURN p.name, p.age, p.city")
			p.PackMapHeader(0)
			p.PackMapHeader(0)
			WriteMessage(conn, p.Bytes())
			data, _ = ReadMessage(conn)
			u = NewUnpacker(bytes.NewReader(data))
			v, _ = u.Unpack()
			if v.(*BoltStruct).Tag != msgSUCCESS {
				errs <- fmt.Errorf("client %d: RUN failed", id)
				return
			}

			p.Reset()
			p.PackStructHeader(1, msgPULL)
			p.PackMapHeader(1)
			p.PackString("n")
			p.PackInt(-1)
			WriteMessage(conn, p.Bytes())

			// Read 3 records + SUCCESS.
			for j := 0; j < 4; j++ {
				_, err := ReadMessage(conn)
				if err != nil {
					errs <- fmt.Errorf("client %d: pull %d: %w", id, j, err)
					return
				}
			}

			errs <- nil
		}(i)
	}

	for i := 0; i < numClients; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// TestChunkingLargeMessage tests that large messages are chunked correctly.
func TestChunkingLargeMessage(t *testing.T) {
	// Create a large result set.
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{
			"name": fmt.Sprintf("Person-%d-with-a-really-long-name-to-make-this-bigger", i),
			"age":  int64(i),
			"bio":  strings.Repeat("x", 200),
		}
	}
	exec := &mockExecutor{
		columns: []string{"name", "age", "bio"},
		rows:    rows,
	}
	srv := startTestServer(t, exec)
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// RUN.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (p:Person) RETURN p.name, p.age, p.bio")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// PULL all 1000.
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	recordCount := 0
	for {
		msg := recvMessage(t, conn)
		if msg.Tag == msgRECORD {
			recordCount++
		} else if msg.Tag == msgSUCCESS {
			break
		} else {
			t.Fatalf("unexpected message 0x%02X", msg.Tag)
		}
	}

	if recordCount != 1000 {
		t.Errorf("expected 1000 records, got %d", recordCount)
	}
	t.Logf("Streamed %d records successfully", recordCount)
}

func assertTag(t *testing.T, msg *BoltStruct, expected byte, context string) {
	t.Helper()
	if msg.Tag != expected {
		detail := ""
		if msg.Tag == msgFAILURE && len(msg.Fields) > 0 {
			if m, ok := msg.Fields[0].(map[string]any); ok {
				detail = fmt.Sprintf(" (%v)", m["message"])
			}
		}
		t.Fatalf("%s: expected 0x%02X, got 0x%02X%s", context, expected, msg.Tag, detail)
	}
}
