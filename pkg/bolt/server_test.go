package bolt

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// mockExecutor implements QueryExecutor for tests.
type mockExecutor struct {
	columns []string
	rows    []map[string]any
	err     error
}

func (m *mockExecutor) Query(cypher string, params map[string]any) ([]string, []map[string]any, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	return m.columns, m.rows, nil
}

func startTestServer(t *testing.T, exec QueryExecutor) *Server {
	t.Helper()
	srv := NewServer("127.0.0.1:0", exec)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

func boltConnect(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	// Send magic preamble.
	if _, err := conn.Write(boltMagic); err != nil {
		t.Fatal("write magic:", err)
	}

	// Send version proposals: [4.4, 0, 0, 0]
	versions := make([]byte, 16)
	versions[2] = 4 // minor
	versions[3] = 4 // major
	if _, err := conn.Write(versions); err != nil {
		t.Fatal("write versions:", err)
	}

	// Read agreed version.
	agreed := make([]byte, 4)
	if _, err := io.ReadFull(conn, agreed); err != nil {
		t.Fatal("read agreed version:", err)
	}
	if agreed[3] != 4 {
		t.Fatalf("expected Bolt 4.x, got %v", agreed)
	}

	return conn
}

func sendMessage(conn net.Conn, p *Packer) error {
	return WriteMessage(conn, p.Bytes())
}

func recvMessage(t *testing.T, conn net.Conn) *BoltStruct {
	t.Helper()
	data, err := ReadMessage(conn)
	if err != nil {
		t.Fatal("recv:", err)
	}
	u := NewUnpacker(bytes.NewReader(data))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal("unpack:", err)
	}
	s, ok := v.(*BoltStruct)
	if !ok {
		t.Fatalf("expected struct, got %T", v)
	}
	return s
}

func TestBoltHandshake(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// Send HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(1)
	p.PackString("user_agent")
	p.PackString("test/1.0")
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS, got 0x%02X", resp.Tag)
	}

	meta := resp.Fields[0].(map[string]any)
	if meta["server"].(string) != "Loveliness/1.0.0" {
		t.Errorf("unexpected server: %v", meta["server"])
	}
}

func TestBoltRunAndPull(t *testing.T) {
	exec := &mockExecutor{
		columns: []string{"name", "age"},
		rows: []map[string]any{
			{"name": "Alice", "age": int64(30)},
			{"name": "Bob", "age": int64(25)},
		},
	}
	srv := startTestServer(t, exec)
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(1)
	p.PackString("user_agent")
	p.PackString("test/1.0")
	sendMessage(conn, p)
	recvMessage(t, conn) // SUCCESS

	// RUN.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (p:Person) RETURN p.name, p.age")
	p.PackMapHeader(0) // params
	p.PackMapHeader(0) // extra
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("RUN: expected SUCCESS, got 0x%02X", resp.Tag)
	}
	meta := resp.Fields[0].(map[string]any)
	fields := meta["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	// PULL all.
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	// Should get 2 RECORD messages then SUCCESS.
	r1 := recvMessage(t, conn)
	if r1.Tag != msgRECORD {
		t.Fatalf("expected RECORD, got 0x%02X", r1.Tag)
	}
	r2 := recvMessage(t, conn)
	if r2.Tag != msgRECORD {
		t.Fatalf("expected RECORD, got 0x%02X", r2.Tag)
	}

	done := recvMessage(t, conn)
	if done.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS after records, got 0x%02X", done.Tag)
	}
	doneMeta := done.Fields[0].(map[string]any)
	if doneMeta["has_more"].(bool) != false {
		t.Error("expected has_more=false")
	}
}

func TestBoltPullBatched(t *testing.T) {
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = map[string]any{"n": int64(i)}
	}
	exec := &mockExecutor{
		columns: []string{"n"},
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
	p.PackString("MATCH (n) RETURN n")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn) // SUCCESS

	// PULL 3 at a time.
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(3)
	sendMessage(conn, p)

	// 3 records + SUCCESS with has_more=true.
	for i := 0; i < 3; i++ {
		r := recvMessage(t, conn)
		if r.Tag != msgRECORD {
			t.Fatalf("record %d: expected RECORD, got 0x%02X", i, r.Tag)
		}
	}
	s := recvMessage(t, conn)
	if s.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS, got 0x%02X", s.Tag)
	}
	meta := s.Fields[0].(map[string]any)
	if meta["has_more"].(bool) != true {
		t.Error("expected has_more=true")
	}
}

func TestBoltParamInterpolation(t *testing.T) {
	cases := []struct {
		cypher   string
		params   map[string]any
		expected string
	}{
		{
			"MATCH (p {name: $name}) RETURN p",
			map[string]any{"name": "Alice"},
			"MATCH (p {name: 'Alice'}) RETURN p",
		},
		{
			"MATCH (p) WHERE p.age > $age RETURN p",
			map[string]any{"age": int64(25)},
			"MATCH (p) WHERE p.age > 25 RETURN p",
		},
		{
			"MATCH (p {active: $flag}) RETURN p",
			map[string]any{"flag": true},
			"MATCH (p {active: true}) RETURN p",
		},
		{
			"RETURN $val",
			map[string]any{"val": nil},
			"RETURN NULL",
		},
	}

	for _, tc := range cases {
		got := interpolateParams(tc.cypher, tc.params)
		if got != tc.expected {
			t.Errorf("interpolate(%q, %v)\n  got:  %q\n  want: %q", tc.cypher, tc.params, got, tc.expected)
		}
	}
}

func TestBoltReset(t *testing.T) {
	exec := &mockExecutor{
		columns: []string{"n"},
		rows:    []map[string]any{{"n": int64(1)}},
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

	// RESET should always succeed.
	p.Reset()
	p.PackStructHeader(0, msgRESET)
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on RESET, got 0x%02X", resp.Tag)
	}
}

func TestBoltLogoff(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// LOGOFF — should succeed and allow re-auth.
	p.Reset()
	p.PackStructHeader(0, msgLOGOFF)
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on LOGOFF, got 0x%02X", resp.Tag)
	}

	// After LOGOFF, LOGON should work to re-authenticate.
	p.Reset()
	p.PackStructHeader(1, msgLOGON)
	p.PackMapHeader(0)
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on LOGON after LOGOFF, got 0x%02X", resp.Tag)
	}
}

func TestBoltTelemetry(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// TELEMETRY — should be accepted silently.
	p.Reset()
	p.PackStructHeader(1, msgTELEMETRY)
	p.PackInt(1) // api_type
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on TELEMETRY, got 0x%02X", resp.Tag)
	}
}

func TestBoltBookmarks(t *testing.T) {
	exec := &mockExecutor{
		columns: []string{"n"},
		rows:    []map[string]any{{"n": int64(1)}},
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

	// RUN + PULL — should get bookmark in final SUCCESS.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (n) RETURN n")
	p.PackMapHeader(0)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn) // RUN SUCCESS

	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)

	recvMessage(t, conn) // RECORD

	done := recvMessage(t, conn)
	if done.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS, got 0x%02X", done.Tag)
	}
	meta := done.Fields[0].(map[string]any)
	bm, ok := meta["bookmark"].(string)
	if !ok || bm == "" {
		t.Fatal("expected bookmark in PULL SUCCESS metadata")
	}
	if bm != "loveliness:v1:tx1" {
		t.Errorf("expected loveliness:v1:tx1, got %s", bm)
	}

	// BEGIN with bookmark — should be accepted.
	p.Reset()
	p.PackStructHeader(1, msgBEGIN)
	p.PackMapHeader(1)
	p.PackString("bookmarks")
	p.PackListHeader(1)
	p.PackString(bm)
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on BEGIN with bookmark, got 0x%02X", resp.Tag)
	}
}

func TestBoltMultiDatabaseParam(t *testing.T) {
	exec := &mockExecutor{
		columns: []string{"n"},
		rows:    []map[string]any{{"n": int64(1)}},
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

	// RUN with db in extra map — should be accepted.
	p.Reset()
	p.PackStructHeader(3, msgRUN)
	p.PackString("MATCH (n) RETURN n")
	p.PackMapHeader(0)
	p.PackMapHeader(1)
	p.PackString("db")
	p.PackString("neo4j")
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on RUN with db param, got 0x%02X", resp.Tag)
	}

	// BEGIN with db param.
	p.Reset()
	p.PackStructHeader(1, msgPULL)
	p.PackMapHeader(1)
	p.PackString("n")
	p.PackInt(-1)
	sendMessage(conn, p)
	recvMessage(t, conn) // RECORD
	recvMessage(t, conn) // SUCCESS

	p.Reset()
	p.PackStructHeader(1, msgBEGIN)
	p.PackMapHeader(1)
	p.PackString("db")
	p.PackString("mydb")
	sendMessage(conn, p)

	resp = recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS on BEGIN with db, got 0x%02X", resp.Tag)
	}
}

func TestBoltGraphTypeWrapping(t *testing.T) {
	// Test that nodeWrapper and relWrapper get packed as Bolt Node/Relationship structs.
	p := NewPacker()

	// Pack a nodeWrapper via PackValue.
	node := &nodeWrapper{
		ID:    42,
		Label: "Person",
		Props: map[string]any{"name": "Alice", "age": int64(30)},
	}
	p.PackValue(node)

	// Unpack and verify it's a struct with tag 0x4E (Node).
	u := NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err := u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(*BoltStruct)
	if !ok {
		t.Fatalf("expected BoltStruct, got %T", v)
	}
	if s.Tag != tagNode {
		t.Fatalf("expected Node tag 0x4E, got 0x%02X", s.Tag)
	}
	if len(s.Fields) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(s.Fields))
	}
	if s.Fields[0].(int64) != 42 {
		t.Errorf("expected ID=42, got %v", s.Fields[0])
	}
	labels := s.Fields[1].([]any)
	if len(labels) != 1 || labels[0].(string) != "Person" {
		t.Errorf("expected labels=[Person], got %v", labels)
	}

	// Pack a relWrapper via PackValue.
	p.Reset()
	rel := &relWrapper{
		ID:      99,
		StartID: 42,
		EndID:   43,
		Type:    "KNOWS",
		Props:   map[string]any{"since": int64(2020)},
	}
	p.PackValue(rel)

	u = NewUnpacker(bytes.NewReader(p.Bytes()))
	v, err = u.Unpack()
	if err != nil {
		t.Fatal(err)
	}
	s, ok = v.(*BoltStruct)
	if !ok {
		t.Fatalf("expected BoltStruct, got %T", v)
	}
	if s.Tag != tagRelationship {
		t.Fatalf("expected Relationship tag 0x52, got 0x%02X", s.Tag)
	}
	if len(s.Fields) != 8 {
		t.Fatalf("expected 8 fields, got %d", len(s.Fields))
	}
	if s.Fields[0].(int64) != 99 {
		t.Errorf("expected ID=99, got %v", s.Fields[0])
	}
	if s.Fields[3].(string) != "KNOWS" {
		t.Errorf("expected type=KNOWS, got %v", s.Fields[3])
	}
}

func TestBoltNodeDetection(t *testing.T) {
	// Test that RowToFields wraps nodes and relationships correctly.
	columns := []string{"p"}
	row := map[string]any{
		"p": map[string]any{
			"_label": "Person",
			"_id":    "alice",
			"name":   "Alice",
			"age":    int64(30),
		},
	}

	fields := RowToFields(columns, row)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}

	nw, ok := fields[0].(*nodeWrapper)
	if !ok {
		t.Fatalf("expected *nodeWrapper, got %T", fields[0])
	}
	if nw.Label != "Person" {
		t.Errorf("expected label=Person, got %s", nw.Label)
	}
	if nw.Props["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", nw.Props["name"])
	}
	// Internal keys should be stripped.
	if _, has := nw.Props["_label"]; has {
		t.Error("_label should not be in props")
	}
	if _, has := nw.Props["_id"]; has {
		t.Error("_id should not be in props")
	}
}

func TestBoltRelDetection(t *testing.T) {
	columns := []string{"r"}
	row := map[string]any{
		"r": map[string]any{
			"_type": "KNOWS",
			"_src":  "alice",
			"_dst":  "bob",
			"since": int64(2020),
		},
	}

	fields := RowToFields(columns, row)
	rw, ok := fields[0].(*relWrapper)
	if !ok {
		t.Fatalf("expected *relWrapper, got %T", fields[0])
	}
	if rw.Type != "KNOWS" {
		t.Errorf("expected type=KNOWS, got %s", rw.Type)
	}
	if rw.Props["since"] != int64(2020) {
		t.Errorf("expected since=2020, got %v", rw.Props["since"])
	}
}

func TestBoltAuth_RejectsBadCredentials(t *testing.T) {
	srv := NewServer("127.0.0.1:0", &mockExecutor{})
	srv.SetAuth("my-bolt-secret")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO with wrong credentials.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(2)
	p.PackString("user_agent")
	p.PackString("test/1.0")
	p.PackString("credentials")
	p.PackString("wrong-secret")
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgFAILURE {
		t.Fatalf("expected FAILURE, got 0x%02X", resp.Tag)
	}
	meta := resp.Fields[0].(map[string]any)
	if meta["code"].(string) != "Neo.ClientError.Security.Unauthorized" {
		t.Errorf("expected Unauthorized error code, got %v", meta["code"])
	}
}

func TestBoltAuth_AcceptsValidCredentials(t *testing.T) {
	srv := NewServer("127.0.0.1:0", &mockExecutor{})
	srv.SetAuth("my-bolt-secret")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO with correct credentials.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(2)
	p.PackString("user_agent")
	p.PackString("test/1.0")
	p.PackString("credentials")
	p.PackString("my-bolt-secret")
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS, got 0x%02X", resp.Tag)
	}
}

func TestBoltAuth_NoAuthConfigAcceptsAnything(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	// No SetAuth call — auth disabled.

	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO with no credentials field at all.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(1)
	p.PackString("user_agent")
	p.PackString("test/1.0")
	sendMessage(conn, p)

	resp := recvMessage(t, conn)
	if resp.Tag != msgSUCCESS {
		t.Fatalf("expected SUCCESS with no auth, got 0x%02X", resp.Tag)
	}
}

func TestBoltGoodbye(t *testing.T) {
	srv := startTestServer(t, &mockExecutor{})
	conn := boltConnect(t, srv.Addr())
	defer conn.Close()

	// HELLO.
	p := NewPacker()
	p.PackStructHeader(1, msgHELLO)
	p.PackMapHeader(0)
	sendMessage(conn, p)
	recvMessage(t, conn)

	// GOODBYE — server should close the connection.
	p.Reset()
	p.PackStructHeader(0, msgGOODBYE)
	sendMessage(conn, p)

	// Next read should fail (connection closed by server).
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Error("expected connection to be closed after GOODBYE")
	}
}
