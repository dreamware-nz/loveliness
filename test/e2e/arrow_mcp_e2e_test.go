// Package e2e_test exercises the full Arrow stack across package
// boundaries: a real Loveliness HTTP server (pkg/api) backed by a
// real router + shard, hosting a real MCP server (pkg/mcp) that
// answers the MCP wire protocol. The MCP client connects through an
// in-memory transport, calls cypher_read with format=arrow, and the
// test parses the Arrow IPC bytes off the embedded resource.
//
// Why a separate test package: pkg/mcp deliberately does not import
// pkg/api (the file pkg/mcp/client.go calls out the avoided cycle).
// Putting the cross-stack test here keeps the boundary intact while
// still proving the two halves connect.
package e2e_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/johnjansen/loveliness/pkg/api"
	"github.com/johnjansen/loveliness/pkg/mcp"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// stand-up helpers

func realLovelinessHTTP(t *testing.T, rows []map[string]any) *httptest.Server {
	t.Helper()
	store := shard.NewMemoryStore()
	for i, r := range rows {
		store.PutNode(rowKey(i), r)
	}
	sh := shard.NewShard(0, store, 4)
	rtr := router.NewRouter([]*shard.Shard{sh}, 5*time.Second)
	api := api.NewServer(rtr, nil, []*shard.Shard{sh}, nil, 5*time.Second)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func rowKey(i int) string {
	if i == 0 {
		return "row-0"
	}
	out := []byte{}
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	return "row-" + string(out)
}

// connectMCP wires a real pkg/mcp.Server to a real backend URL, then
// connects an MCP client over in-memory transports. Returns the
// client session and a context with a generous timeout.
func connectMCP(t *testing.T, backendURL string) (*mcpsdk.ClientSession, context.Context) {
	t.Helper()
	mcpServer := mcp.New(mcp.Options{URL: backendURL, Timeout: 5 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	t1, t2 := mcpsdk.NewInMemoryTransports()
	serverSession, err := mcpServer.SDK().Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "e2e", Version: "0.0.1"}, nil)
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	return cs, ctx
}

// TestE2E_FullStack_CypherArrowEmbeddedResource is the headline E2E:
// it proves that a Cypher query issued through the MCP cypher_read
// tool with format=arrow flows all the way through:
//
//	MCP client (in-memory)
//	  -> MCP server (pkg/mcp)
//	    -> HTTP request with Accept: application/vnd.apache.arrow.stream
//	      -> Loveliness API server (pkg/api)
//	        -> router + shard + MemoryStore
//	      -> arrow.go encoder (real apache/arrow-go IPC stream writer)
//	    -> HTTP response
//	  -> Embedded resource on the MCP CallToolResult
//	-> arrow.ipc.Reader on the resource blob (real IPC stream reader)
//
// Anything that goes wrong in any of those layers — wrong MIME, wrong
// header forwarding, wrong content negotiation, broken Arrow encoding,
// the MCP embedded-resource shape — surfaces as a failure here.
func TestE2E_FullStack_CypherArrowEmbeddedResource(t *testing.T) {
	rows := []map[string]any{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(25)},
		{"name": "carol", "age": int64(40)},
	}
	backend := realLovelinessHTTP(t, rows)
	cs, ctx := connectMCP(t, backend.URL)

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "cypher_read",
		Arguments: map[string]any{
			"query":  "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
			"format": "arrow",
		},
	})
	if err != nil {
		t.Fatalf("cypher_read(arrow): %v", err)
	}
	if res.IsError {
		t.Fatalf("cypher_read returned error: %+v", res.Content)
	}

	// The arrow path returns 2 content blocks: a text summary + the
	// embedded resource carrying the Arrow IPC stream.
	if len(res.Content) != 2 {
		t.Fatalf("content blocks = %d, want 2 (text + embedded resource)", len(res.Content))
	}
	er, ok := res.Content[1].(*mcpsdk.EmbeddedResource)
	if !ok {
		t.Fatalf("content[1] = %T, want *mcp.EmbeddedResource", res.Content[1])
	}
	if er.Resource == nil {
		t.Fatal("embedded resource has nil Resource")
	}
	if er.Resource.MIMEType != mcp.ArrowStreamMIME {
		t.Errorf("MIME type = %q, want %q", er.Resource.MIMEType, mcp.ArrowStreamMIME)
	}
	if len(er.Resource.Blob) == 0 {
		t.Fatal("resource blob is empty — Arrow bytes did not propagate")
	}

	// The IPC stream must parse cleanly with the upstream apache/arrow
	// reader. This guards against any byte-level mangling the MCP
	// transport could quietly introduce.
	r, err := ipc.NewReader(bytes.NewReader(er.Resource.Blob), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("ipc.NewReader on resource blob: %v", err)
	}
	defer r.Release()

	schema := r.Schema()
	cols := map[string]bool{}
	for _, f := range schema.Fields() {
		cols[f.Name] = true
	}
	for _, want := range []string{"name", "age"} {
		if !cols[want] {
			t.Errorf("schema missing column %q; have %v", want, cols)
		}
	}

	var totalRows int64
	for r.Next() {
		rec := r.RecordBatch()
		totalRows += rec.NumRows()
	}
	if err := r.Err(); err != nil {
		t.Fatalf("stream reader err: %v", err)
	}
	if totalRows != int64(len(rows)) {
		t.Errorf("rows in blob = %d, want %d", totalRows, len(rows))
	}
}

// TestE2E_FullStack_JSONDefaultStillWorks is the negative control —
// the same plumbing must continue to deliver JSON when format is
// omitted, otherwise the Arrow path could be silently overriding the
// default.
func TestE2E_FullStack_JSONDefaultStillWorks(t *testing.T) {
	rows := []map[string]any{
		{"name": "alice", "age": int64(30)},
	}
	backend := realLovelinessHTTP(t, rows)
	cs, ctx := connectMCP(t, backend.URL)

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "cypher_read",
		Arguments: map[string]any{
			"query": "MATCH (n:Person) RETURN n.name AS name",
		},
	})
	if err != nil {
		t.Fatalf("cypher_read: %v", err)
	}
	if res.IsError {
		t.Fatalf("cypher_read returned error: %+v", res.Content)
	}
	// JSON path: structured output auto-populates, no embedded resource.
	for _, c := range res.Content {
		if _, ok := c.(*mcpsdk.EmbeddedResource); ok {
			t.Errorf("JSON default path must not return EmbeddedResource; got one with content %T", c)
		}
	}
}

// TestE2E_FullStack_ArrowEmptyResultStillRoundTrips proves that an
// empty result set survives the round-trip — the encoder still emits
// a schema (so DuckDB-WASM and friends can parse it as a valid IPC
// stream) and the MCP layer doesn't strip the resource just because
// no rows came back.
func TestE2E_FullStack_ArrowEmptyResultStillRoundTrips(t *testing.T) {
	backend := realLovelinessHTTP(t, nil) // empty store
	cs, ctx := connectMCP(t, backend.URL)

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "cypher_read",
		Arguments: map[string]any{
			"query":  "MATCH (n) RETURN n",
			"format": "arrow",
		},
	})
	if err != nil {
		t.Fatalf("cypher_read(arrow,empty): %v", err)
	}
	if res.IsError {
		t.Fatalf("cypher_read returned error: %+v", res.Content)
	}
	if len(res.Content) < 2 {
		t.Fatalf("expected at least 2 content blocks, got %d", len(res.Content))
	}
	er, ok := res.Content[1].(*mcpsdk.EmbeddedResource)
	if !ok {
		t.Fatalf("content[1] = %T, want *mcp.EmbeddedResource", res.Content[1])
	}
	if len(er.Resource.Blob) == 0 {
		t.Fatal("empty result must still carry a parseable Arrow stream — schema-only is not zero bytes")
	}
	r, err := ipc.NewReader(bytes.NewReader(er.Resource.Blob), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("empty Arrow stream did not parse: %v", err)
	}
	defer r.Release()
	// Schema must be present even with no rows.
	if r.Schema() == nil {
		t.Fatal("empty stream missing schema")
	}
}
