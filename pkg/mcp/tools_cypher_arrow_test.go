package mcp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fakeArrowCypherServer answers /cypher with either a JSON envelope
// or an Arrow IPC stream depending on the Accept header — the same
// content negotiation contract pkg/api enforces. The MCP integration
// can be unit-tested against this without standing up a real
// cluster.
func fakeArrowCypherServer(t *testing.T) (*httptest.Server, []byte) {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()

	var arrowBuf bytes.Buffer
	w := ipc.NewWriter(&arrowBuf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("write arrow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close arrow: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Accept") {
		case ArrowStreamMIME:
			w.Header().Set("Content-Type", ArrowStreamMIME)
			_, _ = w.Write(arrowBuf.Bytes())
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"columns":["n"],"rows":[{"n":1},{"n":2},{"n":3}],"stats":{"ms":1}}`))
		}
	}))
	return srv, arrowBuf.Bytes()
}

func TestClientCypherArrow_AcceptHeaderAndBytes(t *testing.T) {
	srv, want := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	got, err := c.CypherArrow(context.Background(), "MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("CypherArrow: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("arrow bytes mismatch: got %d bytes, want %d", len(got), len(want))
	}
	// Verify the bytes are a parseable IPC stream — a cheap check that
	// guards against the response-body machinery quietly mangling
	// binary data.
	r, err := ipc.NewReader(bytes.NewReader(got), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("ipc.NewReader: %v", err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal("expected one record batch")
	}
}

func TestRunCypher_JSONIsDefault(t *testing.T) {
	srv, _ := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, out, err := runCypher(context.Background(), c, CypherInput{Query: "MATCH (n) RETURN n"})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	if res != nil {
		t.Fatalf("JSON path must return a nil CallToolResult so structured output auto-populates; got %+v", res)
	}
	if len(out.Rows) != 3 {
		t.Errorf("rows = %d, want 3", len(out.Rows))
	}
}

func TestRunCypher_ExplicitJSON(t *testing.T) {
	srv, _ := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, out, err := runCypher(context.Background(), c, CypherInput{
		Query:  "MATCH (n) RETURN n",
		Format: "json",
	})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	if res != nil {
		t.Fatalf("JSON path must return nil CallToolResult, got %+v", res)
	}
	if len(out.Rows) != 3 {
		t.Errorf("rows = %d, want 3", len(out.Rows))
	}
}

func TestRunCypher_ArrowReturnsEmbeddedResource(t *testing.T) {
	srv, wantBytes := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, out, err := runCypher(context.Background(), c, CypherInput{
		Query:  "MATCH (n) RETURN n",
		Format: "arrow",
	})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	if res == nil {
		t.Fatal("arrow path must return an explicit CallToolResult")
	}
	if len(out.Rows) != 0 {
		t.Errorf("arrow path must skip JSON rows; got %d", len(out.Rows))
	}
	if len(res.Content) != 2 {
		t.Fatalf("expected 2 content blocks (text summary + embedded resource), got %d", len(res.Content))
	}

	if _, ok := res.Content[0].(*mcp.TextContent); !ok {
		t.Errorf("first content block must be TextContent, got %T", res.Content[0])
	}
	er, ok := res.Content[1].(*mcp.EmbeddedResource)
	if !ok {
		t.Fatalf("second content block must be EmbeddedResource, got %T", res.Content[1])
	}
	if er.Resource == nil {
		t.Fatal("embedded resource has nil Resource")
	}
	if er.Resource.MIMEType != ArrowStreamMIME {
		t.Errorf("MIME type = %q, want %q", er.Resource.MIMEType, ArrowStreamMIME)
	}
	if !bytes.Equal(er.Resource.Blob, wantBytes) {
		t.Errorf("resource blob does not match the upstream Arrow bytes (got %d bytes, want %d)",
			len(er.Resource.Blob), len(wantBytes))
	}
}

func TestRunCypher_ArrowResourceBlobIsParseableIPCStream(t *testing.T) {
	srv, _ := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, _, err := runCypher(context.Background(), c, CypherInput{
		Query:  "MATCH (n) RETURN n",
		Format: "arrow",
	})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	er := res.Content[1].(*mcp.EmbeddedResource)

	r, err := ipc.NewReader(bytes.NewReader(er.Resource.Blob), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("ipc.NewReader on resource blob: %v", err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal("expected one record batch in the embedded blob")
	}
	rec := r.RecordBatch()
	if rec.NumRows() != 3 {
		t.Errorf("rows in blob = %d, want 3", rec.NumRows())
	}
}

func TestRunCypher_RejectsUnknownFormat(t *testing.T) {
	srv, _ := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, _, err := runCypher(context.Background(), c, CypherInput{
		Query:  "MATCH (n) RETURN n",
		Format: "parquet",
	})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	if res == nil || !res.IsError {
		t.Fatalf("expected IsError result for unknown format, got %+v", res)
	}
}

func TestRunCypher_ArrowAcceptsJSONUppercase(t *testing.T) {
	// Format strings should be case-insensitive — JSON, Arrow, ARROW
	// all need to work, since LLMs often shout.
	srv, _ := fakeArrowCypherServer(t)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	res, _, err := runCypher(context.Background(), c, CypherInput{
		Query:  "MATCH (n) RETURN n",
		Format: "ARROW",
	})
	if err != nil {
		t.Fatalf("runCypher: %v", err)
	}
	if res == nil {
		t.Fatal("expected CallToolResult for uppercase ARROW format")
	}
	if _, ok := res.Content[1].(*mcp.EmbeddedResource); !ok {
		t.Errorf("expected EmbeddedResource for ARROW format")
	}
}
