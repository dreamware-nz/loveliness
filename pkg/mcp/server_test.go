package mcp

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestServerToolsViaMCP exercises the server end-to-end using the MCP
// in-memory transports: we stand up a fake Loveliness backend with
// httptest, point the MCP server at it, connect an MCP client in the
// same process, and drive a tool call.
func TestServerToolsViaMCP(t *testing.T) {
	backend := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"columns":["c"],"rows":[{"c":1}]}`))
		},
		"/cluster": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"leader":"127.0.0.1:9000","node_id":"n1","is_leader":true,"shard_map":{},"nodes":[]}`))
		},
	})
	t.Cleanup(backend.Close)

	srv := New(Options{URL: backend.URL, Timeout: 2 * time.Second})
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.1"}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t1, t2 := mcp.NewInMemoryTransports()
	serverSession, err := srv.SDK().Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })

	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// List tools — should include cypher_read and cluster_status.
	lt, err := cs.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range lt.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"cypher_read", "cypher_write", "schema", "cluster_status", "bulk_nodes", "bulk_edges", "ingest_nodes", "ingest_edges", "ingest_status"} {
		if !names[want] {
			t.Errorf("missing tool %q; have %v", want, names)
		}
	}

	// Call cypher_read.
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "cypher_read",
		Arguments: map[string]any{"query": "MATCH (n) RETURN count(n)"},
	})
	if err != nil {
		t.Fatalf("call cypher_read: %v", err)
	}
	if res.IsError {
		t.Fatalf("cypher_read returned error: %v", res.Content)
	}

	// cypher_read should reject a write statement.
	res2, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "cypher_read",
		Arguments: map[string]any{"query": "CREATE (n:Person)"},
	})
	if err != nil {
		t.Fatalf("call cypher_read(write): %v", err)
	}
	if !res2.IsError {
		t.Fatal("expected cypher_read to reject write")
	}
	text := textOf(res2.Content)
	if !strings.Contains(text, "CREATE") {
		t.Errorf("error message should mention CREATE; got %q", text)
	}

	// Call cluster_status.
	res3, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "cluster_status"})
	if err != nil {
		t.Fatalf("call cluster_status: %v", err)
	}
	if res3.IsError {
		t.Fatalf("cluster_status returned error: %v", textOf(res3.Content))
	}
}

func TestServerReadonlyOmitsWriters(t *testing.T) {
	backend := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
		},
	})
	t.Cleanup(backend.Close)

	srv := New(Options{URL: backend.URL, Readonly: true, Timeout: time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t1, t2 := mcp.NewInMemoryTransports()
	ss, err := srv.SDK().Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0.0.1"}, nil)
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	lt, err := cs.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	for _, tool := range lt.Tools {
		switch tool.Name {
		case "cypher_write", "bulk_nodes", "bulk_edges", "ingest_nodes", "ingest_edges":
			t.Errorf("readonly: unexpected write tool %q", tool.Name)
		}
	}
}

func textOf(contents []mcp.Content) string {
	var out strings.Builder
	for _, c := range contents {
		if tc, ok := c.(*mcp.TextContent); ok {
			out.WriteString(tc.Text)
		}
	}
	return out.String()
}
