//go:build integration
// +build integration

// Package mcp_test spins up a real single-node Loveliness cluster and a
// real loveliness-mcp subprocess wired to it, then drives the MCP server
// with an in-process MCP Go client via CommandTransport.
//
// Run with:
//
//	go test -tags=integration ./test/mcp/...
//
// Requires LadybugDB to be installed locally (CGO build of `loveliness`).
package mcp_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildBinaries compiles loveliness and loveliness-mcp into a tempdir and
// returns their absolute paths. Skips the test if the build fails.
func buildBinaries(t *testing.T) (lovelinessBin, mcpBin string) {
	t.Helper()
	dir := t.TempDir()
	lovelinessBin = filepath.Join(dir, "loveliness")
	mcpBin = filepath.Join(dir, "loveliness-mcp")

	build := func(out, pkg string, cgo string) {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Env = append(os.Environ(), "CGO_ENABLED="+cgo)
		cmd.Dir = repoRoot(t)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Skipf("skipping: go build %s failed (LadybugDB likely not installed): %v\n%s", pkg, err, b)
		}
	}
	build(lovelinessBin, "./cmd/loveliness", "1")
	build(mcpBin, "./cmd/loveliness-mcp", "0")
	return lovelinessBin, mcpBin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// test/mcp/ -> repo root is two levels up
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func TestMCPE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	lovelinessBin, mcpBin := buildBinaries(t)

	// Start a single-node Loveliness cluster.
	dataDir := t.TempDir()
	srv := exec.Command(lovelinessBin, "serve")
	srv.Env = append(os.Environ(),
		"LOVELINESS_NODE_ID=e2e-node",
		"LOVELINESS_BIND_ADDR=127.0.0.1:18080",
		"LOVELINESS_RAFT_ADDR=127.0.0.1:19000",
		"LOVELINESS_GRPC_ADDR=127.0.0.1:19001",
		"LOVELINESS_BOLT_ADDR=",
		"LOVELINESS_DATA_DIR="+dataDir,
		"LOVELINESS_SHARD_COUNT=1",
		"LOVELINESS_BOOTSTRAP=true",
	)
	srv.Stdout = os.Stderr
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		t.Fatalf("start loveliness: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Process.Signal(os.Interrupt)
		_ = srv.Wait()
	})

	// Wait for the HTTP endpoint to accept a health probe.
	waitUp(t, "http://127.0.0.1:18080/health", 30*time.Second)

	// Start the MCP server and wire via CommandTransport.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, mcpBin, "--url", "http://127.0.0.1:18080", "--timeout", "10s")
	transport := &mcp.CommandTransport{Command: cmd}

	client := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "0.0.1"}, nil)
	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// schema on an empty DB returns zero tables.
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "schema"}); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// cypher_write: create a node table and insert a row.
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "cypher_write",
		Arguments: map[string]any{
			"query": "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))",
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "cypher_write",
		Arguments: map[string]any{
			"query": "CREATE (p:Person {name: 'Alice', age: 30})",
		},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// cypher_read: MATCH the row.
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "cypher_read",
		Arguments: map[string]any{
			"query": "MATCH (p:Person {name: 'Alice'}) RETURN p.age",
		},
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if res.IsError {
		t.Fatalf("match returned error: %v", res.Content)
	}

	// cluster_status: smoke.
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "cluster_status"}); err != nil {
		t.Fatalf("cluster_status: %v", err)
	}
}

func waitUp(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("server at %s did not come up in %s", url, timeout)
}
