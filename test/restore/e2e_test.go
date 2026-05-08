//go:build integration
// +build integration

// Package restore_test spins up a real single-node Loveliness cluster,
// writes data, takes a backup via GET /backup, stops the cluster, wipes
// the data dir, runs `loveliness restore --file ...`, restarts the
// cluster, and asserts the data is back.
//
// Run with:
//
//	go test -tags=integration ./test/restore/...
//
// Requires LadybugDB to be installed locally (CGO build of `loveliness`).
package restore_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func buildLoveliness(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "loveliness")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/loveliness")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	cmd.Dir = repoRoot(t)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("skipping: go build loveliness failed (LadybugDB likely not installed): %v\n%s", err, b)
	}
	return bin
}

// pickFreePorts returns three free TCP ports for HTTP, Raft, gRPC.
func pickFreePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = l
		ports[i] = l.Addr().(*net.TCPAddr).Port
	}
	for _, l := range listeners {
		l.Close()
	}
	return ports
}

func waitUp(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("server at %s never became ready", url)
}

// startServer launches `loveliness serve` with the given data dir and
// returns the running command and the HTTP base URL. The caller must
// call stop() to terminate it cleanly.
func startServer(t *testing.T, lovelinessBin, dataDir string, ports []int) (cmd *exec.Cmd, baseURL string, stop func()) {
	t.Helper()
	httpAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])
	raftAddr := fmt.Sprintf("127.0.0.1:%d", ports[1])
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", ports[2])
	baseURL = "http://" + httpAddr

	cmd = exec.Command(lovelinessBin, "serve")
	cmd.Env = append(os.Environ(),
		"LOVELINESS_NODE_ID=restore-e2e",
		"LOVELINESS_BIND_ADDR="+httpAddr,
		"LOVELINESS_RAFT_ADDR="+raftAddr,
		"LOVELINESS_GRPC_ADDR="+grpcAddr,
		"LOVELINESS_BOLT_ADDR=",
		"LOVELINESS_DATA_DIR="+dataDir,
		"LOVELINESS_SHARD_COUNT=1",
		"LOVELINESS_BOOTSTRAP=true",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start loveliness: %v", err)
	}
	stop = func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}
	waitUp(t, baseURL+"/health", 30*time.Second)
	return cmd, baseURL, stop
}

// runCypher posts a query and returns the parsed JSON body.
func runCypher(t *testing.T, baseURL, cypher string) map[string]any {
	t.Helper()
	resp, err := http.Post(baseURL+"/cypher", "text/plain", strings.NewReader(cypher))
	if err != nil {
		t.Fatalf("cypher: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cypher %d: %s", resp.StatusCode, body)
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("parse cypher response: %v\n%s", err, body)
	}
	return parsed
}

// TestRestoreRoundTrip is the smoke test for the disaster-recovery
// pipeline: write a node, snapshot, wipe the data dir, restore from
// the archive, and confirm the node is queryable again.
func TestRestoreRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	lovelinessBin := buildLoveliness(t)
	dataDir := t.TempDir()
	ports := pickFreePorts(t, 3)

	// Boot, write, snapshot.
	_, baseURL, stop := startServer(t, lovelinessBin, dataDir, ports)
	runCypher(t, baseURL, "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")
	runCypher(t, baseURL, "CREATE (p:Person {name: 'Alice', age: 30})")
	runCypher(t, baseURL, "CREATE (p:Person {name: 'Bob', age: 25})")

	// Pull the archive.
	resp, err := http.Get(baseURL + "/backup")
	if err != nil {
		t.Fatalf("get /backup: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("backup %d: %s", resp.StatusCode, body)
	}
	archive, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read /backup: %v", err)
	}
	archivePath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	if err := os.WriteFile(archivePath, archive, 0640); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	// Stop the server before touching the data dir — Kuzu holds file
	// handles and would overwrite restored bytes on shutdown.
	stop()

	// Wipe the data dir.
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("wipe data dir: %v", err)
	}
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		t.Fatalf("recreate data dir: %v", err)
	}

	// Run `loveliness restore --file ...` with the same NODE_ID so
	// the cross-cluster guard is satisfied.
	restoreCmd := exec.Command(lovelinessBin, "restore",
		"--file", archivePath,
		"--data-dir", dataDir,
	)
	restoreCmd.Env = append(os.Environ(), "LOVELINESS_NODE_ID=restore-e2e")
	out, err := restoreCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("restored archive")) {
		t.Fatalf("unexpected restore output: %s", out)
	}

	// Restart and query.
	_, baseURL, stop2 := startServer(t, lovelinessBin, dataDir, ports)
	defer stop2()

	got := runCypher(t, baseURL, "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.name")
	rows, _ := got["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("after restore expected 2 rows, got %d (raw: %v)", len(rows), got)
	}
	first, _ := rows[0].(map[string]any)
	if first["name"] != "Alice" {
		t.Fatalf("after restore expected Alice first, got %v", first)
	}
}

// TestRestoreManifestOnly confirms the --manifest-only flag prints
// the manifest without touching the data dir.
func TestRestoreManifestOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	lovelinessBin := buildLoveliness(t)
	dataDir := t.TempDir()
	ports := pickFreePorts(t, 3)

	_, baseURL, stop := startServer(t, lovelinessBin, dataDir, ports)
	runCypher(t, baseURL, "CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id))")
	resp, err := http.Get(baseURL + "/backup")
	if err != nil {
		t.Fatalf("get /backup: %v", err)
	}
	archive, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	stop()

	archivePath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	_ = os.WriteFile(archivePath, archive, 0640)

	intactBefore, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("stat data dir: %v", err)
	}

	cmd := exec.Command(lovelinessBin, "restore",
		"--file", archivePath,
		"--data-dir", dataDir,
		"--manifest-only",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("manifest-only restore: %v\n%s", err, out)
	}
	var manifest map[string]any
	if err := json.Unmarshal(out, &manifest); err != nil {
		t.Fatalf("parse manifest output: %v\n%s", err, out)
	}
	if manifest["version"] == nil {
		t.Fatalf("manifest missing version: %v", manifest)
	}
	if manifest["node_id"] != "restore-e2e" {
		t.Fatalf("manifest node_id mismatch: %v", manifest["node_id"])
	}

	// Data dir should be untouched.
	intactAfter, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("stat data dir after: %v", err)
	}
	if len(intactBefore) != len(intactAfter) {
		t.Fatalf("data dir entry count changed: before=%d after=%d", len(intactBefore), len(intactAfter))
	}
}
