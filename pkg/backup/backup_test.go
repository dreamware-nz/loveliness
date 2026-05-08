package backup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestCreateAndRestoreBackup(t *testing.T) {
	// Create temp dirs for source and destination.
	srcDir, _ := os.MkdirTemp("", "backup-src-*")
	defer os.RemoveAll(srcDir)
	dstDir, _ := os.MkdirTemp("", "backup-dst-*")
	defer os.RemoveAll(dstDir)

	// Create fake shard files.
	for i := 0; i < 2; i++ {
		path := filepath.Join(srcDir, "shard-"+string(rune('0'+i)))
		os.WriteFile(path, []byte("shard data "+string(rune('0'+i))), 0640)
	}

	// Create mock shards that return from Query.
	shards := []*shard.Shard{
		shard.NewShard(0, &mockStore{}, 1),
		shard.NewShard(1, &mockStore{}, 1),
	}

	// Plant Kuzu-style WAL sidecars and a fake raft directory so the
	// round-trip exercises every kind of file the backup is supposed to
	// carry. Without this we'd only test the basic path.
	for i := 0; i < 2; i++ {
		walPath := filepath.Join(srcDir, "shard-"+string(rune('0'+i))+".wal")
		os.WriteFile(walPath, []byte("kuzu-wal-"+string(rune('0'+i))), 0640)
	}
	raftDir := filepath.Join(srcDir, "raft")
	os.MkdirAll(filepath.Join(raftDir, "snapshots", "snap1"), 0750)
	os.WriteFile(filepath.Join(raftDir, "raft-log.bolt"), []byte("bolt-log"), 0640)
	os.WriteFile(filepath.Join(raftDir, "snapshots", "snap1", "state.bin"), []byte("snap-state"), 0640)

	// Create backup.
	mgr := NewManager(srcDir, "test-node")
	var buf bytes.Buffer
	manifest, err := mgr.CreateBackup(&buf, shards, 42, nil)
	if err != nil {
		t.Fatal(err)
	}

	if manifest.Version != 2 {
		t.Fatalf("expected version 2, got %d", manifest.Version)
	}
	if manifest.ShardCount != 2 {
		t.Fatalf("expected 2 shards, got %d", manifest.ShardCount)
	}
	if manifest.WALSequence != 42 {
		t.Fatalf("expected wal seq 42, got %d", manifest.WALSequence)
	}
	if !manifest.IncludesRaft {
		t.Fatalf("expected IncludesRaft=true, got false")
	}

	// Restore to destination.
	dstMgr := NewManager(dstDir, "test-node")
	restored, err := dstMgr.RestoreBackup(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if restored.ShardCount != 2 {
		t.Fatalf("restored shard count: got %d want 2", restored.ShardCount)
	}

	// Verify shard main file + Kuzu WAL sidecar were extracted.
	for i := 0; i < 2; i++ {
		path := filepath.Join(dstDir, "shard-"+string(rune('0'+i)))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("shard-%d not restored: %v", i, err)
		}
		expected := "shard data " + string(rune('0'+i))
		if string(data) != expected {
			t.Fatalf("shard-%d content mismatch: got %q want %q", i, data, expected)
		}
		walPath := path + ".wal"
		walData, err := os.ReadFile(walPath)
		if err != nil {
			t.Fatalf("shard-%d.wal not restored: %v", i, err)
		}
		if string(walData) != "kuzu-wal-"+string(rune('0'+i)) {
			t.Fatalf("shard-%d.wal content mismatch: got %q", i, walData)
		}
	}

	// Verify the raft tree restored under the destination.
	for _, rel := range []string{"raft/raft-log.bolt", "raft/snapshots/snap1/state.bin"} {
		if _, err := os.Stat(filepath.Join(dstDir, rel)); err != nil {
			t.Fatalf("%s not restored: %v", rel, err)
		}
	}
}

// TestRestoreWipesStaleRaft asserts the restore step removes any
// pre-existing raft/ directory before extracting — otherwise stale FSM
// log entries could mix with the restored snapshot.
func TestRestoreWipesStaleRaft(t *testing.T) {
	srcDir, _ := os.MkdirTemp("", "backup-src-*")
	defer os.RemoveAll(srcDir)
	dstDir, _ := os.MkdirTemp("", "backup-dst-*")
	defer os.RemoveAll(dstDir)

	// Source: minimal raft + one file inside it.
	os.MkdirAll(filepath.Join(srcDir, "raft"), 0750)
	os.WriteFile(filepath.Join(srcDir, "raft", "fresh.bin"), []byte("fresh"), 0640)
	os.WriteFile(filepath.Join(srcDir, "shard-0"), []byte("s"), 0640)

	// Destination: stale raft entry that must be removed during restore.
	os.MkdirAll(filepath.Join(dstDir, "raft"), 0750)
	os.WriteFile(filepath.Join(dstDir, "raft", "stale.bin"), []byte("stale"), 0640)

	shards := []*shard.Shard{shard.NewShard(0, &mockStore{}, 1)}
	mgr := NewManager(srcDir, "test-node")
	var buf bytes.Buffer
	if _, err := mgr.CreateBackup(&buf, shards, 0, nil); err != nil {
		t.Fatal(err)
	}
	dst := NewManager(dstDir, "test-node")
	if _, err := dst.RestoreBackup(&buf); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "raft", "stale.bin")); !os.IsNotExist(err) {
		t.Fatalf("stale raft entry should be wiped during restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "raft", "fresh.bin")); err != nil {
		t.Fatalf("fresh raft entry not restored: %v", err)
	}
}

type mockStore struct{}

func (m *mockStore) Query(cypher string) (*shard.QueryResponse, error) {
	return &shard.QueryResponse{
		Columns: []string{"result"},
		Rows:    []map[string]any{{"result": 1}},
	}, nil
}

func (m *mockStore) Close() error { return nil }
