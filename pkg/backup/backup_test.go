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

	// Create backup.
	mgr := NewManager(srcDir, "test-node")
	var buf bytes.Buffer
	manifest, err := mgr.CreateBackup(&buf, shards, 42)
	if err != nil {
		t.Fatal(err)
	}

	if manifest.Version != 1 {
		t.Fatalf("expected version 1, got %d", manifest.Version)
	}
	if manifest.ShardCount != 2 {
		t.Fatalf("expected 2 shards, got %d", manifest.ShardCount)
	}
	if manifest.WALSequence != 42 {
		t.Fatalf("expected wal seq 42, got %d", manifest.WALSequence)
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

	// Verify files were extracted.
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
