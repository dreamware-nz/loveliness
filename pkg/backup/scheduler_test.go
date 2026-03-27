package backup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupTestShards(t *testing.T, dataDir string, count int) []*shard.Shard {
	t.Helper()
	shards := make([]*shard.Shard, count)
	for i := 0; i < count; i++ {
		// Create dummy shard files that CreateBackup can archive.
		shardPath := filepath.Join(dataDir, fmt.Sprintf("shard-%d", i))
		os.WriteFile(shardPath, []byte(fmt.Sprintf("shard-%d-data", i)), 0644)
		shards[i] = shard.NewShard(i, shard.NewMemoryStore(), 4)
	}
	return shards
}

func TestSchedulerRunNow(t *testing.T) {
	dataDir := t.TempDir()
	storeDir := t.TempDir()
	store, _ := NewLocalStore(storeDir)
	mgr := NewManager(dataDir, "test-node")
	shards := setupTestShards(t, dataDir, 1)

	sched := NewScheduler(mgr, store, shards, func() uint64 { return 42 }, time.Hour, 3)

	manifest, key, err := sched.RunNow()
	if err != nil {
		t.Fatal("RunNow:", err)
	}
	if manifest == nil {
		t.Fatal("expected non-nil manifest")
	}
	if key == "" {
		t.Fatal("expected non-empty key")
	}
	if manifest.WALSequence != 42 {
		t.Errorf("expected WAL seq 42, got %d", manifest.WALSequence)
	}

	metas, _ := store.List()
	if len(metas) != 1 {
		t.Fatalf("expected 1 backup, got %d", len(metas))
	}
}

func TestSchedulerRetention(t *testing.T) {
	dataDir := t.TempDir()
	storeDir := t.TempDir()
	store, _ := NewLocalStore(storeDir)

	// Pre-populate 5 backups.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("backup-%d.tar.gz", i)
		store.Put(key, bytes.NewReader([]byte("data")))
		time.Sleep(10 * time.Millisecond)
	}

	mgr := NewManager(dataDir, "test-node")
	shards := setupTestShards(t, dataDir, 1)

	sched := NewScheduler(mgr, store, shards, func() uint64 { return 1 }, time.Hour, 3)
	_, _, err := sched.RunNow()
	if err != nil {
		t.Fatal("RunNow:", err)
	}

	sched.enforceRetention()

	metas, _ := store.List()
	if len(metas) != 3 {
		t.Errorf("expected 3 backups after retention, got %d", len(metas))
	}
}
