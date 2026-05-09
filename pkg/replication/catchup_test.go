package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestCatchupShard_RecordsTimestamps(t *testing.T) {
	dir, _ := os.MkdirTemp("", "catchup-ts-*")
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Append(0, "CREATE (n {id: 1})")
	w.Append(0, "CREATE (n {id: 2})")
	headTS := w.HeadTimestamp(0)

	shards := []*shard.Shard{shard.NewShard(0, shard.NewMemoryStore(), 4)}
	state := NewReplicaState()
	cm := NewCatchupManager(w, state, shards)

	replayed, err := cm.CatchupShard(context.Background(), 0, "node-2")
	if err != nil {
		t.Fatal(err)
	}
	if replayed != 2 {
		t.Errorf("expected 2 entries replayed, got %d", replayed)
	}

	if got := state.GetPosition(0, "node-2"); got != 2 {
		t.Errorf("expected position 2, got %d", got)
	}
	appliedTS := state.GetTimestamp(0, "node-2")
	if appliedTS.IsZero() {
		t.Error("applied timestamp must be recorded after catchup")
	}
	// The applied timestamp should equal the head timestamp (since we
	// replayed every entry).
	if !appliedTS.Equal(headTS) {
		t.Errorf("applied timestamp %v != head timestamp %v", appliedTS, headTS)
	}
	// Time-lag at this point should be ~0 because applied == head.
	if !appliedTS.Equal(headTS) {
		t.Errorf("time lag should be zero, got applied %v vs head %v", appliedTS, headTS)
	}
}

func TestCatchupShard_LagBytesShrinksOnReplay(t *testing.T) {
	dir, _ := os.MkdirTemp("", "catchup-bytes-*")
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir)
	defer w.Close()

	w.Append(0, "q1")
	w.Append(0, "q2")
	w.Append(0, "q3")

	state := NewReplicaState()
	shards := []*shard.Shard{shard.NewShard(0, shard.NewMemoryStore(), 4)}
	cm := NewCatchupManager(w, state, shards)

	before := w.LagBytes(0, state.GetPosition(0, "node-2"))
	if before == 0 {
		t.Fatal("expected non-zero lag before catchup")
	}

	if _, err := cm.CatchupShard(context.Background(), 0, "node-2"); err != nil {
		t.Fatal(err)
	}

	after := w.LagBytes(0, state.GetPosition(0, "node-2"))
	if after != 0 {
		t.Errorf("expected zero lag bytes after full catchup, got %d", after)
	}

	// Spot check timestamps to avoid unused-import flakiness if test grows.
	_ = time.Now()
}
