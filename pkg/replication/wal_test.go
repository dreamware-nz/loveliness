package replication

import (
	"os"
	"testing"
	"time"
)

func TestWALAppendAndRead(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Append entries to two shards.
	seq1, _ := w.Append(0, "CREATE (n:Person {name: 'Alice'})")
	seq2, _ := w.Append(1, "CREATE (n:Person {name: 'Bob'})")
	seq3, _ := w.Append(0, "CREATE (n:Person {name: 'Charlie'})")

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Fatalf("expected sequences 1,2,3 got %d,%d,%d", seq1, seq2, seq3)
	}
	if w.LastSequence() != 3 {
		t.Fatalf("expected last sequence 3, got %d", w.LastSequence())
	}

	// Read shard 0 from beginning.
	entries, err := w.ReadFrom(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries for shard 0, got %d", len(entries))
	}
	if entries[0].Sequence != 1 || entries[1].Sequence != 3 {
		t.Fatalf("unexpected sequences: %d, %d", entries[0].Sequence, entries[1].Sequence)
	}

	// Read shard 0 from after seq 1.
	entries, err = w.ReadFrom(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Sequence != 3 {
		t.Fatalf("expected 1 entry after seq 1, got %d", len(entries))
	}

	// Read shard 1.
	entries, err = w.ReadFrom(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Cypher != "CREATE (n:Person {name: 'Bob'})" {
		t.Fatalf("unexpected shard 1 entries: %v", entries)
	}

	// Read non-existent shard.
	entries, err = w.ReadFrom(99, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for shard 99, got %d", len(entries))
	}
}

func TestWALRecovery(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-recovery-*")
	defer os.RemoveAll(dir)

	// Write entries then close.
	w1, _ := NewWAL(dir)
	w1.Append(0, "query 1")
	w1.Append(0, "query 2")
	w1.Append(1, "query 3")
	w1.Close()

	// Reopen — should recover sequence.
	w2, err := NewWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if w2.LastSequence() != 3 {
		t.Fatalf("expected recovered sequence 3, got %d", w2.LastSequence())
	}

	// New entries should continue from 3.
	seq, _ := w2.Append(0, "query 4")
	if seq != 4 {
		t.Fatalf("expected sequence 4 after recovery, got %d", seq)
	}
}

func TestWALTruncate(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-truncate-*")
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir)
	w.Append(0, "q1")
	w.Append(0, "q2")
	w.Append(0, "q3")
	w.Append(0, "q4")

	// Truncate up to seq 2 — should keep seq 3 and 4.
	if err := w.Truncate(0, 2); err != nil {
		t.Fatal(err)
	}

	entries, _ := w.ReadFrom(0, 0)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after truncate, got %d", len(entries))
	}
	if entries[0].Sequence != 3 || entries[1].Sequence != 4 {
		t.Fatalf("unexpected entries after truncate: %v", entries)
	}

	// Can still append after truncate.
	seq, _ := w.Append(0, "q5")
	if seq != 5 {
		t.Fatalf("expected seq 5 after truncate+append, got %d", seq)
	}
	w.Close()
}

func TestWALShardSequence(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-shardseq-*")
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir)
	defer w.Close()

	w.Append(0, "a")
	w.Append(1, "b")
	w.Append(0, "c")

	if w.ShardSequence(0) != 3 {
		t.Fatalf("expected shard 0 seq 3, got %d", w.ShardSequence(0))
	}
	if w.ShardSequence(1) != 2 {
		t.Fatalf("expected shard 1 seq 2, got %d", w.ShardSequence(1))
	}
	if w.ShardSequence(99) != 0 {
		t.Fatalf("expected shard 99 seq 0, got %d", w.ShardSequence(99))
	}
}

func TestWAL_HeadTimestamp(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-headts-*")
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir)
	defer w.Close()

	// Empty shard — zero time.
	if !w.HeadTimestamp(0).IsZero() {
		t.Error("empty shard head timestamp must be zero")
	}

	before := time.Now()
	w.Append(0, "q1")
	after := time.Now()

	got := w.HeadTimestamp(0)
	if got.Before(before) || got.After(after) {
		t.Errorf("head timestamp %v outside [%v, %v]", got, before, after)
	}

	// Second append — head advances.
	time.Sleep(10 * time.Millisecond)
	w.Append(0, "q2")
	if !w.HeadTimestamp(0).After(got) {
		t.Errorf("head timestamp must advance on append")
	}
}

func TestWAL_LagBytes(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-lagbytes-*")
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir)
	defer w.Close()

	w.Append(0, "q1")
	w.Append(0, "q2")
	w.Append(0, "q3")

	// Caught up — zero lag.
	if got := w.LagBytes(0, 3); got != 0 {
		t.Errorf("expected 0 lag bytes when caught up, got %d", got)
	}

	// Behind by all 3 entries — non-zero lag.
	all := w.LagBytes(0, 0)
	if all <= 0 {
		t.Errorf("expected positive lag bytes for fully-behind replica, got %d", all)
	}

	// Behind by last entry only — strictly less than `all`.
	one := w.LagBytes(0, 2)
	if one <= 0 || one >= all {
		t.Errorf("expected partial < total lag, got partial=%d total=%d", one, all)
	}

	// Non-existent shard — zero.
	if got := w.LagBytes(99, 0); got != 0 {
		t.Errorf("expected 0 lag bytes for missing shard, got %d", got)
	}
}

func TestWAL_HeadTimestampSurvivesReopen(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-headts-recover-*")
	defer os.RemoveAll(dir)

	w1, _ := NewWAL(dir)
	w1.Append(0, "q1")
	w1.Append(0, "q2")
	first := w1.HeadTimestamp(0)
	w1.Close()

	w2, _ := NewWAL(dir)
	defer w2.Close()
	// HeadTimestamp(0) is zero until the file is reopened for write — but
	// once an Append happens the cached state must advance from the
	// recovered head, not start fresh.
	if seq := w2.LastSequence(); seq != 2 {
		t.Fatalf("expected recovered sequence 2, got %d", seq)
	}
	w2.Append(0, "q3")
	got := w2.HeadTimestamp(0)
	if !got.After(first) {
		t.Errorf("after reopen+append head should advance past prior head")
	}
}

func TestReplicaState_TimestampTracking(t *testing.T) {
	rs := NewReplicaState()
	now := time.Now()

	rs.SetPositionAt(0, "node-2", 5, now)
	if got := rs.GetPosition(0, "node-2"); got != 5 {
		t.Errorf("expected position 5, got %d", got)
	}
	if got := rs.GetTimestamp(0, "node-2"); !got.Equal(now) {
		t.Errorf("expected timestamp %v, got %v", now, got)
	}

	// Older sequence ignored.
	earlier := now.Add(-time.Hour)
	rs.SetPositionAt(0, "node-2", 3, earlier)
	if got := rs.GetTimestamp(0, "node-2"); !got.Equal(now) {
		t.Errorf("regression — old timestamp overwrote newer one: %v", got)
	}

	// Plain SetPosition path still works (zero time).
	rs.SetPosition(1, "node-3", 10)
	if got := rs.GetPosition(1, "node-3"); got != 10 {
		t.Errorf("plain SetPosition broken, got %d", got)
	}
}
