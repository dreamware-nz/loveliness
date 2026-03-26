package replication

import (
	"os"
	"testing"
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
