package cluster

import (
	"testing"
)

func TestCrossShardStats_RecordAndTop(t *testing.T) {
	s := NewCrossShardStats()

	s.RecordTraversal(0, 1)
	s.RecordTraversal(0, 1)
	s.RecordTraversal(0, 1)
	s.RecordTraversal(1, 2)

	top := s.TopCrossings(10)
	if len(top) != 2 {
		t.Fatalf("expected 2 crossings, got %d", len(top))
	}
	// Highest count first.
	if top[0].FromShard != 0 || top[0].ToShard != 1 || top[0].Count != 3 {
		t.Errorf("expected (0→1, 3), got %+v", top[0])
	}
	if top[1].FromShard != 1 || top[1].ToShard != 2 || top[1].Count != 1 {
		t.Errorf("expected (1→2, 1), got %+v", top[1])
	}
}

func TestCrossShardStats_SameShardIgnored(t *testing.T) {
	s := NewCrossShardStats()

	s.RecordTraversal(0, 0) // same shard, should be ignored
	s.RecordTraversal(1, 1)

	if s.TotalCrossings() != 0 {
		t.Errorf("expected 0 crossings for same-shard, got %d", s.TotalCrossings())
	}
}

func TestCrossShardStats_Reset(t *testing.T) {
	s := NewCrossShardStats()
	s.RecordTraversal(0, 1)
	s.Reset()

	if s.TotalCrossings() != 0 {
		t.Errorf("expected 0 after reset, got %d", s.TotalCrossings())
	}
}

func TestPartitioner_SuggestColocation(t *testing.T) {
	s := NewCrossShardStats()
	// Lots of traffic between shard 0 and shard 1.
	for i := 0; i < 100; i++ {
		s.RecordTraversal(0, 1)
	}
	// Little traffic between shard 2 and shard 3.
	s.RecordTraversal(2, 3)

	p := NewPartitioner(s, 10) // threshold of 10

	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-2"},
		2: {Primary: "node-1"},
		3: {Primary: "node-2"},
	}

	suggestions := p.Suggest(assignments)

	// Should suggest colocating shard 0 and 1 (100 crossings > threshold 10).
	// Should NOT suggest shard 2 and 3 (1 crossing < threshold 10).
	if len(suggestions) != 1 {
		t.Fatalf("expected 1 suggestion, got %d: %+v", len(suggestions), suggestions)
	}
	if suggestions[0].ShardA != 0 || suggestions[0].ShardB != 1 {
		t.Errorf("expected shard 0↔1 suggestion, got %+v", suggestions[0])
	}
}

func TestPartitioner_AlreadyColocated(t *testing.T) {
	s := NewCrossShardStats()
	for i := 0; i < 100; i++ {
		s.RecordTraversal(0, 1)
	}

	p := NewPartitioner(s, 10)

	// Both shards already on same node — no suggestion needed.
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-1"},
	}

	suggestions := p.Suggest(assignments)
	if len(suggestions) != 0 {
		t.Errorf("expected 0 suggestions when already colocated, got %+v", suggestions)
	}
}

func TestPartitioner_BelowThreshold(t *testing.T) {
	s := NewCrossShardStats()
	s.RecordTraversal(0, 1) // only 1 crossing

	p := NewPartitioner(s, 10) // threshold 10

	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-2"},
	}

	suggestions := p.Suggest(assignments)
	if len(suggestions) != 0 {
		t.Errorf("expected 0 suggestions below threshold, got %+v", suggestions)
	}
}
