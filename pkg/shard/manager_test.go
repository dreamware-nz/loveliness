package shard

import (
	"sort"
	"testing"
)

func TestManager_OpenShardsOnAssignment(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1", Replica: "node-2"},
		1: {Primary: "node-2", Replica: "node-1"},
		2: {Primary: "node-2", Replica: "node-3"},
	})

	// node-1 is primary for 0, replica for 1. Not assigned 2.
	if !m.IsLocal(0) {
		t.Error("shard 0 should be local (primary)")
	}
	if !m.IsLocal(1) {
		t.Error("shard 1 should be local (replica)")
	}
	if m.IsLocal(2) {
		t.Error("shard 2 should NOT be local")
	}
	if m.ShardCount() != 2 {
		t.Errorf("expected 2 local shards, got %d", m.ShardCount())
	}
}

func TestManager_CloseShardsOnReassignment(t *testing.T) {
	m := NewTestManager("node-1")

	// Initially assigned shards 0 and 1.
	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-1"},
	})
	if m.ShardCount() != 2 {
		t.Fatalf("expected 2 shards, got %d", m.ShardCount())
	}

	// Reassign: shard 1 moves away, shard 2 arrives.
	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-2"},
		2: {Primary: "node-1"},
	})

	if !m.IsLocal(0) {
		t.Error("shard 0 should still be local")
	}
	if m.IsLocal(1) {
		t.Error("shard 1 should be closed (reassigned away)")
	}
	if !m.IsLocal(2) {
		t.Error("shard 2 should be opened (newly assigned)")
	}
}

func TestManager_PrimaryAndReplica(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1", Replica: "node-2"},
		1: {Primary: "node-2", Replica: "node-1"},
	})

	// Node hosts both: shard 0 as primary, shard 1 as replica.
	ids := m.LocalShardIDs()
	sort.Ints(ids)
	if len(ids) != 2 || ids[0] != 0 || ids[1] != 1 {
		t.Errorf("expected shards [0, 1], got %v", ids)
	}
}

func TestManager_GetShard_NotLocal(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-2"},
	})

	if s := m.GetShard(0); s != nil {
		t.Error("shard 0 should not be local")
	}
	if s := m.GetShard(99); s != nil {
		t.Error("nonexistent shard should return nil")
	}
}

func TestManager_GetShard_Local(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1"},
	})

	s := m.GetShard(0)
	if s == nil {
		t.Fatal("shard 0 should be local")
	}
	if s.ID != 0 {
		t.Errorf("expected shard ID 0, got %d", s.ID)
	}
}

func TestManager_Close(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-1"},
	})

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.ShardCount() != 0 {
		t.Errorf("expected 0 shards after close, got %d", m.ShardCount())
	}
}

func TestManager_GetLocalShards(t *testing.T) {
	m := NewTestManager("node-1")

	m.UpdateAssignments(map[int]Assignment{
		0: {Primary: "node-1"},
		3: {Primary: "node-1"},
		7: {Primary: "node-2", Replica: "node-1"},
	})

	shards := m.GetLocalShards()
	if len(shards) != 3 {
		t.Errorf("expected 3 local shards, got %d", len(shards))
	}
}

func TestManager_IdempotentUpdate(t *testing.T) {
	m := NewTestManager("node-1")

	assignments := map[int]Assignment{
		0: {Primary: "node-1"},
	}

	m.UpdateAssignments(assignments)
	s1 := m.GetShard(0)

	// Same assignment again — shard should be the same instance (not reopened).
	m.UpdateAssignments(assignments)
	s2 := m.GetShard(0)

	if s1 != s2 {
		t.Error("idempotent update should not reopen existing shards")
	}
}
