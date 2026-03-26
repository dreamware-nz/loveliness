package cluster

import (
	"testing"
)

func TestPlanMoves_DeadPrimaryPromotesReplica(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1", Replica: "node-2"},
		1: {Primary: "node-2", Replica: "node-1"},
	}
	alive := []string{"node-2"} // node-1 is dead

	moves := planMoves(assignments, alive)

	// Shard 0's primary (node-1) is dead, replica (node-2) should be promoted.
	found := false
	for _, m := range moves {
		if m.ShardID == 0 && m.Role == "primary" && m.ToNode == "node-2" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected shard 0 primary moved to node-2, got %+v", moves)
	}
}

func TestPlanMoves_RebalanceOverloaded(t *testing.T) {
	// 4 shards, 2 nodes — all on node-1.
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
		1: {Primary: "node-1"},
		2: {Primary: "node-1"},
		3: {Primary: "node-1"},
	}
	alive := []string{"node-1", "node-2"}

	moves := planMoves(assignments, alive)

	// Should move 2 shards to node-2 for balance (2 each).
	movedToNode2 := 0
	for _, m := range moves {
		if m.Role == "primary" && m.ToNode == "node-2" {
			movedToNode2++
		}
	}
	if movedToNode2 != 2 {
		t.Errorf("expected 2 shards moved to node-2, got %d: %+v", movedToNode2, moves)
	}
}

func TestPlanMoves_AlreadyBalanced(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1", Replica: "node-2"},
		1: {Primary: "node-2", Replica: "node-1"},
	}
	alive := []string{"node-1", "node-2"}

	moves := planMoves(assignments, alive)

	// Should have no primary moves (already balanced).
	primaryMoves := 0
	for _, m := range moves {
		if m.Role == "primary" {
			primaryMoves++
		}
	}
	if primaryMoves != 0 {
		t.Errorf("expected 0 primary moves, got %d: %+v", primaryMoves, moves)
	}
}

func TestPlanMoves_FixMissingReplica(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1", Replica: ""},
	}
	alive := []string{"node-1", "node-2"}

	moves := planMoves(assignments, alive)

	found := false
	for _, m := range moves {
		if m.ShardID == 0 && m.Role == "replica" && m.ToNode == "node-2" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected replica assigned to node-2, got %+v", moves)
	}
}

func TestPlanMoves_NoAliveNodes(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
	}

	moves := planMoves(assignments, nil)
	if len(moves) != 0 {
		t.Errorf("expected no moves with no alive nodes, got %+v", moves)
	}
}

func TestPlanMoves_SingleNode_NoReplica(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1"},
	}
	alive := []string{"node-1"}

	moves := planMoves(assignments, alive)

	// Can't have replicas with 1 node — shouldn't try to assign one.
	for _, m := range moves {
		if m.Role == "replica" {
			t.Errorf("should not assign replica with single node: %+v", m)
		}
	}
}
