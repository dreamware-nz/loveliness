package cluster

import (
	"testing"
)

// fakeRebalancerCluster is a minimal rebalancerCluster that records
// AssignShard calls into an in-memory shard map. Used to test Execute
// without spinning up Raft.
type fakeRebalancerCluster struct {
	sm ShardMap
}

func (f *fakeRebalancerCluster) GetShardMap() ShardMap { return f.sm }
func (f *fakeRebalancerCluster) AssignShard(shardID int, primary string, replicas []string) error {
	if f.sm.Assignments == nil {
		f.sm.Assignments = map[int]ShardAssignment{}
	}
	f.sm.Assignments[shardID] = ShardAssignment{Primary: primary, Replicas: replicas}
	return nil
}

func TestPlanMoves_DeadPrimaryPromotesReplica(t *testing.T) {
	assignments := map[int]ShardAssignment{
		0: {Primary: "node-1", Replicas: []string{"node-2"}},
		1: {Primary: "node-2", Replicas: []string{"node-1"}},
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
		0: {Primary: "node-1", Replicas: []string{"node-2"}},
		1: {Primary: "node-2", Replicas: []string{"node-1"}},
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
		0: {Primary: "node-1"},
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

func TestExecute_PromotionDropsNewPrimaryFromReplicas(t *testing.T) {
	// node-1 is dead, node-2 was its replica. Promotion should land
	// the shard at primary=node-2 with replicas that no longer
	// contain node-2 (no duplicate) or node-1 (dead).
	fake := &fakeRebalancerCluster{
		sm: ShardMap{
			Nodes: map[string]NodeInfo{
				"node-1": {ID: "node-1", Alive: false},
				"node-2": {ID: "node-2", Alive: true},
				"node-3": {ID: "node-3", Alive: true},
			},
			Assignments: map[int]ShardAssignment{
				0: {Primary: "node-1", Replicas: []string{"node-2", "node-3"}},
			},
		},
	}
	r := &Rebalancer{cluster: fake}
	moves := r.Plan()
	if err := r.Execute(moves); err != nil {
		t.Fatalf("execute: %v", err)
	}

	got := fake.sm.Assignments[0]
	if got.Primary != "node-2" {
		t.Errorf("expected primary=node-2, got %q", got.Primary)
	}
	for _, r := range got.Replicas {
		if r == "node-2" {
			t.Errorf("new primary node-2 must not appear in replicas: %v", got.Replicas)
		}
		if r == "node-1" {
			t.Errorf("dead primary node-1 must not appear in replicas: %v", got.Replicas)
		}
	}
}

func TestExecute_PromotionWithoutReplicaList(t *testing.T) {
	// No replicas at all — promotion picks the least-loaded alive node
	// and replicas should remain empty (and certainly no duplicates).
	fake := &fakeRebalancerCluster{
		sm: ShardMap{
			Nodes: map[string]NodeInfo{
				"node-1": {ID: "node-1", Alive: false},
				"node-2": {ID: "node-2", Alive: true},
			},
			Assignments: map[int]ShardAssignment{
				0: {Primary: "node-1"},
			},
		},
	}
	r := &Rebalancer{cluster: fake}
	if err := r.Execute(r.Plan()); err != nil {
		t.Fatalf("execute: %v", err)
	}

	got := fake.sm.Assignments[0]
	if got.Primary != "node-2" {
		t.Errorf("expected primary=node-2, got %q", got.Primary)
	}
	for _, rep := range got.Replicas {
		if rep == "node-2" || rep == "node-1" {
			t.Errorf("promotion should not put primary or dead node into replicas: %v", got.Replicas)
		}
	}
}

func TestFilterReplicas_DropsTargetsAndEmpties(t *testing.T) {
	got := filterReplicas([]string{"node-2", "", "node-1", "node-3"}, "node-2", "node-1")
	want := []string{"node-3"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Errorf("filterReplicas: got %v, want %v", got, want)
	}
}
