package cluster

import (
	"testing"
)

func TestRoundRobin_PrimaryDistribution(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	got := make(map[string]int)
	for i := 0; i < 9; i++ {
		p := RoundRobinStrategy{}.Place(i, nodes, 1, nil)
		got[p.Primary]++
	}
	for _, n := range nodes {
		if got[n] != 3 {
			t.Errorf("expected 3 primaries on %s, got %d (full map: %v)", n, got[n], got)
		}
	}
}

func TestRoundRobin_ReplicasNeverColocateWithPrimary(t *testing.T) {
	nodes := []string{"a", "b", "c", "d"}
	for i := 0; i < 12; i++ {
		p := RoundRobinStrategy{}.Place(i, nodes, 3, nil)
		if len(p.Replicas) != 2 {
			t.Fatalf("shard %d: want 2 replicas, got %d (%v)", i, len(p.Replicas), p.Replicas)
		}
		for _, r := range p.Replicas {
			if r == p.Primary {
				t.Errorf("shard %d: replica colocated with primary %s", i, p.Primary)
			}
		}
	}
}

func TestRoundRobin_RFClampedToNodeCount(t *testing.T) {
	nodes := []string{"a", "b"}
	p := RoundRobinStrategy{}.Place(0, nodes, 5, nil)
	// rf=5 with 2 nodes → primary + at most 1 replica, no panic.
	if len(p.Replicas) != 1 {
		t.Errorf("rf clamp expected 1 replica, got %d (%v)", len(p.Replicas), p.Replicas)
	}
	if p.Primary != "a" || p.Replicas[0] != "b" {
		t.Errorf("unexpected placement: %+v", p)
	}
}

func TestRoundRobin_RFOneEmptyReplicas(t *testing.T) {
	p := RoundRobinStrategy{}.Place(7, []string{"a", "b", "c"}, 1, nil)
	if p.Primary != "b" {
		t.Errorf("expected primary=b for shard 7 across 3 nodes, got %s", p.Primary)
	}
	if len(p.Replicas) != 0 {
		t.Errorf("rf=1 should produce no replicas, got %v", p.Replicas)
	}
}

func TestRoundRobin_BumpsEpochWhenCurrentExists(t *testing.T) {
	nodes := []string{"a", "b"}
	cur := &ShardAssignment{Primary: "a", Replicas: []string{"b"}, Epoch: 4}
	p := RoundRobinStrategy{}.Place(0, nodes, 2, cur)
	if p.Epoch != 5 {
		t.Errorf("expected epoch=5 (4+1), got %d", p.Epoch)
	}
}

func TestRoundRobin_NoNodes(t *testing.T) {
	p := RoundRobinStrategy{}.Place(0, nil, 2, nil)
	if p.Primary != "" || len(p.Replicas) != 0 {
		t.Errorf("expected empty placement for no nodes, got %+v", p)
	}
}
