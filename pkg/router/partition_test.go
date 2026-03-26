package router

import (
	"testing"
)

func TestLabelPropagation_SingleCommunity(t *testing.T) {
	shards := makeTestShards(2)
	r := NewRouter(shards, 5e9)
	pa := NewPartitionAnalyzer(shards, r)

	// All nodes connected — should converge to one community.
	nodes := []nodeInfo{
		{key: "A", shardID: 0},
		{key: "B", shardID: 0},
		{key: "C", shardID: 1},
	}
	edges := []edgeInfo{
		{from: "A", to: "B"},
		{from: "B", to: "C"},
		{from: "A", to: "C"},
	}

	labels := pa.labelPropagation(nodes, edges, 10)

	// All should have the same label (single community).
	labelSet := make(map[int]bool)
	for _, lbl := range labels {
		labelSet[lbl] = true
	}
	if len(labelSet) != 1 {
		t.Errorf("expected 1 community, got %d: %v", len(labelSet), labels)
	}
}

func TestLabelPropagation_TwoCommunities(t *testing.T) {
	shards := makeTestShards(2)
	r := NewRouter(shards, 5e9)
	pa := NewPartitionAnalyzer(shards, r)

	// Two disjoint cliques: {A,B,C} and {D,E,F}.
	nodes := []nodeInfo{
		{key: "A", shardID: 0},
		{key: "B", shardID: 0},
		{key: "C", shardID: 0},
		{key: "D", shardID: 1},
		{key: "E", shardID: 1},
		{key: "F", shardID: 1},
	}
	edges := []edgeInfo{
		{from: "A", to: "B"}, {from: "B", to: "C"}, {from: "A", to: "C"},
		{from: "D", to: "E"}, {from: "E", to: "F"}, {from: "D", to: "F"},
	}

	labels := pa.labelPropagation(nodes, edges, 10)

	// A, B, C should share one label; D, E, F should share another.
	if labels["A"] != labels["B"] || labels["B"] != labels["C"] {
		t.Errorf("clique 1 not unified: A=%d, B=%d, C=%d", labels["A"], labels["B"], labels["C"])
	}
	if labels["D"] != labels["E"] || labels["E"] != labels["F"] {
		t.Errorf("clique 2 not unified: D=%d, E=%d, F=%d", labels["D"], labels["E"], labels["F"])
	}
	if labels["A"] == labels["D"] {
		t.Error("expected two distinct communities, got one")
	}
}

func TestComputeMigrationPlan(t *testing.T) {
	shards := makeTestShards(2)
	r := NewRouter(shards, 5e9)
	pa := NewPartitionAnalyzer(shards, r)

	// Two communities scattered across shards:
	// Clique 1 {A,B,C} all on shard 0, Clique 2 {D,E,F} all on shard 1.
	// Edges are only within each clique — no cross-shard edges.
	// But also add a badly-placed node G on shard 0 connected only to {D,E,F}.
	nodes := []nodeInfo{
		{key: "A", shardID: 0},
		{key: "B", shardID: 0},
		{key: "C", shardID: 0},
		{key: "D", shardID: 1},
		{key: "E", shardID: 1},
		{key: "F", shardID: 1},
		{key: "G", shardID: 0}, // misplaced — belongs with D,E,F
	}
	edges := []edgeInfo{
		{from: "A", to: "B"}, {from: "B", to: "C"}, {from: "A", to: "C"},
		{from: "D", to: "E"}, {from: "E", to: "F"}, {from: "D", to: "F"},
		{from: "G", to: "D"}, {from: "G", to: "E"}, {from: "G", to: "F"},
	}

	labels := pa.labelPropagation(nodes, edges, 10)
	plan := pa.computeMigrationPlan(nodes, edges, labels, "Person")

	// G should be migrated to join {D,E,F}, reducing cross-shard edges.
	if plan.CurrentCuts == 0 {
		t.Fatal("expected some current cross-shard edges")
	}
	if plan.ProjectedCuts > plan.CurrentCuts {
		t.Errorf("expected projected cuts (%d) <= current cuts (%d)",
			plan.ProjectedCuts, plan.CurrentCuts)
	}
	if plan.Communities < 2 {
		t.Errorf("expected at least 2 communities, got %d", plan.Communities)
	}
}

func TestComputeMigrationPlan_EmptyGraph(t *testing.T) {
	shards := makeTestShards(2)
	r := NewRouter(shards, 5e9)
	pa := NewPartitionAnalyzer(shards, r)

	plan := pa.computeMigrationPlan(nil, nil, nil, "Person")
	if plan.CurrentCuts != 0 || plan.ProjectedCuts != 0 || len(plan.Moves) != 0 {
		t.Errorf("expected empty plan for empty graph, got %+v", plan)
	}
}
