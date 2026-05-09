package leiden

import (
	"math"
	"reflect"
	"testing"
)

// addUndirectedEdges helps tests express graphs as edge lists.
func addUndirectedEdges(g *Graph, edges [][2]int) {
	for _, e := range edges {
		g.AddEdge(e[0], e[1], 1.0)
	}
}

// TestRun_TwoDisjointTriangles is the classic sanity check: a graph
// of two triangles with no inter-triangle edges must yield exactly
// two communities at γ=1, with the two halves cleanly separated.
func TestRun_TwoDisjointTriangles(t *testing.T) {
	g := NewGraph(6)
	addUndirectedEdges(g, [][2]int{{0, 1}, {1, 2}, {2, 0}, {3, 4}, {4, 5}, {5, 3}})

	res := Run(g, 1.0, 42, 0)
	if res.NumComms != 2 {
		t.Errorf("expected 2 communities, got %d (comms=%v)", res.NumComms, res.Communities)
	}
	// Nodes 0,1,2 share a community; 3,4,5 share another.
	if res.Communities[0] != res.Communities[1] || res.Communities[1] != res.Communities[2] {
		t.Errorf("triangle 1 split: %v", res.Communities)
	}
	if res.Communities[3] != res.Communities[4] || res.Communities[4] != res.Communities[5] {
		t.Errorf("triangle 2 split: %v", res.Communities)
	}
	if res.Communities[0] == res.Communities[3] {
		t.Errorf("triangles should be in different communities: %v", res.Communities)
	}
}

// TestRun_SingleClique: a fully-connected graph at γ=1 should resolve
// to one community (modularity is maximised by keeping it whole — no
// split improves Q for a clique with no resolution boost).
func TestRun_SingleClique(t *testing.T) {
	const n = 6
	g := NewGraph(n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			g.AddEdge(i, j, 1.0)
		}
	}
	res := Run(g, 1.0, 1, 0)
	if res.NumComms != 1 {
		t.Errorf("clique at γ=1 should be one community, got %d (%v)", res.NumComms, res.Communities)
	}
}

// TestRun_HighGammaShattersClique: at γ well above any internal-edge
// gain, every node should be alone in its own community. For an
// 8-clique with unit weights, γ=5 is far past the shatter threshold.
func TestRun_HighGammaShattersClique(t *testing.T) {
	const n = 8
	g := NewGraph(n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			g.AddEdge(i, j, 1.0)
		}
	}
	res := Run(g, 5.0, 1, 0)
	if res.NumComms != n {
		t.Errorf("γ=5 should fully fragment an %d-clique into %d communities, got %d (%v)",
			n, n, res.NumComms, res.Communities)
	}
}

// TestRun_GraphWithSelfLoops verifies the self-loop convention is
// internally consistent. A clique whose nodes carry self-loops should
// still resolve to one community at γ=1.
func TestRun_GraphWithSelfLoops(t *testing.T) {
	const n = 5
	g := NewGraph(n)
	for i := 0; i < n; i++ {
		g.AddEdge(i, i, 0.5) // self-loop on every node
		for j := i + 1; j < n; j++ {
			g.AddEdge(i, j, 1.0)
		}
	}
	res := Run(g, 1.0, 1, 0)
	if res.NumComms != 1 {
		t.Errorf("clique with self-loops at γ=1 should be 1 community, got %d (%v)",
			res.NumComms, res.Communities)
	}
	// For any complete graph with everything in one community,
	// Σ_in = Σ_tot = 2m, so Q = 1 - 1 = 0 at γ=1. This locks in
	// that the A_ii=w convention is consistent end-to-end (NodeWeight,
	// TotalWeight, and modularity's sIn all agree on self-loop weight).
	if math.Abs(res.Modularity) > 1e-9 {
		t.Errorf("expected Q=0 for one-community complete graph, got %v", res.Modularity)
	}
}

// TestAggregate_PreservesTotalWeight: 2m must be invariant under
// aggregation. A direct check that the aggregate routine doesn't
// inflate (or shrink) total edge weight.
func TestAggregate_PreservesTotalWeight(t *testing.T) {
	g := NewGraph(6)
	addUndirectedEdges(g, [][2]int{
		{0, 1}, {1, 2}, {2, 0}, // triangle A
		{3, 4}, {4, 5}, {5, 3}, // triangle B
		{2, 3},                 // bridge
	})
	// Group: nodes 0..2 in label 100, nodes 3..5 in label 200.
	// (Labels need not be 0-indexed; aggregate normalises.)
	sub := []int{100, 100, 100, 200, 200, 200}
	originalTW := g.TotalWeight
	agg, _ := aggregate(g, sub)
	if math.Abs(agg.TotalWeight-originalTW) > 1e-9 {
		t.Errorf("aggregate altered TotalWeight: original=%v aggregated=%v",
			originalTW, agg.TotalWeight)
	}
}

// TestAggregate_SelfLoopFromInternalEdges: if every edge in a graph
// is internal to one community, the aggregate should be a single
// node with a self-loop whose weight equals 2 × (sum of original
// edge weights), matching the modularity 2m invariance.
func TestAggregate_SelfLoopFromInternalEdges(t *testing.T) {
	g := NewGraph(3)
	addUndirectedEdges(g, [][2]int{{0, 1}, {1, 2}, {2, 0}})
	originalTW := g.TotalWeight // 2m = 6 (3 edges × 2)
	sub := []int{42, 42, 42}
	agg, mapping := aggregate(g, sub)
	if len(mapping) != 1 {
		t.Fatalf("expected 1 super-node, got %d", len(mapping))
	}
	if math.Abs(agg.TotalWeight-originalTW) > 1e-9 {
		t.Errorf("2m not preserved: original=%v aggregated=%v", originalTW, agg.TotalWeight)
	}
	// Single node should carry one self-loop (with the aggregated weight).
	if len(agg.Adj[0]) != 1 || agg.Adj[0][0].To != 0 {
		t.Errorf("expected single self-loop on the super-node, got Adj[0]=%v", agg.Adj[0])
	}
}

// TestRun_EmptyGraph: zero nodes, zero edges — should not crash and
// should return an empty result.
func TestRun_EmptyGraph(t *testing.T) {
	g := NewGraph(0)
	res := Run(g, 1.0, 0, 0)
	if res.NumComms != 0 {
		t.Errorf("empty graph: NumComms=%d", res.NumComms)
	}
	if len(res.Communities) != 0 {
		t.Errorf("empty graph: Communities=%v", res.Communities)
	}
}

// TestRun_IsolatedNodes: nodes with no edges. Each isolated node
// is its own community.
func TestRun_IsolatedNodes(t *testing.T) {
	g := NewGraph(4)
	res := Run(g, 1.0, 0, 0)
	if res.NumComms != 4 {
		t.Errorf("4 isolated nodes should be 4 communities, got %d", res.NumComms)
	}
}

// TestRun_Deterministic: same seed, same graph, same γ → same partition.
func TestRun_Deterministic(t *testing.T) {
	build := func() *Graph {
		g := NewGraph(10)
		// Two near-cliques with one bridge.
		for _, e := range [][2]int{
			{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 0}, {0, 2}, {1, 3},
			{5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 5}, {5, 7}, {6, 8},
			{4, 5}, // bridge
		} {
			g.AddEdge(e[0], e[1], 1.0)
		}
		return g
	}
	a := Run(build(), 1.0, 1234, 0)
	b := Run(build(), 1.0, 1234, 0)
	if !reflect.DeepEqual(a.Communities, b.Communities) {
		t.Errorf("non-deterministic: %v vs %v", a.Communities, b.Communities)
	}
}

// TestRun_PlantedPartition: 3 cliques of size 5 connected by single
// bridge edges between adjacent cliques. At γ=1 we expect to recover
// the 3 planted communities.
func TestRun_PlantedPartition(t *testing.T) {
	const k = 3   // number of communities
	const s = 5   // size of each
	const n = k * s
	g := NewGraph(n)
	for c := 0; c < k; c++ {
		base := c * s
		for i := 0; i < s; i++ {
			for j := i + 1; j < s; j++ {
				g.AddEdge(base+i, base+j, 1.0)
			}
		}
	}
	// Bridges between consecutive cliques.
	for c := 0; c < k-1; c++ {
		g.AddEdge(c*s+s-1, (c+1)*s, 1.0)
	}

	res := Run(g, 1.0, 7, 0)
	if res.NumComms != k {
		t.Errorf("planted-partition: expected %d communities, got %d (%v)", k, res.NumComms, res.Communities)
	}
	// Each planted clique must be intact.
	for c := 0; c < k; c++ {
		base := c * s
		want := res.Communities[base]
		for i := 1; i < s; i++ {
			if res.Communities[base+i] != want {
				t.Errorf("clique %d split at node %d: %v", c, base+i, res.Communities)
				break
			}
		}
	}
}

// TestModularity_SingleClique: a 4-clique as one community at γ=1.
// Σ_in = 12 (6 edges × 2), Σ_tot = 12 (each node degree 3, 4 nodes × 3),
// 2m = 12. Q = 12/12 - (12/12)^2 = 0.
func TestModularity_SingleClique(t *testing.T) {
	g := NewGraph(4)
	for i := 0; i < 4; i++ {
		for j := i + 1; j < 4; j++ {
			g.AddEdge(i, j, 1.0)
		}
	}
	q := modularity(g, []int{0, 0, 0, 0}, 1.0)
	if math.Abs(q-0.0) > 1e-9 {
		t.Errorf("expected Q=0 for one-community 4-clique, got %v", q)
	}
}

// TestModularity_TwoDisjointTriangles: each triangle is one community.
// Per triangle: Σ_in = 6 (3 edges × 2), Σ_tot = 6, 2m_local = 6.
// Total 2m = 12. Σ_in/2m for each = 6/12 = 0.5. (Σ_tot/2m)^2 = (6/12)^2 = 0.25.
// Q = 2 × (0.5 - 0.25) = 0.5.
func TestModularity_TwoDisjointTriangles(t *testing.T) {
	g := NewGraph(6)
	addUndirectedEdges(g, [][2]int{{0, 1}, {1, 2}, {2, 0}, {3, 4}, {4, 5}, {5, 3}})
	q := modularity(g, []int{0, 0, 0, 1, 1, 1}, 1.0)
	if math.Abs(q-0.5) > 1e-9 {
		t.Errorf("expected Q=0.5, got %v", q)
	}
}
