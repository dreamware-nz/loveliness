// Package leiden is a pure-Go implementation of the Leiden community
// detection algorithm (Traag, Waltman, van Eck 2019), an improvement on
// Louvain that guarantees connected communities and avoids badly-
// connected splits.
//
// Quality function is resolution-parameterised modularity:
//
//	Q(γ) = 1/(2m) Σ_C [Σ_in(C) - γ * (Σ_tot(C))^2 / (2m)]
//
// At γ=1 this is standard Newman-Girvan modularity. Higher γ pushes
// toward smaller communities, lower γ toward fewer larger ones.
//
// The implementation runs three phases per outer iteration:
//   - local move: greedy ΔQ-improving moves until no node moves
//   - refinement: split each community into well-connected
//     sub-communities so the aggregate graph respects connectivity
//   - aggregate: collapse refined communities into super-nodes; recurse
//
// Determinism: with a fixed seed, the same graph + γ produce the same
// partition on the same Go version. Internal node iteration order is
// shuffled each pass via the seeded RNG.
package leiden

import (
	"math/rand"
	"sort"
)

// Neighbor is one entry in an adjacency list: a destination node and
// the weight of the edge to it.
type Neighbor struct {
	To     int
	Weight float64
}

// Graph is an undirected weighted graph. Adj[i] holds i's outgoing
// edges; for an undirected non-self edge (u,v,w) both Adj[u] and
// Adj[v] hold an entry of weight w. A self-loop (u,u,w) appears once
// in Adj[u].
//
// Convention: under modularity, a self-loop of weight w contributes w
// (not 2w) to NodeWeight[u] and w to TotalWeight. The kernel's ΔQ
// formula is symmetric under this choice — the self-loop terms cancel
// when a node moves communities — so localMove handles self-loops
// correctly without an explicit branch.
type Graph struct {
	N           int
	Adj         [][]Neighbor
	NodeWeight  []float64 // sum of incident edge weights (self-loop counted once)
	TotalWeight float64   // sum over nodes of NodeWeight[i]
}

// Coarsen collapses each community in `comm` into a single super-node,
// returning a new graph where nodes are communities and edges are the
// sum of cross-community edges from the original graph.
//
// Self-loops (intra-community edges) are aggregated into the super-node's
// self-loop weight. Cross-community edges become edges between the
// corresponding super-nodes.
//
// The returned mapping maps each super-node index to the list of original
// node indices it represents.
func Coarsen(g *Graph, comm []int) (*Graph, [][]int) {
	// Gather members of each community in deterministic order.
	type pair struct{ label, node int }
	pairs := make([]pair, 0, g.N)
	for u, c := range comm {
		pairs = append(pairs, pair{c, u})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].label != pairs[j].label {
			return pairs[i].label < pairs[j].label
		}
		return pairs[i].node < pairs[j].node
	})

	labelToNew := map[int]int{}
	mapping := [][]int{}
	for _, p := range pairs {
		idx, ok := labelToNew[p.label]
		if !ok {
			idx = len(mapping)
			labelToNew[p.label] = idx
			mapping = append(mapping, nil)
		}
		mapping[idx] = append(mapping[idx], p.node)
	}

	newG := NewGraph(len(mapping))

	type key struct{ a, b int }
	cross := map[key]float64{} // canonical a<=b: sum of original cross edge weights, each counted ONCE
	self := map[int]float64{}  // super-node a: aggregated self-loop weight

	for u := 0; u < g.N; u++ {
		nu := labelToNew[comm[u]]
		for _, e := range g.Adj[u] {
			nv := labelToNew[comm[e.To]]
			if e.To == u {
				// Original self-loop: appears once in Adj[u].
				self[nu] += e.Weight
				continue
			}
			if nu == nv {
				// Within-community non-self edge u-v (u≠v). Both
				// directions contribute; sum = 2w, which is the
				// self-loop weight needed to preserve 2m.
				self[nu] += e.Weight
				continue
			}
			// Cross-community non-self edge. Both directions; halve
			// at the end for canonical single-counting.
			a, b := nu, nv
			if a > b {
				a, b = b, a
			}
			cross[key{a, b}] += e.Weight
		}
	}

	for k, w := range cross {
		newG.AddEdge(k.a, k.b, w/2)
	}
	for a, w := range self {
		if w > 0 {
			newG.AddEdge(a, a, w)
		}
	}

	return newG, mapping
}

// NewGraph allocates an empty graph with n nodes.
func NewGraph(n int) *Graph {
	return &Graph{
		N:          n,
		Adj:        make([][]Neighbor, n),
		NodeWeight: make([]float64, n),
	}
}

// AddEdge adds an undirected edge u—v with weight w. Self-loops are
// allowed. Adding a duplicate edge stacks weights — callers can
// pre-aggregate or rely on this.
//
// Self-loop convention: weight w contributes w (not 2w) to
// NodeWeight[u] and w to TotalWeight. See Graph for why this choice
// is internally consistent with the kernel formulas.
func (g *Graph) AddEdge(u, v int, w float64) {
	if w == 0 {
		return
	}
	g.Adj[u] = append(g.Adj[u], Neighbor{To: v, Weight: w})
	g.NodeWeight[u] += w
	g.TotalWeight += w
	if u != v {
		g.Adj[v] = append(g.Adj[v], Neighbor{To: u, Weight: w})
		g.NodeWeight[v] += w
		g.TotalWeight += w
	}
}

// Result is the output of Run.
type Result struct {
	// Communities[i] is the community label for node i, relabelled so
	// labels are dense in [0, NumComms).
	Communities []int
	NumComms    int
	// Modularity is Q(γ) of the returned partition.
	Modularity float64
	// Iterations counts completed outer iterations (local-move +
	// refine + aggregate cycles).
	Iterations int
}

// Run executes Leiden with resolution γ. Seed controls RNG ordering;
// pass 0 for a fixed default. maxIter caps the outer loop; ≤0 falls
// back to a sensible default. Returns a partition over g's nodes.
func Run(g *Graph, gamma float64, seed int64, maxIter int) *Result {
	if maxIter <= 0 {
		maxIter = 32
	}
	rng := rand.New(rand.NewSource(seed))

	// Phase-0 partition: every node its own community.
	current := identityPartition(g.N)

	working := g
	// trace[level][i] = community of original node i in `current` at
	// level. We reconstruct the original-node partition from the
	// aggregated communities by following the chain.
	type level struct {
		// nodeOf[k] = list of working-graph indices that node k aggregates.
		// At level 0 this is identity ([k] -> [k]).
		nodeToOriginals [][]int
		// commAfterMove[k] = community of working-graph node k after
		// local-moving but BEFORE the refinement split (used to seed
		// the next level's partition; see Traag et al. §3.3).
		commAfterMove []int
	}

	levels := []level{{nodeToOriginals: identityNodeMap(g.N)}}
	iter := 0
	for ; iter < maxIter; iter++ {
		// Local move on the working graph, starting from `current`.
		localMoveImproved := localMove(working, current, gamma, rng)

		// Snapshot post-local-move partition (used to seed next level).
		postLocal := append([]int(nil), current...)

		// Refine inside each community, then aggregate.
		refined := refine(working, current, gamma, rng)
		aggregated, mapping := aggregate(working, refined)

		// If aggregation didn't change anything (every refined comm has
		// one node — i.e., the partition is fully refined-stable) and
		// local move didn't improve, we're done.
		if !localMoveImproved && len(aggregated.Adj) == working.N {
			levels[len(levels)-1].commAfterMove = postLocal
			iter++
			break
		}

		// Seed next level's partition from `postLocal` projected onto
		// the aggregated graph: each new node's community is the
		// post-local-move community of any of its constituents (they
		// all share one because refinement only splits within them).
		nextPartition := make([]int, len(aggregated.Adj))
		for newIdx, originals := range mapping {
			// Pick the community label from the first original.
			nextPartition[newIdx] = postLocal[originals[0]]
		}
		// Densify labels.
		nextPartition = densify(nextPartition)

		levels[len(levels)-1].commAfterMove = postLocal
		levels = append(levels, level{nodeToOriginals: expandMapping(mapping, levels[len(levels)-1].nodeToOriginals)})

		working = aggregated
		current = nextPartition
	}

	// Project the final partition (`current` over the deepest aggregate)
	// back onto the original nodes.
	finalPerOrig := make([]int, g.N)
	deepest := levels[len(levels)-1].nodeToOriginals
	for newIdx, comm := range current {
		for _, orig := range deepest[newIdx] {
			finalPerOrig[orig] = comm
		}
	}
	finalPerOrig = densify(finalPerOrig)
	q := modularity(g, finalPerOrig, gamma)

	return &Result{
		Communities: finalPerOrig,
		NumComms:    countDistinct(finalPerOrig),
		Modularity:  q,
		Iterations:  iter,
	}
}

// identityPartition returns [0,1,2,...,n-1].
func identityPartition(n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return p
}

// identityNodeMap returns [[0],[1],...,[n-1]] for level-0 mapping.
func identityNodeMap(n int) [][]int {
	m := make([][]int, n)
	for i := range m {
		m[i] = []int{i}
	}
	return m
}

// expandMapping composes a level-N mapping (newNode -> []prevNode)
// with a level-(N-1) mapping (prevNode -> []origNode) to produce
// level-N -> []origNode.
func expandMapping(newToPrev [][]int, prevToOrig [][]int) [][]int {
	out := make([][]int, len(newToPrev))
	for i, prevs := range newToPrev {
		var origs []int
		for _, p := range prevs {
			origs = append(origs, prevToOrig[p]...)
		}
		out[i] = origs
	}
	return out
}

// densify rewrites a partition so labels are 0..k-1 in order of first
// appearance. Stable for deterministic output.
func densify(p []int) []int {
	remap := map[int]int{}
	out := make([]int, len(p))
	next := 0
	for i, c := range p {
		nc, ok := remap[c]
		if !ok {
			nc = next
			next++
			remap[c] = nc
		}
		out[i] = nc
	}
	return out
}

func countDistinct(p []int) int {
	seen := map[int]struct{}{}
	for _, c := range p {
		seen[c] = struct{}{}
	}
	return len(seen)
}

// modularity computes Q(γ) for the given partition.
//
//	Q = 1/(2m) * Σ_C [Σ_in(C) - γ * (Σ_tot(C))^2 / (2m)]
//
// Convention: A_uv = w throughout (including the diagonal — a self-loop
// contributes w to A_uu, not 2w). Under this convention, summing the
// adjacency-list directly yields each undirected non-self edge counted
// twice (once from each endpoint) and each self-loop counted once. This
// matches the bookkeeping in NodeWeight and TotalWeight: 2m =
// Σ_u NodeWeight[u] = Σ_uv A_uv. Σ_in(C) is the sum of A_uv over u,v∈C
// (so each internal non-self edge contributes 2w, each internal self-loop
// contributes w). Σ_tot(C) is the sum of node weights in C.
//
// aggregate preserves all three quantities exactly: 2m, Σ_in(C), and
// Σ_tot(C) under collapse, so Q is invariant across aggregation levels.
func modularity(g *Graph, comm []int, gamma float64) float64 {
	if g.TotalWeight == 0 {
		return 0
	}
	twoM := g.TotalWeight
	// Σ_in(C) and Σ_tot(C).
	sIn := map[int]float64{}
	sTot := map[int]float64{}
	for u := 0; u < g.N; u++ {
		cu := comm[u]
		sTot[cu] += g.NodeWeight[u]
		for _, e := range g.Adj[u] {
			if comm[e.To] == cu {
				sIn[cu] += e.Weight
			}
		}
	}
	q := 0.0
	for c, in := range sIn {
		tot := sTot[c]
		q += in/twoM - gamma*(tot/twoM)*(tot/twoM)
	}
	return q
}

// localMove runs the greedy local-moving phase. Each pass shuffles
// node order, considers the best-ΔQ move for each node, and applies
// it. Loops until a full pass produces no movement. Returns whether
// any move happened across all passes.
func localMove(g *Graph, comm []int, gamma float64, rng *rand.Rand) bool {
	twoM := g.TotalWeight
	if twoM == 0 {
		return false
	}

	// Σ_tot(C): degree-sum per community.
	sTot := make(map[int]float64)
	for u := 0; u < g.N; u++ {
		sTot[comm[u]] += g.NodeWeight[u]
	}

	order := make([]int, g.N)
	for i := range order {
		order[i] = i
	}

	anyImproved := false
	for {
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		improvedThisPass := false
		for _, u := range order {
			cu := comm[u]
			ku := g.NodeWeight[u]

			// Sum of edge weights from u to each candidate community.
			toComm := map[int]float64{}
			selfLoop := 0.0
			for _, e := range g.Adj[u] {
				if e.To == u {
					selfLoop += e.Weight
					continue
				}
				toComm[comm[e.To]] += e.Weight
			}

			// Removing u from cu drops Σ_tot(cu) by ku.
			// "Adjusted" removal gain (cost of being where we are):
			//   removing u recovers gamma*ku*(Σ_tot(cu)-ku)/m  − 2*e_u_cu/2m
			// We compute ΔQ for moving to each candidate (including
			// staying: stay yields 0 by construction).
			bestDelta := 0.0
			bestComm := cu
			eUtoCU := toComm[cu] // weight from u to cu \ {u} (self-loop excluded above)
			sumCUminusU := sTot[cu] - ku

			// Self-loops are excluded from toComm above. They cancel
			// out of ΔQ symmetrically (a self-loop on u moves with u,
			// so its Σ_in contribution leaves cu and enters c by the
			// same amount), so ignoring them here is correct.
			_ = selfLoop

			for c, eUtoC := range toComm {
				if c == cu {
					continue
				}
				sumC := sTot[c]
				// ΔQ for u: cu -> c, derived from
				//   ΔQ = 2(eUtoC - eUtoCU)/2m - γ·ku·(sumC - (sumCu-ku))/(2m)²
				delta := 2*(eUtoC-eUtoCU)/twoM - 2*gamma*ku*(sumC-sumCUminusU)/(twoM*twoM)
				if delta > bestDelta+1e-12 {
					bestDelta = delta
					bestComm = c
				}
			}

			if bestComm != cu {
				sTot[cu] -= ku
				sTot[bestComm] += ku
				comm[u] = bestComm
				improvedThisPass = true
				anyImproved = true
			}
		}
		if !improvedThisPass {
			break
		}
	}

	return anyImproved
}

// refine takes the post-local-move partition and, for each community
// from that partition, splits it into well-connected sub-communities.
// Each node starts in its own singleton sub-community; we then run a
// Leiden-style refinement to fixpoint: a node may move only to a
// sub-community that lies entirely inside its parent community, and
// only if the move strictly improves Q at the current γ. Iterating to
// fixpoint matches the paper's specification — a single pass would let
// early-shuffled nodes block better moves for later nodes.
//
// This restriction is what makes the aggregated graph respect
// connectivity (the central Leiden fix vs. Louvain).
func refine(g *Graph, parent []int, gamma float64, rng *rand.Rand) []int {
	twoM := g.TotalWeight
	if twoM == 0 {
		return append([]int(nil), parent...)
	}

	// Each node starts in its own sub-community. Sub-community labels
	// are global (across parent communities); we seed them with the
	// original node indices.
	sub := identityPartition(g.N)
	sTot := make(map[int]float64, g.N)
	for u := 0; u < g.N; u++ {
		sTot[sub[u]] = g.NodeWeight[u]
	}

	order := make([]int, g.N)
	for i := range order {
		order[i] = i
	}

	for {
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		improved := false
		for _, u := range order {
			pu := parent[u]
			ku := g.NodeWeight[u]
			cu := sub[u]

			toSub := map[int]float64{}
			for _, e := range g.Adj[u] {
				if e.To == u {
					continue
				}
				if parent[e.To] != pu {
					continue
				}
				toSub[sub[e.To]] += e.Weight
			}

			bestDelta := 0.0
			bestComm := cu
			eUtoCU := toSub[cu]
			sumCUminusU := sTot[cu] - ku

			for c, eUtoC := range toSub {
				if c == cu {
					continue
				}
				sumC := sTot[c]
				delta := 2*(eUtoC-eUtoCU)/twoM - 2*gamma*ku*(sumC-sumCUminusU)/(twoM*twoM)
				if delta > bestDelta+1e-12 {
					bestDelta = delta
					bestComm = c
				}
			}

			if bestComm != cu {
				sTot[cu] -= ku
				sTot[bestComm] += ku
				sub[u] = bestComm
				improved = true
			}
		}
		if !improved {
			break
		}
	}

	return sub
}

// aggregate collapses each refined sub-community into one node in a
// new graph. Returns the aggregate graph and a mapping
// newNode -> []origNode.
//
// 2m invariance: original 2m must equal aggregated 2m. With our
// self-loop-weight-w-contributes-w-to-2m convention this means:
//
//   - cross-community non-self edges (u,v,w) → super-edge (a,b,w)
//   - within-community non-self edge (u,v,w) → contributes 2w to the
//     self-loop on its super-node (each end of the original edge added
//     w to original 2m; the resulting self-loop must add 2w to keep 2m
//     invariant under our convention)
//   - within-community self-loop (u,u,w) → contributes w to the self-
//     loop on its super-node
func aggregate(g *Graph, sub []int) (*Graph, [][]int) {
	// Gather members of each sub-community in deterministic order so
	// the new node indexing is stable across runs with the same RNG.
	type pair struct{ label, node int }
	pairs := make([]pair, 0, g.N)
	for u, c := range sub {
		pairs = append(pairs, pair{c, u})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].label != pairs[j].label {
			return pairs[i].label < pairs[j].label
		}
		return pairs[i].node < pairs[j].node
	})

	labelToNew := map[int]int{}
	mapping := [][]int{}
	for _, p := range pairs {
		idx, ok := labelToNew[p.label]
		if !ok {
			idx = len(mapping)
			labelToNew[p.label] = idx
			mapping = append(mapping, nil)
		}
		mapping[idx] = append(mapping[idx], p.node)
	}

	newG := NewGraph(len(mapping))

	type key struct{ a, b int }
	cross := map[key]float64{} // canonical a<=b: sum of original cross edge weights, each counted ONCE
	self := map[int]float64{}  // super-node a: aggregated self-loop weight (already adjusted)

	for u := 0; u < g.N; u++ {
		nu := labelToNew[sub[u]]
		for _, e := range g.Adj[u] {
			nv := labelToNew[sub[e.To]]
			if e.To == u {
				// Original self-loop: appears once in Adj[u].
				self[nu] += e.Weight
				continue
			}
			if nu == nv {
				// Within-community non-self edge u-v (u≠v). Adj[u] has
				// (v,w) and Adj[v] has (u,w); both iterations contribute
				// e.Weight here. Sum of both = 2w, which is exactly the
				// self-loop weight needed to preserve 2m.
				self[nu] += e.Weight
				continue
			}
			// Cross-community non-self edge. Both directions contribute;
			// halve at the end for canonical single-counting.
			a, b := nu, nv
			if a > b {
				a, b = b, a
			}
			cross[key{a, b}] += e.Weight
		}
	}

	for k, w := range cross {
		newG.AddEdge(k.a, k.b, w/2)
	}
	for a, w := range self {
		if w > 0 {
			newG.AddEdge(a, a, w)
		}
	}

	return newG, mapping
}
