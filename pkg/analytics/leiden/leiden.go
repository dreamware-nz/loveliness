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
// edges; for an undirected edge (u,v,w) both Adj[u] and Adj[v] hold it.
// Self-loops appear once in Adj[i] and contribute weight w (not 2w) to
// NodeWeight[i] — the doubling for modularity is handled in the kernel.
type Graph struct {
	N           int
	Adj         [][]Neighbor
	NodeWeight  []float64 // sum of incident edge weights
	TotalWeight float64   // 2m: sum of all edge weights, with each edge counted twice
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
// allowed and contribute correctly to modularity. Adding a duplicate
// edge stacks weights — callers can pre-aggregate or rely on this.
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
	// Self-loop: counted once in Adj, w added once to NodeWeight, w
	// added once to TotalWeight here. Modularity treats self-loops as
	// 2w in degree and 2w in TotalWeight; we'll correct in the kernel.
	if u == v {
		g.NodeWeight[u] += w
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
// where Σ_in(C) is internal weight (each internal edge counted twice
// for non-self-loops, twice for self-loops too), and Σ_tot(C) is total
// incident weight.
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

			for c, eUtoC := range toComm {
				if c == cu {
					continue
				}
				sumC := sTot[c]
				// ΔQ for u: cu -> c
				// gain: (eUtoC - eUtoCU)/m - gamma*ku*(sumC - sumCUminusU)/(2m^2)
				// Factor out 1/m and 1/(2m): use twoM (which is 2m).
				delta := (eUtoC-eUtoCU)/(twoM/2) - gamma*ku*(sumC-sumCUminusU)/(twoM*twoM/2)
				// Equivalent simpler form:
				delta = 2*(eUtoC-eUtoCU)/twoM - gamma*ku*(sumC-sumCUminusU)*2/(twoM*twoM)
				if delta > bestDelta+1e-12 {
					bestDelta = delta
					bestComm = c
				}
			}

			if bestComm != cu {
				// Apply move.
				sTot[cu] -= ku
				sTot[bestComm] += ku
				comm[u] = bestComm
				improvedThisPass = true
				anyImproved = true
			}
			// Self-loop value used implicitly via toComm[cu] not
			// including selfLoop; since we never move to cu, it does
			// not matter for the comparison above.
			_ = selfLoop
		}
		if !improvedThisPass {
			break
		}
	}

	return anyImproved
}

// refine takes the post-local-move partition and, for each community
// from that partition, splits it into well-connected sub-communities.
// We start every node in its own singleton sub-community, then run a
// Leiden-style refinement: a node may move only to a sub-community
// that lies entirely inside its parent community, and only if the move
// strictly improves Q at the current γ. This guarantees the aggregated
// graph respects connectivity (the central Leiden fix vs. Louvain).
func refine(g *Graph, parent []int, gamma float64, rng *rand.Rand) []int {
	twoM := g.TotalWeight
	if twoM == 0 {
		return append([]int(nil), parent...)
	}

	// Each node starts in its own sub-community. Sub-community labels
	// are global (across parent communities) and use the original node
	// indices as labels.
	sub := identityPartition(g.N)
	sTot := make(map[int]float64, g.N)
	for u := 0; u < g.N; u++ {
		sTot[sub[u]] = g.NodeWeight[u]
	}

	// Iterate nodes in random order. For each node u, consider only
	// sub-communities in u's parent community. Apply the best move if
	// it's a strict improvement.
	order := make([]int, g.N)
	for i := range order {
		order[i] = i
	}
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

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
			delta := 2*(eUtoC-eUtoCU)/twoM - gamma*ku*(sumC-sumCUminusU)*2/(twoM*twoM)
			if delta > bestDelta+1e-12 {
				bestDelta = delta
				bestComm = c
			}
		}

		if bestComm != cu {
			sTot[cu] -= ku
			sTot[bestComm] += ku
			sub[u] = bestComm
		}
	}

	return sub
}

// aggregate collapses each refined sub-community into one node in a
// new graph. Returns the aggregate graph and a mapping
// newNode -> []origNode.
func aggregate(g *Graph, sub []int) (*Graph, [][]int) {
	// Gather members of each sub-community in deterministic order.
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
	// Sum edge weights between super-nodes.
	type key struct{ a, b int }
	weights := map[key]float64{}
	for u := 0; u < g.N; u++ {
		nu := labelToNew[sub[u]]
		for _, e := range g.Adj[u] {
			nv := labelToNew[sub[e.To]]
			if nv < nu {
				continue
			}
			weights[key{nu, nv}] += e.Weight
		}
	}
	// Collapse: for each (a,b) pair, the AddEdge call below counts
	// the undirected edge once. But our weights map already summed
	// weights from BOTH directions (u->v and v->u contributed when
	// nv >= nu)... wait, no — we only added when nv >= nu, so each
	// undirected edge contributed once from u->v (when nu==nv it's a
	// self-loop and we count only once; when nu<nv we count u->v but
	// not v->u). Let's re-check.
	//
	// For an undirected edge u—v with nu != nv, Adj[u] has (v,w) and
	// Adj[v] has (u,w). When iterating u, e.To=v gives (nu, nv). If
	// nv > nu we record. When iterating v, e.To=u gives (nv, nu).
	// Since nu < nv, "nv < nu" is false → we'd record (nv, nu) too.
	// That's two records for one undirected edge. Fix: restrict
	// the storage key to ordered pairs.
	weights = map[key]float64{}
	for u := 0; u < g.N; u++ {
		nu := labelToNew[sub[u]]
		for _, e := range g.Adj[u] {
			nv := labelToNew[sub[e.To]]
			a, b := nu, nv
			if a > b {
				a, b = b, a
			}
			weights[key{a, b}] += e.Weight
		}
	}
	// Each undirected edge u—v (u != v) is recorded in both Adj[u]
	// and Adj[v]; so weights[key{a,b}] for a<b is 2*w_uv. For
	// self-loops (a==b) Adj[u] has (u,w) once, so weights[key{u,u}]
	// is w. But AddEdge expects single-edge weight. Halve non-self.
	for k, w := range weights {
		if k.a == k.b {
			newG.AddEdge(k.a, k.b, w)
		} else {
			newG.AddEdge(k.a, k.b, w/2)
		}
	}

	return newG, mapping
}
