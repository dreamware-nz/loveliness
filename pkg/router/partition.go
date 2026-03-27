package router

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// PartitionStrategy defines how nodes are assigned to shards.
type PartitionStrategy int

const (
	// PartitionHash uses FNV-32a hash of the shard key (default).
	PartitionHash PartitionStrategy = iota
	// PartitionLabel uses label propagation community detection to co-locate
	// densely connected subgraphs on the same shard.
	PartitionLabel
)

// PartitionAnalyzer analyzes the graph topology to find communities
// and suggest shard reassignments that minimize cross-shard edges.
//
// Uses a simplified label propagation algorithm:
//  1. Each node starts with its own label (= current shard ID)
//  2. Each node adopts the most frequent label among its neighbors
//  3. Repeat until convergence or max iterations
//  4. Nodes with the same label form a community → same shard
type PartitionAnalyzer struct {
	shards     []*shard.Shard
	shardCount int
	router     *Router
}

// NewPartitionAnalyzer creates a partition analyzer.
func NewPartitionAnalyzer(shards []*shard.Shard, r *Router) *PartitionAnalyzer {
	return &PartitionAnalyzer{
		shards:     shards,
		shardCount: len(shards),
		router:     r,
	}
}

// MigrationPlan describes which nodes should move between shards
// to improve locality.
type MigrationPlan struct {
	Moves        []NodeMove // individual node migrations
	CurrentCuts  int        // cross-shard edges before migration
	ProjectedCuts int       // estimated cross-shard edges after migration
	Communities  int        // number of detected communities
}

// NodeMove describes a single node migration.
type NodeMove struct {
	Table      string
	Key        string
	FromShard  int
	ToShard    int
	EdgesCut   int // number of cross-shard edges this move eliminates
}

// AnalyzeCommunities runs label propagation on a sampled graph to detect
// communities and compute a migration plan.
//
// Parameters:
//   - table: node table to analyze
//   - shardKeyProp: the shard key property name
//   - sampleSize: max nodes to sample per shard (0 = all)
//   - maxIter: max label propagation iterations
func (pa *PartitionAnalyzer) AnalyzeCommunities(
	ctx context.Context,
	table, shardKeyProp string,
	sampleSize, maxIter int,
) (*MigrationPlan, error) {

	if maxIter <= 0 {
		maxIter = 10
	}

	// Collect graph topology from all shards.
	nodes, edges, err := pa.collectTopology(ctx, table, shardKeyProp, sampleSize)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		return &MigrationPlan{}, nil
	}

	// Run label propagation.
	labels := pa.labelPropagation(nodes, edges, maxIter)

	// Compute the migration plan.
	plan := pa.computeMigrationPlan(nodes, edges, labels, table)
	return plan, nil
}

// nodeInfo holds a node's key and current shard assignment.
type nodeInfo struct {
	key     string
	shardID int
}

// edgeInfo holds an edge between two node keys.
type edgeInfo struct {
	from string
	to   string
}

// collectTopology gathers nodes and edges from all shards.
func (pa *PartitionAnalyzer) collectTopology(
	ctx context.Context,
	table, shardKeyProp string,
	sampleSize int,
) ([]nodeInfo, []edgeInfo, error) {

	type shardData struct {
		nodes []nodeInfo
		edges []edgeInfo
		err   error
	}

	var wg sync.WaitGroup
	results := make(chan shardData, pa.shardCount)

	for i, s := range pa.shards {
		if !s.IsHealthy() {
			continue
		}
		wg.Add(1)
		go func(sid int, sh *shard.Shard) {
			defer wg.Done()

			// Fetch nodes.
			limit := ""
			if sampleSize > 0 {
				limit = fmt.Sprintf(" LIMIT %d", sampleSize)
			}
			nodeCypher := fmt.Sprintf("MATCH (n:%s) RETURN n.%s%s", table, shardKeyProp, limit)
			nodeResp, err := sh.Query(nodeCypher)
			if err != nil {
				results <- shardData{err: fmt.Errorf("shard %d nodes: %w", sid, err)}
				return
			}

			var nodes []nodeInfo
			col := fmt.Sprintf("n.%s", shardKeyProp)
			for _, row := range nodeResp.Rows {
				if v, ok := row[col]; ok && v != nil {
					nodes = append(nodes, nodeInfo{
						key:     fmt.Sprintf("%v", v),
						shardID: sid,
					})
				}
			}

			// Fetch edges (connections between nodes).
			edgeCypher := fmt.Sprintf("MATCH (a:%s)-[]->(b:%s) RETURN a.%s, b.%s%s",
				table, table, shardKeyProp, shardKeyProp, limit)
			edgeResp, err := sh.Query(edgeCypher)
			if err != nil {
				// Non-fatal: we might not have edges.
				results <- shardData{nodes: nodes}
				return
			}

			var edges []edgeInfo
			fromCol := fmt.Sprintf("a.%s", shardKeyProp)
			toCol := fmt.Sprintf("b.%s", shardKeyProp)
			for _, row := range edgeResp.Rows {
				fromVal := row[fromCol]
				toVal := row[toCol]
				if fromVal != nil && toVal != nil {
					edges = append(edges, edgeInfo{
						from: fmt.Sprintf("%v", fromVal),
						to:   fmt.Sprintf("%v", toVal),
					})
				}
			}

			results <- shardData{nodes: nodes, edges: edges}
		}(i, s)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allNodes []nodeInfo
	var allEdges []edgeInfo

	for res := range results {
		if res.err != nil {
			slog.Warn("partition analysis: shard error", "err", res.err)
			continue
		}
		allNodes = append(allNodes, res.nodes...)
		allEdges = append(allEdges, res.edges...)
	}

	return allNodes, allEdges, nil
}

// labelPropagation runs the label propagation community detection algorithm.
// Returns a map of nodeKey → communityLabel (shard assignment).
func (pa *PartitionAnalyzer) labelPropagation(
	nodes []nodeInfo,
	edges []edgeInfo,
	maxIter int,
) map[string]int {

	// Initialize: each node's label = its current shard.
	labels := make(map[string]int, len(nodes))
	for _, n := range nodes {
		labels[n.key] = n.shardID
	}

	// Build adjacency list.
	adj := make(map[string][]string)
	for _, e := range edges {
		adj[e.from] = append(adj[e.from], e.to)
		adj[e.to] = append(adj[e.to], e.from)
	}

	// Iterate: each node adopts the most frequent label among neighbors.
	for iter := 0; iter < maxIter; iter++ {
		changed := 0
		for _, n := range nodes {
			neighbors := adj[n.key]
			if len(neighbors) == 0 {
				continue
			}

			// Count neighbor labels.
			freq := make(map[int]int)
			for _, neighbor := range neighbors {
				if lbl, ok := labels[neighbor]; ok {
					freq[lbl]++
				}
			}

			// Find the most frequent label.
			bestLabel := labels[n.key]
			bestCount := 0
			for lbl, cnt := range freq {
				if cnt > bestCount || (cnt == bestCount && lbl < bestLabel) {
					bestLabel = lbl
					bestCount = cnt
				}
			}

			if labels[n.key] != bestLabel {
				labels[n.key] = bestLabel
				changed++
			}
		}

		slog.Debug("label propagation iteration", "iter", iter, "changed", changed)
		if changed == 0 {
			break // converged
		}
	}

	return labels
}

// computeMigrationPlan generates moves from community labels.
func (pa *PartitionAnalyzer) computeMigrationPlan(
	nodes []nodeInfo,
	edges []edgeInfo,
	labels map[string]int,
	table string,
) *MigrationPlan {

	// Count current cross-shard edges.
	currentCuts := 0
	nodeShards := make(map[string]int)
	for _, n := range nodes {
		nodeShards[n.key] = n.shardID
	}
	for _, e := range edges {
		if nodeShards[e.from] != nodeShards[e.to] {
			currentCuts++
		}
	}

	// Map community labels to target shards.
	// Communities are assigned to shards by frequency: biggest community → shard 0, etc.
	communitySize := make(map[int]int)
	for _, lbl := range labels {
		communitySize[lbl]++
	}

	// Sort communities by size (descending).
	type commInfo struct {
		label int
		size  int
	}
	var comms []commInfo
	for lbl, sz := range communitySize {
		comms = append(comms, commInfo{lbl, sz})
	}
	sort.Slice(comms, func(i, j int) bool {
		return comms[i].size > comms[j].size
	})

	// Assign communities to shards round-robin by size.
	communityToShard := make(map[int]int)
	for i, c := range comms {
		communityToShard[c.label] = i % pa.shardCount
	}

	// Compute moves.
	var moves []NodeMove
	projectedShards := make(map[string]int)
	for _, n := range nodes {
		targetShard := communityToShard[labels[n.key]]
		projectedShards[n.key] = targetShard
		if targetShard != n.shardID {
			moves = append(moves, NodeMove{
				Table:     table,
				Key:       n.key,
				FromShard: n.shardID,
				ToShard:   targetShard,
			})
		}
	}

	// Count projected cross-shard edges.
	projectedCuts := 0
	for _, e := range edges {
		if projectedShards[e.from] != projectedShards[e.to] {
			projectedCuts++
		}
	}

	return &MigrationPlan{
		Moves:         moves,
		CurrentCuts:   currentCuts,
		ProjectedCuts: projectedCuts,
		Communities:   len(comms),
	}
}

// LocalityScore returns the fraction of edges that are shard-local (0.0 to 1.0).
// Higher is better — 1.0 means all edges are within the same shard.
func (pa *PartitionAnalyzer) LocalityScore(ctx context.Context, table, shardKeyProp string) (float64, error) {
	_, edges, err := pa.collectTopology(ctx, table, shardKeyProp, 0)
	if err != nil {
		return 0, err
	}
	if len(edges) == 0 {
		return 1.0, nil
	}

	localEdges := 0
	for _, e := range edges {
		fromShard := pa.router.resolveShard(e.from)
		toShard := pa.router.resolveShard(e.to)
		if fromShard == toShard {
			localEdges++
		}
	}

	return float64(localEdges) / float64(len(edges)), nil
}

// ShardBalance returns a measure of how evenly nodes are distributed.
// Returns a map of shardID → node count and the balance ratio (min/max).
func (pa *PartitionAnalyzer) ShardBalance(ctx context.Context, table, shardKeyProp string) (map[int]int, float64, error) {
	nodes, _, err := pa.collectTopology(ctx, table, shardKeyProp, 0)
	if err != nil {
		return nil, 0, err
	}

	counts := make(map[int]int)
	for _, n := range nodes {
		counts[n.shardID]++
	}

	if len(counts) == 0 {
		return counts, 1.0, nil
	}

	minCount := int(^uint(0) >> 1) // max int
	maxCount := 0
	for _, c := range counts {
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}

	var balance float64
	if maxCount > 0 {
		balance = float64(minCount) / float64(maxCount)
	}

	return counts, balance, nil
}

