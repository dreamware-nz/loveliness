package cluster

import (
	"sort"
	"sync"
)

// CrossShardStats tracks cross-shard edge traversal counts.
// Used to identify hot cross-shard paths for locality optimization.
type CrossShardStats struct {
	mu    sync.Mutex
	edges map[[2]int]int64 // [sourceShard, targetShard] → traversal count
}

// NewCrossShardStats creates a new stats tracker.
func NewCrossShardStats() *CrossShardStats {
	return &CrossShardStats{
		edges: make(map[[2]int]int64),
	}
}

// RecordTraversal increments the cross-shard traversal counter.
func (s *CrossShardStats) RecordTraversal(fromShard, toShard int) {
	if fromShard == toShard {
		return // not cross-shard
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.edges[[2]int{fromShard, toShard}]++
}

// TopCrossings returns the top N cross-shard paths by traversal count.
func (s *CrossShardStats) TopCrossings(n int) []ShardCrossing {
	s.mu.Lock()
	defer s.mu.Unlock()

	crossings := make([]ShardCrossing, 0, len(s.edges))
	for pair, count := range s.edges {
		crossings = append(crossings, ShardCrossing{
			FromShard: pair[0],
			ToShard:   pair[1],
			Count:     count,
		})
	}

	sort.Slice(crossings, func(i, j int) bool {
		return crossings[i].Count > crossings[j].Count
	})

	if n > len(crossings) {
		n = len(crossings)
	}
	return crossings[:n]
}

// TotalCrossings returns the total number of cross-shard traversals.
func (s *CrossShardStats) TotalCrossings() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, count := range s.edges {
		total += count
	}
	return total
}

// Reset clears all stats.
func (s *CrossShardStats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.edges = make(map[[2]int]int64)
}

// ShardCrossing represents a cross-shard traversal path and its frequency.
type ShardCrossing struct {
	FromShard int   `json:"from_shard"`
	ToShard   int   `json:"to_shard"`
	Count     int64 `json:"count"`
}

// Partitioner uses cross-shard statistics to suggest node migrations
// that would reduce cross-shard traversals.
//
// Algorithm: simplified label propagation.
// 1. For each pair of shards with high cross-traffic, suggest colocating
//    them on the same node (if they're currently on different nodes).
// 2. Only suggest moves that reduce total cross-shard traversals below
//    the threshold.
type Partitioner struct {
	stats     *CrossShardStats
	threshold int64 // minimum traversal count to consider a crossing "hot"
}

// NewPartitioner creates a partitioner with the given stats and threshold.
func NewPartitioner(stats *CrossShardStats, threshold int64) *Partitioner {
	return &Partitioner{
		stats:     stats,
		threshold: threshold,
	}
}

// ColocateSuggestion recommends placing two shards on the same node.
type ColocateSuggestion struct {
	ShardA    int
	ShardB    int
	Crossings int64
}

// Suggest returns shard colocation suggestions to reduce cross-shard traffic.
func (p *Partitioner) Suggest(assignments map[int]ShardAssignment) []ColocateSuggestion {
	crossings := p.stats.TopCrossings(100)

	var suggestions []ColocateSuggestion
	for _, c := range crossings {
		if c.Count < p.threshold {
			break // sorted by count, so everything after is below threshold
		}

		aOwner := assignments[c.FromShard].Primary
		bOwner := assignments[c.ToShard].Primary

		if aOwner == bOwner {
			continue // already colocated
		}

		suggestions = append(suggestions, ColocateSuggestion{
			ShardA:    c.FromShard,
			ShardB:    c.ToShard,
			Crossings: c.Count,
		})
	}

	return suggestions
}
