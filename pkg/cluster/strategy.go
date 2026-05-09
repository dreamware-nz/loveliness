package cluster

// PlacementStrategy decides where each shard's primary and replicas
// live. Implementations are pure functions of (shardID, nodes, rf,
// current) — they do not touch Raft, do not run side effects, and
// must produce stable output for stable input so re-runs of bootstrap
// are deterministic.
//
// A nil `current` means "no existing placement" — bootstrap path.
// A non-nil `current` lets the strategy preserve placements when
// possible (e.g. a node-leave reassignment shouldn't move every shard
// just because the node count changed).
//
// Strategies should clamp rf to len(nodes) internally; callers
// should not have to special-case under-replicated bootstrap.
type PlacementStrategy interface {
	// Place returns the desired placement for shardID given the
	// available nodes, requested replication factor, and any existing
	// placement (nil if none).
	Place(shardID int, nodes []string, rf int, current *ShardAssignment) ShardAssignment
}

// RoundRobinStrategy is the default placement strategy. It assigns
// the primary of shard i to nodes[i % len(nodes)] and walks forward
// through the node list to fill replicas. Replicas are never placed
// on the same node as the primary, and rf is clamped to len(nodes).
//
// This is the simplest strategy that satisfies the issue #5 spec.
// Rack/AZ-aware variants implement the same interface.
type RoundRobinStrategy struct{}

// Place implements PlacementStrategy.
func (RoundRobinStrategy) Place(shardID int, nodes []string, rf int, current *ShardAssignment) ShardAssignment {
	n := len(nodes)
	if n == 0 {
		return ShardAssignment{}
	}
	if rf < 1 {
		rf = 1
	}
	if rf > n {
		rf = n
	}
	primaryIdx := ((shardID % n) + n) % n
	primary := nodes[primaryIdx]
	replicas := make([]string, 0, rf-1)
	for i := 1; i < rf; i++ {
		replicas = append(replicas, nodes[(primaryIdx+i)%n])
	}
	out := ShardAssignment{
		Primary:  primary,
		Replicas: replicas,
	}
	if current != nil {
		out.Epoch = current.Epoch + 1
	}
	return out
}
