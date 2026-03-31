package cluster

// PlacementAdapter provides shard→node lookups from the live Raft FSM state.
// It satisfies router.PlacementResolver.
type PlacementAdapter struct {
	cluster *Cluster
}

// NewPlacementAdapter creates a PlacementAdapter for the given cluster.
func NewPlacementAdapter(c *Cluster) *PlacementAdapter {
	return &PlacementAdapter{cluster: c}
}

// PrimaryForShard returns the primary node ID for a shard.
func (p *PlacementAdapter) PrimaryForShard(shardID int) string {
	return p.cluster.GetShardMap().PrimaryForShard(shardID)
}
