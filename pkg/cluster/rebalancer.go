package cluster

import (
	"log/slog"
	"sort"
)

// rebalancerCluster is the surface Rebalancer needs from a cluster.
// Pulled out as an interface so Execute can be unit-tested without a
// live Raft FSM.
type rebalancerCluster interface {
	GetShardMap() ShardMap
	AssignShard(shardID int, primary string, replicas []string) error
}

// Rebalancer computes shard migrations when nodes join or leave the cluster.
// Runs on the leader. Produces a list of moves to equalize shard distribution.
type Rebalancer struct {
	cluster rebalancerCluster
}

// NewRebalancer creates a rebalancer for the given cluster.
func NewRebalancer(c *Cluster) *Rebalancer {
	return &Rebalancer{cluster: c}
}

// Move describes a shard migration from one node to another.
// For Role == "replica", ReplicaIdx is the position in the
// Replicas slice that the new node should occupy. For Role ==
// "primary", ReplicaIdx is ignored.
type Move struct {
	ShardID    int
	Role       string // "primary" or "replica"
	FromNode   string
	ToNode     string
	ReplicaIdx int
}

// firstReplica returns the first non-empty replica or "".
// Convenience for rebalancer logic that historically managed only
// a single replica slot — kept until rebalance grows full multi-
// replica awareness (filed as follow-up to #5).
func firstReplica(a ShardAssignment) string {
	for _, r := range a.Replicas {
		if r != "" {
			return r
		}
	}
	return ""
}

type nodeLoad struct {
	id    string
	count int
	max   int
}

// Plan computes the migrations needed to balance shards across alive nodes.
// It doesn't execute them — call Execute for that.
func (r *Rebalancer) Plan() []Move {
	sm := r.cluster.GetShardMap()

	// Collect alive nodes.
	var alive []string
	for id, info := range sm.Nodes {
		if info.Alive {
			alive = append(alive, id)
		}
	}
	sort.Strings(alive)

	if len(alive) == 0 {
		return nil
	}

	return planMoves(sm.Assignments, alive)
}

// planMoves is the pure algorithm, separated for testing.
func planMoves(assignments map[int]ShardAssignment, alive []string) []Move {
	if len(alive) == 0 {
		return nil
	}

	aliveSet := make(map[string]bool, len(alive))
	for _, n := range alive {
		aliveSet[n] = true
	}

	// Count primaries per node.
	primaryCount := make(map[string]int)
	for _, n := range alive {
		primaryCount[n] = 0
	}
	for _, a := range assignments {
		if aliveSet[a.Primary] {
			primaryCount[a.Primary]++
		}
	}

	// Ideal distribution: total shards / nodes.
	totalShards := len(assignments)
	ideal := totalShards / len(alive)
	remainder := totalShards % len(alive)

	// Nodes that can accept more shards, sorted by current load (ascending).
	var loads []nodeLoad
	for i, n := range alive {
		maxForNode := ideal
		if i < remainder {
			maxForNode = ideal + 1
		}
		loads = append(loads, nodeLoad{id: n, count: primaryCount[n], max: maxForNode})
	}

	var moves []Move

	// Phase 1: Fix primaries on dead nodes.
	for shardID, a := range assignments {
		if a.Primary != "" && !aliveSet[a.Primary] {
			// Primary is on a dead node. Promote the first alive
			// replica if any; otherwise reassign to the least loaded
			// alive node.
			promoted := ""
			for _, r := range a.Replicas {
				if r != "" && aliveSet[r] {
					promoted = r
					break
				}
			}
			if promoted != "" {
				moves = append(moves, Move{
					ShardID:  shardID,
					Role:     "primary",
					FromNode: a.Primary,
					ToNode:   promoted,
				})
				primaryCount[promoted]++
			} else {
				target := leastLoaded(loads)
				if target != "" {
					moves = append(moves, Move{
						ShardID:  shardID,
						Role:     "primary",
						FromNode: a.Primary,
						ToNode:   target,
					})
					primaryCount[target]++
					for i := range loads {
						if loads[i].id == target {
							loads[i].count++
						}
					}
				}
			}
		}
	}

	// Phase 2: Rebalance overloaded nodes.
	// Sort shards by their current primary for determinism.
	type shardEntry struct {
		id int
		a  ShardAssignment
	}
	var sortedShards []shardEntry
	for id, a := range assignments {
		sortedShards = append(sortedShards, shardEntry{id, a})
	}
	sort.Slice(sortedShards, func(i, j int) bool {
		return sortedShards[i].id < sortedShards[j].id
	})

	// Refresh loads after phase 1.
	for i := range loads {
		loads[i].count = primaryCount[loads[i].id]
	}

	for _, se := range sortedShards {
		if !aliveSet[se.a.Primary] {
			continue // already handled in phase 1
		}
		ownerIdx := -1
		for i := range loads {
			if loads[i].id == se.a.Primary {
				ownerIdx = i
				break
			}
		}
		if ownerIdx == -1 || loads[ownerIdx].count <= loads[ownerIdx].max {
			continue
		}
		// This node is overloaded — find an underloaded target.
		target := ""
		for i := range loads {
			if loads[i].count < loads[i].max {
				target = loads[i].id
				break
			}
		}
		if target == "" {
			continue
		}
		moves = append(moves, Move{
			ShardID:  se.id,
			Role:     "primary",
			FromNode: se.a.Primary,
			ToNode:   target,
		})
		loads[ownerIdx].count--
		for i := range loads {
			if loads[i].id == target {
				loads[i].count++
			}
		}
	}

	// Phase 3: Fix the first replica slot if it's empty, dead, or
	// colocated with the primary. Multi-replica rebalance (filling
	// every slot up to RF) is tracked as a follow-up to issue #5;
	// this phase only ever touches Replicas[0].
	for shardID, a := range assignments {
		if len(alive) < 2 {
			break // can't have replicas with only 1 node
		}
		current := firstReplica(a)
		needsNewReplica := false
		if current == "" {
			needsNewReplica = true
		} else if !aliveSet[current] {
			needsNewReplica = true
		} else if current == a.Primary {
			needsNewReplica = true
		}

		if needsNewReplica {
			// Find a node that isn't the primary.
			currentPrimary := a.Primary
			// Check if we moved the primary in a previous phase.
			for _, m := range moves {
				if m.ShardID == shardID && m.Role == "primary" {
					currentPrimary = m.ToNode
				}
			}
			for _, n := range alive {
				if n != currentPrimary {
					moves = append(moves, Move{
						ShardID:    shardID,
						Role:       "replica",
						FromNode:   current,
						ToNode:     n,
						ReplicaIdx: 0,
					})
					break
				}
			}
		}
	}

	return moves
}

func leastLoaded(loads []nodeLoad) string {
	minCount := int(^uint(0) >> 1) // max int
	target := ""
	for _, l := range loads {
		if l.count < minCount {
			minCount = l.count
			target = l.id
		}
	}
	return target
}

// Execute applies the planned moves to the cluster via Raft.
func (r *Rebalancer) Execute(moves []Move) error {
	for _, m := range moves {
		sm := r.cluster.GetShardMap()
		a := sm.Assignments[m.ShardID]

		switch m.Role {
		case "primary":
			a.Primary = m.ToNode
			// Promotion: the replica we just promoted must not also
			// stay in the replicas list, and the dead old primary
			// (FromNode) shouldn't sneak in either. Drop both.
			a.Replicas = filterReplicas(a.Replicas, m.ToNode, m.FromNode)
		case "replica":
			idx := m.ReplicaIdx
			if idx < 0 {
				idx = 0
			}
			for len(a.Replicas) <= idx {
				a.Replicas = append(a.Replicas, "")
			}
			a.Replicas[idx] = m.ToNode
		}

		if err := r.cluster.AssignShard(m.ShardID, a.Primary, a.Replicas); err != nil {
			return err
		}
		slog.Info("rebalanced shard",
			"shard", m.ShardID,
			"role", m.Role,
			"from", m.FromNode,
			"to", m.ToNode,
		)
	}
	return nil
}

// filterReplicas returns replicas with any node in `drop` removed,
// preserving order. Empty slots are dropped too — they're never useful
// post-promotion and only confuse downstream consumers.
func filterReplicas(replicas []string, drop ...string) []string {
	if len(replicas) == 0 {
		return replicas
	}
	bad := make(map[string]struct{}, len(drop)+1)
	bad[""] = struct{}{}
	for _, d := range drop {
		bad[d] = struct{}{}
	}
	out := replicas[:0:0]
	for _, r := range replicas {
		if _, skip := bad[r]; skip {
			continue
		}
		out = append(out, r)
	}
	return out
}
