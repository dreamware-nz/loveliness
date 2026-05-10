## Context

The `leiden` plugin (`pkg/analytics/plugins/leiden.go`) runs the Leiden community-detection algorithm on a result-set interpreted as an edge list. It supports single-γ and multi-γ sweep modes, returning flat partitions. The underlying `leiden.Run` function iterates internally (local-moving → aggregation → repeat) but only exposes the final iteration's partition.

Users who want multi-scale community structure must manually run the plugin multiple times and reconcile results. This is cumbersome and loses the parent-child relationships between communities.

## Goals / Non-Goals

**Goals:**
- Add a `hierarchical` mode to the `leiden` plugin that produces nested community structure
- Each level of the hierarchy has its own modularity, size histogram, and per-node assignments
- Configurable depth (default 3 levels)
- Reuses the existing `leiden.Run` and `leiden.Graph` — no new algorithm

**Non-Goals:**
- Hierarchical plateau detection (which γ produces the most stable level at each depth)
- Visual output (treemap, dendrogram, etc.)
- Performance optimization of the aggregation step (v1 is correct-first)

## Decisions

### 1. Coarsening strategy: aggregate nodes into super-nodes
After each Leiden run, merge all nodes in the same community into a single super-node. Edges between super-nodes are the sum of edges between their members. Self-loops (within-community edges) are discarded. This is the standard Louvain/Leiden aggregation step.

**Rationale:** This is the canonical approach used by both Louvain and Leiden internally. It preserves edge weights and produces a valid coarser graph that can be fed into another Leiden run.

### 2. Depth param (default 3) vs. adaptive stopping
Use a configurable `depth` param rather than auto-stopping when a level has only one community. The user controls the trade-off between granularity and cost.

**Rationale:** Simple, predictable API. Adaptive stopping can be added later if needed.

### 3. γ schedule: explicit `gamma_schedule` + auto-discover fallback
The plugin accepts either:
- **`gamma_schedule`** (explicit): a list of γ values, one per level, in ascending resolution order (coarse → fine). E.g., `[0.5, 1.0, 2.0]` produces 3 levels at those γs.
- **Auto-discover (default)**: when neither `gamma` nor `gamma_schedule` is provided, the plugin runs `resolution_plateau` internally to find stable γ ranges and picks one representative γ per level from those plateaus.

If `gamma` is provided alone (single value), it applies to all levels (backward-compatible with the current behavior).

**Rationale:** Option C gives users full control (`gamma_schedule`) while keeping the simple case (`depth: 3` alone) working via auto-discover. The `gamma`-alone path preserves backward compatibility with the existing API.

### 4. Response shape: flat `levels` array with per-level assignments
Each level contains: the γ used, modularity, number of communities, size histogram, and (if `include_assignments` is true) a map of node ID → community index.

```json
{
  "num_nodes": 100,
  "depth": 3,
  "levels": [
    {
      "gamma": 1.0,
      "num_communities": 3,
      "modularity": 0.38,
      "size_histogram": [50, 30, 20],
      "assignments": { "node1": 0, "node2": 0, ... }
    },
    {
      "gamma": 1.0,
      "num_communities": 8,
      "modularity": 0.42,
      "size_histogram": [25, 15, 12, 10, 8, 7, 2, 1],
      "assignments": { "node1": 0, "node2": 3, ... }
    },
    {
      "gamma": 1.0,
      "num_communities": 15,
      "modularity": 0.44,
      "size_histogram": [12, 10, 9, 8, 7, 6, 5, 5, 4, 4, 3, 3, 2, 1, 1],
      "assignments": { "node1": 0, "node2": 7, ... }
    }
  ]
}
```

**Rationale:** Flat array is simpler than nested trees for JSON serialization and client-side rendering. The parent-child relationship is implicit: community 0 at level 2 contains a subset of the nodes in some community at level 1.

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Aggregation can lose node-level detail in intermediate levels | Each level includes full assignments; clients can reconstruct the hierarchy |
| O(n²) worst case if one community contains all nodes | Depth is bounded; users can set `depth: 1` as a fallback |
| Seed non-determinism across levels | The plugin uses the same seed for all levels; the aggregation step is deterministic given the same partition |
| Response size grows with depth × node count | `include_assignments` is opt-in; without it, the response is just the level summaries |

## Migration Plan

This is a new mode, not a breaking change. The existing single-γ and sweep modes remain unchanged. The `hierarchical` flag in the plugin params gates the new behavior.

## Open Questions

1. Should we expose a `gamma_schedule` param (one γ per level) in a future iteration?
2. Should the plugin auto-choose γ per level (e.g., γ × 1.1^(depth - 1 - i) for finer resolution at deeper levels)?
3. Should we add a `min_community_size` param to skip levels where all communities are trivially small?
