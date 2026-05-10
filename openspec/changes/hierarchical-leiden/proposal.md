## Why

The current `leiden` plugin returns only a flat partition — one community assignment per node at a single γ (or independent partitions across a γ sweep). Real community structure is inherently multi-scale: nodes belong to local clusters within larger super-clusters. Without hierarchical output, users must manually chain queries at different γ values and reconcile results, which is error-prone and inefficient.

## What Changes

- Add a `hierarchical` mode to the `leiden` plugin that produces nested community structure
- The plugin runs Leiden iteratively: after each run, aggregates nodes in the same community into super-nodes, and re-runs at the next γ on the aggregated graph
- Returns a flat `levels` array where each level has its own γ, modularity, size histogram, and (optionally) assignments
- γ schedule is controlled via three mutually exclusive options:
  - `gamma_schedule`: explicit list of γ values, one per level (coarse → fine)
  - `gamma`: single value applied to all levels (backward-compatible)
  - auto-discover: when neither is provided, run `resolution_plateau` to find stable γ ranges and pick one representative per level
- Exposes a `depth` param (default 3) to control max levels when using auto-discover or `gamma`
- Each level's assignments map node IDs to community indices at that level
- Early termination if a level collapses to one community

## Capabilities

### New Capabilities

- `hierarchical-leiden`: multi-level community detection with nested assignments and per-level modularity

### Modified Capabilities

- none

## Impact

- `pkg/analytics/plugins/leiden.go` — add hierarchical mode to `Compute`
- `pkg/analytics/leiden/leiden.go` — add `Aggregate` or `Coarsen` helper to build a coarser graph from a partition
- `pkg/analytics/plugins/plateau.go` — consider integrating hierarchical plateau detection in a future iteration
- Wire format: new response shape with nested `levels` array
