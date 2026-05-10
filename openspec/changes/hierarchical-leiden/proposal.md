## Why

The current `leiden` plugin returns only a flat partition — one community assignment per node at a single γ (or independent partitions across a γ sweep). Real community structure is inherently multi-scale: nodes belong to local clusters within larger super-clusters. Without hierarchical output, users must manually chain queries at different γ values and reconcile results, which is error-prone and inefficient.

## What Changes

- Add a `hierarchical` mode to the `leiden` plugin that produces nested community structure
- The plugin runs Leiden iteratively: after each run, aggregates nodes in the same community into super-nodes, and re-runs at a finer γ on the aggregated graph
- Returns a tree of communities where each level has its own modularity, size histogram, and assignments
- Exposes a `depth` param (default 3) to control how many levels to recurse
- Each level's assignments map node IDs to community indices at that level
- Top-level partition uses the current single-gamma or sweep path (unchanged)

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
