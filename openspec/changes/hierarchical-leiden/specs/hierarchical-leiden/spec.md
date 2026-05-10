## ADDED Requirements

### Requirement: Plugin supports hierarchical mode
The `leiden` plugin SHALL produce a hierarchical partition when the `hierarchical` param is set to `true`.

#### Scenario: Hierarchical mode enabled
- **WHEN** the `leiden` plugin is called with `hierarchical: true`
- **THEN** the response includes a `levels` array with one entry per hierarchy depth
- **AND** each level contains `gamma`, `num_communities`, `modularity`, and `size_histogram`

#### Scenario: Hierarchical mode disabled (default)
- **WHEN** the `leiden` plugin is called without `hierarchical` or with `hierarchical: false`
- **THEN** the response shape is unchanged (flat single partition or sweep)

### Requirement: Hierarchical depth is configurable
The plugin SHALL accept a `depth` param (default 3) that controls how many levels to recurse.

#### Scenario: Default depth
- **WHEN** `hierarchical: true` is set without `depth`
- **THEN** the plugin produces exactly 3 levels

#### Scenario: Custom depth
- **WHEN** `hierarchical: true` and `depth: 5` are set
- **THEN** the plugin produces exactly 5 levels

### Requirement: Coarsening aggregates communities into super-nodes
Each level is computed by aggregating the previous level's communities into super-nodes and running Leiden on the coarser graph.

#### Scenario: Aggregation produces valid coarser graph
- **WHEN** a level has N communities
- **THEN** the next level's graph has N super-nodes with edges weighted by the sum of inter-community edges from the previous level
- **AND** self-loops (intra-community edges) are excluded

### Requirement: Each level includes per-node assignments when requested
If `include_assignments` is `true`, every level SHALL include an `assignments` map.

#### Scenario: Assignments included per level
- **WHEN** `hierarchical: true` and `include_assignments: true`
- **THEN** each level in `levels` includes an `assignments` map from node ID to community index at that level

#### Scenario: Assignments omitted by default
- **WHEN** `hierarchical: true` and `include_assignments` is not set
- **THEN** no `assignments` field appears in any level

### Requirement: Seed is shared across levels
The plugin SHALL use the same RNG seed for all levels to ensure deterministic output.

#### Scenario: Deterministic output
- **WHEN** the plugin is called twice with the same inputs and seed
- **THEN** the output is identical both times

### Requirement: Hierarchical mode respects context cancellation
The plugin SHALL check `ctx` between levels and abort if cancelled.

#### Scenario: Context cancelled between levels
- **WHEN** `ctx` is cancelled after level 1 completes
- **THEN** the plugin returns an error and only includes completed levels
