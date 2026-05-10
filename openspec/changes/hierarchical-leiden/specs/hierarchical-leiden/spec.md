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
- **THEN** the plugin produces up to 3 levels (stops early if a level has one community)

#### Scenario: Custom depth
- **WHEN** `hierarchical: true` and `depth: 5` are set
- **THEN** the plugin produces up to 5 levels (stops early if a level has one community)

### Requirement: γ schedule — explicit gamma_schedule param
The plugin SHALL accept a `gamma_schedule` param: a list of γ values, one per level, in ascending resolution order (coarse → fine).

#### Scenario: Explicit gamma_schedule
- **WHEN** `hierarchical: true` and `gamma_schedule: [0.5, 1.0, 2.0]` are set
- **THEN** level 1 runs at γ=0.5, level 2 at γ=1.0, level 3 at γ=2.0

#### Scenario: gamma_schedule length exceeds depth
- **WHEN** `gamma_schedule: [0.5, 1.0, 2.0, 4.0]` is set with `depth: 3`
- **THEN** only the first 3 γ values are used (depth caps the schedule)

#### Scenario: gamma_schedule length is shorter than depth
- **WHEN** `gamma_schedule: [0.5, 1.0]` is set with `depth: 3`
- **THEN** remaining levels use γ=1.0 (the last value in the schedule) as a fallback

### Requirement: γ schedule — single gamma (backward-compatible)
When `gamma` is provided alone (no `gamma_schedule`), the same γ applies to all levels.

#### Scenario: Single gamma applies to all levels
- **WHEN** `hierarchical: true` and `gamma: 1.0` are set (no `gamma_schedule`)
- **THEN** every level runs at γ=1.0

### Requirement: γ schedule — auto-discover via plateau
When neither `gamma` nor `gamma_schedule` is provided, the plugin SHALL auto-discover γ values by running `resolution_plateau` and picking one representative γ per plateau as a level.

#### Scenario: Auto-discover produces levels from plateaus
- **WHEN** `hierarchical: true` is set without `gamma` or `gamma_schedule`
- **THEN** the plugin runs internal plateau detection and assigns one level per discovered plateau (capped by `depth`)

#### Scenario: Auto-discover with depth cap
- **WHEN** `hierarchical: true`, `depth: 3`, and no γ params are set
- **THEN** the plugin produces at most 3 levels even if more plateaus are found

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
