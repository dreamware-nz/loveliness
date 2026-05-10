# Analytics Plugins

Loveliness can run opt-in, post-execution analytics on a Cypher result.
You issue one HTTP request that runs Cypher first and then feeds the
columns/rows into one or more registered plugins. Plugin output comes
back alongside the normal query result.

This is one round-trip, one connection, one query. The plugin sees the
exact same materialised result your client would have received from
`/db/{name}/cypher`.

## Endpoint

```
POST /db/{name}/query
Content-Type: application/json
```

**Request envelope:**

```json
{
  "cypher": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst",
  "analytics": [
    { "name": "leiden", "params": { "gamma": 1.0 } }
  ]
}
```

- `cypher` (required) — the query whose result feeds the plugin(s).
- `analytics` (optional) — array of `{name, params}` entries. Order is
  preserved but plugins do not see each other's output; they all run
  against the same Cypher result.

**Response envelope:**

```json
{
  "columns": ["src", "dst"],
  "rows":    [{"src": "Alice", "dst": "Bob"}, ...],
  "stats":   {"compile_time_ms": 0.12, "exec_time_ms": 0.45},
  "analytics": {
    "leiden": { ... plugin-specific payload ... }
  },
  "analytics_errors": {
    "some_other_plugin": "leiden: src column \"src\" not in result"
  }
}
```

- The base shape (`columns`, `rows`, `stats`, `partial`, `errors`) is the
  same as `/db/{name}/cypher` — `/query` is a strict superset.
- `analytics` is keyed by plugin name. Missing if no plugins ran.
- `analytics_errors` is per-plugin. One bad plugin does not poison the
  rest of the response — always check both maps.

**Discovery:**

```
GET /analytics
→ { "plugins": ["connected_components", "count_by_label", "leiden", "resolution_plateau"] }
```

Plugin set is frozen at boot. Names returned here are the names you pass
in `analytics[].name`.

---

## `leiden` — community detection

Pure-Go implementation of the Leiden algorithm (Traag, Waltman, van Eck
2019). Interprets the Cypher result as an edge list and returns
community structure. Two modes: single γ (one partition) or γ sweep
(N partitions in parallel).

### Single-γ mode

**Request:**

```json
{
  "cypher": "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS src, b.name AS dst",
  "analytics": [
    {
      "name": "leiden",
      "params": {
        "gamma": 1.0,
        "seed":  7,
        "include_assignments": true
      }
    }
  ]
}
```

**Params:**

| Name                  | Type     | Default | Notes                                                               |
|-----------------------|----------|---------|---------------------------------------------------------------------|
| `src`                 | string   | `"src"` | Source-id column name                                               |
| `dst`                 | string   | `"dst"` | Destination-id column name                                          |
| `weight`              | string   | —       | Edge-weight column. Absent → every edge weighs 1.0                  |
| `gamma`               | float    | `1.0`   | Resolution. >1 → finer (more, smaller communities); <1 → coarser   |
| `seed`                | int64    | `0`     | RNG seed. Same seed + same graph + same γ → same partition         |
| `max_iter`            | int      | `32`    | Outer-iteration cap. `0` (or omitted) means "use the default of 32" |
| `include_assignments` | bool     | `false` | If true, response carries per-node `id → community` map             |

NaN, ±Inf, and negative γ are rejected with HTTP 200 + an entry in
`analytics_errors`. Missing `src`/`dst` columns are rejected the same way.

**Response (`analytics.leiden`):**

```json
{
  "gamma": 1.0,
  "num_nodes": 8,
  "num_communities": 2,
  "modularity": 0.41,
  "iterations": 3,
  "size_histogram": [4, 4],
  "assignments": {
    "Alice":   0,
    "Bob":     0,
    "Carol":   0,
    "Dave":    0,
    "Erin":    1,
    "Frank":   1,
    "Greta":   1,
    "Heidi":   1
  }
}
```

| Field             | Meaning                                                                |
|-------------------|------------------------------------------------------------------------|
| `gamma`           | Echo of the resolution used                                            |
| `num_nodes`       | Distinct nodes seen across `src` ∪ `dst`                               |
| `num_communities` | How many communities the partition has                                 |
| `modularity`      | `Q(γ) = 1/(2m) Σ_C [Σ_in(C) − γ·(Σ_tot(C))²/(2m)]`                    |
| `iterations`      | Outer iterations until convergence (≤ `max_iter`)                     |
| `size_histogram`  | Community sizes, sorted descending                                    |
| `assignments`     | Only present with `include_assignments: true`                          |

`assignments` can dominate response size on large graphs — keep it off
unless you actually need to *use* the partition.

### γ-sweep mode

Pass `gammas` instead of `gamma` to run Leiden once per resolution in
parallel against the same graph:

```json
{
  "cypher": "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS src, b.name AS dst",
  "analytics": [
    { "name": "leiden",
      "params": { "gammas": [0.5, 1.0, 2.0, 4.0], "seed": 7 } }
  ]
}
```

`gamma` and `gammas` are mutually exclusive. The sweep is bounded to
`runtime.NumCPU()` workers, so a 1000-entry `gammas` list will not
saturate the host. ctx cancellation is honored between γs; an in-flight
γ runs to completion.

**Response shape switches to `partitions: [...]`:**

```json
{
  "num_nodes": 8,
  "partitions": [
    { "gamma": 0.5, "num_communities": 1, "modularity": 0.0,  "iterations": 2, "size_histogram": [8] },
    { "gamma": 1.0, "num_communities": 2, "modularity": 0.41, "iterations": 3, "size_histogram": [4, 4] },
    { "gamma": 2.0, "num_communities": 2, "modularity": 0.41, "iterations": 3, "size_histogram": [4, 4] },
    { "gamma": 4.0, "num_communities": 8, "modularity": 0.0,  "iterations": 1, "size_histogram": [1, 1, 1, 1, 1, 1, 1, 1] }
  ]
}
```

`partitions[i]` carries the same fields as the single-γ response, in the
order you supplied `gammas`. Add `include_assignments: true` to attach
the id→community map to every entry.

---

## `resolution_plateau` — auto-discover stable resolutions

Picking γ by hand is awkward. Plateaus are γ ranges where the partition
is stable — γ values inside one plateau give the same answer, so picking
any representative is safe. This plugin runs a γ sweep, computes
adjacent-pair NMI between partitions, groups consecutive γ entries with
NMI ≥ threshold into plateaus, and returns the plateau list.

**Request (with explicit γs):**

```json
{
  "cypher": "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS src, b.name AS dst",
  "analytics": [
    { "name": "resolution_plateau",
      "params": { "gammas": [0.5, 1.0, 2.0, 4.0], "nmi_threshold": 0.95 } }
  ]
}
```

**Request (with auto-range):**

```json
{
  "cypher": "MATCH (a)-[:KNOWS]->(b) RETURN a.name AS src, b.name AS dst",
  "analytics": [
    { "name": "resolution_plateau",
      "params": {
        "gamma_min":   0.1,
        "gamma_max":   5.0,
        "gamma_steps": 21,
        "include_assignments": true
      } }
  ]
}
```

**Params:**

| Name                  | Type     | Default | Notes                                                                  |
|-----------------------|----------|---------|------------------------------------------------------------------------|
| `src`                 | string   | `"src"` | Source-id column name                                                  |
| `dst`                 | string   | `"dst"` | Destination-id column name                                             |
| `weight`              | string   | —       | Edge-weight column. Absent → every edge weighs 1.0                     |
| `gammas`              | []float  | —       | Explicit γ list. Sorted internally. Wins over `gamma_*` if present     |
| `gamma_min`           | float    | `0.1`   | Range start (used when `gammas` absent)                                |
| `gamma_max`           | float    | `5.0`   | Range end. Must be > `gamma_min`                                       |
| `gamma_steps`         | int      | `21`    | Linear-spaced points across `[gamma_min, gamma_max]`. Must be ≥ 2     |
| `nmi_threshold`       | float    | `0.95`  | Adjacent partitions with NMI ≥ this are part of the same plateau      |
| `seed`                | int64    | `0`     | RNG seed (per γ)                                                      |
| `max_iter`            | int      | `32`    | Outer-iteration cap (per γ). `0` (or omitted) means "use the default of 32" |
| `include_partitions`  | bool     | `false` | Surface every per-γ partition summary (default hidden)                |
| `include_assignments` | bool     | `false` | Attach `representative_partition` (id→community) to every plateau     |

**Response (`analytics.resolution_plateau`):**

```json
{
  "num_nodes": 8,
  "nmi_threshold": 0.95,
  "plateaus": [
    {
      "gamma_min": 0.1,
      "gamma_max": 0.5,
      "num_communities": 1,
      "representative_gamma": 0.3
    },
    {
      "gamma_min": 0.7,
      "gamma_max": 2.5,
      "num_communities": 2,
      "representative_gamma": 1.5,
      "representative_partition": {
        "Alice": 0, "Bob": 0, "Carol": 0, "Dave": 0,
        "Erin": 1, "Frank": 1, "Greta": 1, "Heidi": 1
      }
    },
    {
      "gamma_min": 2.7,
      "gamma_max": 5.0,
      "num_communities": 8,
      "representative_gamma": 3.8
    }
  ]
}
```

| Field                        | Meaning                                                                          |
|------------------------------|----------------------------------------------------------------------------------|
| `num_nodes`                  | Distinct nodes seen                                                              |
| `nmi_threshold`              | Echo of the threshold used                                                       |
| `plateaus[].gamma_min/max`   | γ range covered by this plateau                                                  |
| `plateaus[].num_communities` | Community count of any partition inside the plateau (they're all NMI≥threshold) |
| `plateaus[].representative_gamma` | A γ in the middle of the plateau — use this when you need a single γ        |
| `plateaus[].representative_partition` | Only when `include_assignments: true`. id→community for the representative |

**With `include_partitions: true`:**

```json
{
  "num_nodes": 8,
  "nmi_threshold": 0.95,
  "plateaus": [ ... as above ... ],
  "partitions": [
    { "gamma": 0.1,  "num_communities": 1, "modularity": 0.0,  "iterations": 2, "size_histogram": [8] },
    { "gamma": 0.345, ... },
    ...
  ]
}
```

This duplicates information the plateau list already summarises, so
keep it off unless you want the raw sweep for plotting or audit.

---

## Other built-in plugins

These were the original two; they document the shape of a minimal plugin:

- **`count_by_label`** — counts rows grouped by a label column. Params:
  `column` (required).
- **`connected_components`** — undirected weakly-connected components
  via union-find. Params: `src`, `dst` (default `"src"`/`"dst"`).

Run `GET /analytics` for the live list.

---

## End-to-end example

```bash
# Two cliques bridged by a single edge.
curl -s localhost:8080/db/social/query \
  -H 'Content-Type: application/json' \
  -d '{
    "cypher": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst",
    "analytics": [
      { "name": "resolution_plateau",
        "params": { "gamma_min": 0.1, "gamma_max": 5.0, "gamma_steps": 21,
                    "nmi_threshold": 0.95, "include_assignments": true } }
    ]
  }'
```

Pick the middle plateau's `representative_gamma`, feed it back to the
`leiden` plugin in single-γ mode, and you have the partition you want
plus its modularity in one more round-trip.

---

## Errors

Errors from a plugin do not fail the request. The Cypher result still
comes back. Any plugin that errored shows up in `analytics_errors[name]`
with its message; the rest of `analytics` is unaffected.

A few common ones:

- `"leiden: src column \"src\" not in result"` — your Cypher RETURN
  didn't produce a column with that name. Add `AS src`.
- `"resolution_plateau: nmi_threshold must be in [0,1], got NaN"` —
  thresholds are validated for NaN explicitly (NaN passes naive bound
  checks).
- `"resolution_plateau: gamma_max (1) must be > gamma_min (1)"` — empty
  range; widen it or pass an explicit `gammas` list.

If the Cypher query itself fails, the response is the standard error
envelope and no plugins run.
