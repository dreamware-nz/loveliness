# Analytics plugin spike — findings

## Status: ✅ proven

The plugin contract works. End-to-end test suite is green; plugin overhead
is comfortably under budget. Cypher CALL syntax is out — HTTP envelope
is the right path.

## What got built

```
spike/analytics-plugin-poc/
├── RESEARCH.md                       # context, options, decisions
├── FINDINGS.md                       # this file
├── analytics/
│   └── analytics.go                  # Plugin interface + Registry
├── plugins/
│   ├── count_by_label.go             # trivial plugin
│   └── connected_components.go       # graph-shape plugin (Leiden stand-in)
├── server/
│   ├── server.go                     # POST /db/{name}/query
│   └── server_test.go                # 5 tests + 1 benchmark
└── cmd/demo/
    └── main.go                       # `go run`-able live HTTP demo
```

## Wire format

```
POST /db/{name}/query
Content-Type: application/json

{
  "cypher": "MATCH (s)-[:KNOWS]->(d) RETURN s.id AS src, d.id AS dst",
  "analytics": [
    {"name": "connected_components", "params": {"src": "src", "dst": "dst"}},
    {"name": "count_by_label",       "params": {"column": "src"}}
  ]
}
```

Response:

```json
{
  "columns": ["src", "dst"],
  "rows": [...],
  "analytics": {
    "connected_components": {
      "num_components": 2,
      "num_nodes": 6,
      "largest": 3,
      "size_histogram": [3, 3]
    },
    "count_by_label": {"counts": {"a": 1, "b": 1, ...}, "total": 6}
  },
  "analytics_errors": {}
}
```

Plugin directory:

```
GET /analytics → {"plugins": ["count_by_label", "connected_components"]}
```

## Plugin contract (Go)

```go
type Plugin interface {
    Name() string
    Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error)
}
```

That's it. Everything else (registry, error isolation, request shape) is
infrastructure.

## Test results

```
ok  github.com/johnjansen/loveliness/spike/analytics-plugin-poc/server  0.539s
```

| Test | What it proves |
|---|---|
| `TestQueryWithoutAnalytics` | new endpoint is a strict superset of `/cypher` — analytics are opt-in |
| `TestCountByLabelPlugin` | trivial plugin sees rows, returns aggregates correctly |
| `TestConnectedComponentsPlugin` | graph-shape plugin: 2 disjoint triangles → 2 components, 6 nodes, largest=3 |
| `TestUnknownPlugin` | unknown plugin name → `analytics_errors["x"] = "unknown plugin"`, request still succeeds |
| `TestPluginErrorIsolation` | bad params → that plugin errors in `analytics_errors`, others still run |

## Bench

```
BenchmarkPluginOverhead10K-8   3517   653158 ns/op   34069 B/op   10015 allocs/op
```

653μs per plugin pass at 10K rows. The acceptance bar was <50ms — we're
~75× under.

The 10015 allocs are mostly per-row map ops (one per row, plus a few
constants). At 1M rows this would be ~70ms — still fine. If we ever care,
swap `map[string]any` → typed columnar access or pool.

## Decisions made

1. **HTTP-only.** Cypher CALL would need a real parser (the existing one
   is prefix-classification). Punted to a future feature, not a spike.
2. **Separate endpoint** (`/db/{name}/query`), not `/cypher` overload.
   Backward compat, clearer contract.
3. **Post-execution hook**, not streaming. Plugins see the full result.
   Streaming variant is a Phase 2 concern; the interface can grow.
4. **Per-plugin error isolation.** A misbehaving plugin produces an entry
   in `analytics_errors`, doesn't kill the response.
5. **No auth changes.** Plugins inherit the request's auth scope — they
   only see what the underlying cypher saw.
6. **`analytics` not `facets` on the wire.** Facet implies categorical
   aggregation; our first real plugin (Leiden) is a graph algorithm.
   "Analytics" is the right umbrella; the path is `/analytics`, the
   request key is `analytics[]`, the response key is `analytics`.

## How this lands in the production code

`pkg/api/api.go` already has the right shape. The merge:

1. Add `pkg/analytics/` (move from spike) — contract + registry, no
   server dependency.
2. Move plugins to their own package(s); Leiden becomes one of them
   (likely calling out to leiden-rs via cgo or running its native Go
   port).
3. In `pkg/api/api.go`:
   - Register `POST /db/{name}/query` alongside the existing cypher routes.
   - The handler reuses `s.router.Execute` — same hook, JSON envelope on top.
4. The existing `POST /cypher` and `POST /db/{name}/cypher` stay
   untouched. Clients opt in by hitting the new endpoint.

Risk: low. New endpoint, no changes to existing behaviour, plugins are
opt-in per request.

## What this means for the system plan

The `loveliness-viz` binary now has a clean dependency:

```
SPA  ─── POST /db/main/query (cypher + leiden plugin) ───►  loveliness-viz
                                                             │
                                                             ▼
                                                        loveliness daemon
                                                          (cypher + leiden plugin)
```

That's the same shape we already drew, just with the plugin layer
identified. The pre-baked snapshot (Arrow IPC) becomes a special case:
it's what you get when a query + plugins is materialised into a
re-fetchable artefact — i.e. a **view** (next spike).

## Views — followup, not v1

A view is just `{cypher, analytics, refresh_policy}` named and persisted.
Materialised views become re-fetchable Arrow snapshots; virtual views
are query-time substitution. Both depend on this plugin contract.

What views need that this spike didn't deliver:

- DDL: `CREATE VIEW`, `DROP VIEW`, `REFRESH VIEW` — needs parser work.
- Storage: persisted view definitions (could live in the schema registry).
- Refresh triggers: on-demand, scheduled, or on-write-to-table.
- Snapshot serialisation: probably the same Arrow path the system plan
  needs anyway.

So views are a **superset** of this spike. Build the plugin layer first,
add views on top once the plugin shape is settled.

## Acceptance criteria (from RESEARCH.md)

- [x] Plugin registry with two registered plugins (one trivial, one
      graph-shape)
- [x] `POST /db/{name}/query` accepting `{cypher, analytics[]}` JSON
- [x] Trivial plugin: `count_by_label`
- [x] Graph plugin: `connected_components` (Leiden stand-in)
- [x] Bench: plugin overhead <50ms at 10K rows (got 653μs — 75× under)
- [x] No regression on existing `/cypher` endpoint (untouched)

## Non-goals reaffirmed

- Cypher CALL syntax — separate parser work
- View DDL — separate spike (see followup section)
- Plugin sandboxing — same trust boundary as the cypher engine
- Streaming/incremental plugins — Phase 2 if needed
- Cross-shard merging — single-node spike

## Next moves (when promoting to production)

1. Move `analytics/` and `plugins/` out of `spike/` into `pkg/`.
2. Wire Leiden as a real plugin — Go port or leiden-rs FFI.
3. Add the `/db/{name}/query` route to `pkg/api/api.go`.
4. Decide on plugin lifecycle: register at boot vs. dynamic load.
5. Add metrics: per-plugin timer + per-plugin error counter (mirrors the
   existing `queryHistogram` pattern in `api.go`).

## Followup spike (separate)

**Views.** See the views spike issue (gh #63). Prereq: this plugin spike
merged.
