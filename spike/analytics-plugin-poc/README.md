# Analytics plugins

Opt-in, post-execution analytics on top of the Cypher resultset.
Inspired by Elasticsearch aggregations: declare what you want when you
make the query, get it back alongside the rows.

> **Status:** spike — `spike/analytics-plugin-poc/`. The promotion path
> into `pkg/` is documented at the bottom of this file. See `RESEARCH.md`
> for context, `FINDINGS.md` for the verdict + bench.

---

## Why this exists

A graph engine that only returns rows is missing half the value. Most
real questions ("what communities are in this neighbourhood?", "how is
degree distributed?", "which nodes are central?") are computed *over* a
result, not by it. Doing them client-side means shipping rows over the
wire just to throw most of them away. Doing them server-side without a
plugin layer means hard-coding every analytic into the engine.

This spike proves a third path: **a plugin registry the server consults
after each query**, executing only the analytics the client asked for.

---

## Calling the system

### Run a query with one or more analytics

```http
POST /db/{name}/query
Content-Type: application/json

{
  "cypher": "MATCH (s)-[:KNOWS]->(d) RETURN s.id AS src, d.id AS dst",
  "analytics": [
    {"name": "connected_components"},
    {"name": "count_by_label", "params": {"column": "src"}}
  ]
}
```

Response:

```json
{
  "columns": ["src", "dst"],
  "rows": [{"src": "a", "dst": "b"}, ...],
  "analytics": {
    "connected_components": {
      "num_components": 2,
      "num_nodes": 6,
      "largest": 3,
      "size_histogram": [3, 3]
    },
    "count_by_label": {
      "counts": {"a": 1, "b": 1, ...},
      "total": 6
    }
  }
}
```

### Run a query with no analytics

The `analytics` key is **opt-in**. Drop it and you get a plain cypher
result back — the path is a strict superset of `/cypher`:

```http
POST /db/{name}/query
{"cypher": "MATCH (n) RETURN n LIMIT 10"}
```

```json
{"columns": ["n"], "rows": [...]}
```

### List available plugins

```http
GET /analytics
```

```json
{"plugins": ["count_by_label", "connected_components"]}
```

### Error behaviour

Errors that come from the cypher engine itself (parse error, timeout,
shard unavailable) return a normal `4xx`/`5xx` with the existing
ladybug error envelope.

Errors that come from a **plugin** are isolated per-plugin: the request
still returns `200 OK` with the rows and any successful plugins, but the
failed plugin lands in a `analytics_errors` map:

```json
{
  "columns": [...],
  "rows": [...],
  "analytics": {"count_by_label": {...}},
  "analytics_errors": {
    "leiden": "leiden: gamma must be > 0",
    "nonexistent": "unknown plugin"
  }
}
```

This means a misbehaving plugin can never poison the rest of the
response. Clients should always check both `analytics` and
`analytics_errors`.

---

## The plugins shipped in the spike

### `count_by_label`

Groups rows by one column and counts each group. Useful for "how many
of each type came back?".

| Param | Type | Required | Default |
|---|---|---|---|
| `column` | string | yes | — |

Output:

```json
{"counts": {"User": 3, "Post": 2}, "total": 5}
```

### `connected_components`

Treats the result as an edge list and returns weakly-connected
components via union-find. This is the **Leiden stand-in for the spike**
— same plugin contract, simpler maths. The real Leiden plugin will
register the same way.

| Param | Type | Required | Default |
|---|---|---|---|
| `src` | string | no | `"src"` |
| `dst` | string | no | `"dst"` |

Output:

```json
{
  "num_components": 2,
  "num_nodes": 6,
  "largest": 3,
  "size_histogram": [3, 3]
}
```

---

## Writing your own plugin

A plugin is any Go type that satisfies this interface:

```go
package analytics

type Plugin interface {
    Name() string
    Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error)
}
```

Implementation rules:

1. **Read-only.** Don't mutate `result.Rows` or `result.Columns`. They're
   about to be serialised to the client; mutating them corrupts the
   response.
2. **Return any JSON-serialisable value.** A map, a struct with json tags,
   a slice — whatever shape makes sense for the analytic. The framework
   serialises it under `analytics.<name>`.
3. **Return errors plainly.** They land in `analytics_errors[name]` and
   don't affect the rest of the response.
4. **Validate params up front.** Cheap fail-fast beats partial
   computation.
5. **Respect ctx.** If the plugin is long-running, check `ctx.Done()` —
   the server cancels the context on request timeout.

Minimal example:

```go
package myplugin

import (
    "context"
    "fmt"

    "github.com/johnjansen/loveliness/pkg/router"
)

type RowCount struct{}

func (RowCount) Name() string { return "row_count" }

func (RowCount) Compute(_ context.Context, r *router.Result, _ map[string]any) (any, error) {
    return map[string]int{"rows": len(r.Rows)}, nil
}
```

Register it once at boot:

```go
reg := analytics.NewRegistry()
if err := reg.Register(myplugin.RowCount{}); err != nil {
    log.Fatalf("register row_count: %v", err)
}
server := server.New(routerExec, reg, 30*time.Second)
```

---

## Running the spike locally

```sh
# Tests
go test ./spike/analytics-plugin-poc/...

# Bench
go test -bench=. -run=^$ ./spike/analytics-plugin-poc/server

# Live HTTP demo (boots in-process httptest server, prints real responses)
go run ./spike/analytics-plugin-poc/cmd/demo
```

---

## Promotion to production

The spike code is structured so the move into `pkg/` is mostly file
relocation:

```
spike/analytics-plugin-poc/analytics/   →  pkg/analytics/
spike/analytics-plugin-poc/plugins/     →  pkg/analytics/plugins/
spike/analytics-plugin-poc/server/      →  hooked into pkg/api/api.go
```

Production wiring lives in `pkg/api/api.go` next to the existing cypher
routes:

```go
protected.HandleFunc("POST /db/{name}/query", s.handleQueryWithAnalytics)
protected.HandleFunc("GET /analytics",         s.handleAnalyticsList)
```

The handler reuses `s.router.Execute(ctx, req.Cypher)` — same engine,
JSON envelope on top.

The existing `POST /cypher` and `POST /db/{name}/cypher` are **left
untouched**. Clients opt in by hitting the new endpoint.

See [issue #62](https://github.com/dreamware-nz/loveliness/issues/62)
for the promotion checklist.

---

## Followup: views

A view is just `{cypher, analytics, refresh_policy}` named and persisted.
Materialised views become re-fetchable Arrow snapshots; they ride on the
same plugin contract this spike establishes. See
[issue #63](https://github.com/dreamware-nz/loveliness/issues/63).

---

## Naming note

We use `analytics` (not `facets`) on the wire and in code. "Facet"
implies categorical aggregation; our first real plugin (Leiden) is a
graph algorithm. "Analytics" is the right umbrella. The directory route
is `/analytics`, the request key is `analytics[]`, the response key is
`analytics`, the Go interface is `analytics.Plugin`. ES inspiration was
useful for the *shape* (opt-in at query time, computed server-side); the
term came along for the ride and we discarded it.
