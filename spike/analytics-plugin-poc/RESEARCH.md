# Analytics plugin spike — research

## What we want

A pluggable analytics layer on top of the cypher resultset. Inspired by
how Elasticsearch lets clients ask for facets/aggregations alongside hits:
opt-in at query time, declared by the client per request, computed
server-side, returned alongside the normal result.

We're calling them **analytics plugins**, not facets — the term "facet"
implies categorical aggregation, but our first real plugin (Leiden) is a
graph algorithm, not an aggregation. "Analytics" is the right umbrella.

Initial component target: **Leiden community detection**. The same plugin
mechanism should make it trivial to add degree distribution, label
histogram, PageRank, shortest-path stats, etc.

## What ladybug looks like today

### HTTP surface

`pkg/api/api.go:107-125` registers cypher endpoints:

```
POST /cypher                  // legacy default-shard
POST /db/{name}/cypher        // multi-database
POST /admin/cypher            // schema/admin
```

`handleCypher` (line 209) reads the body as **raw cypher text** (not JSON),
calls `s.router.Execute(ctx, cypher)`, gets back a `*router.Result`, and
hands it to `writeNegotiated` for content-negotiated serialisation
(JSON / Arrow stream / Arrow file).

### Result shape

`pkg/router/router.go:23`:

```go
type Result struct {
    Columns []string         `json:"columns"`
    Rows    []map[string]any `json:"rows"`
}
```

Tabular. One row per result tuple. Columns are just names.

### Cypher parser

`pkg/router/parser.go` is **prefix-based classification**, not an AST. It
looks at the first token to decide read/write/schema and does light
keyword scanning to find shard keys. There's no syntax tree we can hang
new clauses off.

`CALL` is mentioned only in a comment at line 15 ("MATCH, OPTIONAL MATCH,
CALL, UNWIND"). It's classified as a read but **not actually parsed or
dispatched as a procedure call**.

## What this rules out for the spike

- **Cypher CALL syntax** (`CALL plugin.leiden(γ=1.0) YIELD community`) —
  needs a real parser. Out of scope. Worth proposing as a follow-up if
  the plugin idea proves itself.
- **Cypher comment hints** (`/*+ ANALYTICS leiden */`) — clever but
  invisible to tooling and brittle. No.
- **Header-based plugin selection** (`X-Analytics: leiden`) — works but
  ugly, hard to express plugin params, defeats Accept-content-negotiation.

## What's left

Three candidates for the request shape:

### A. New JSON endpoint (recommended)

```
POST /db/{name}/query
Content-Type: application/json

{
  "cypher": "MATCH (u:User)-[:KNOWS]->(v:User) RETURN u, v",
  "analytics": [
    {"name": "leiden", "params": {"gamma": 1.0, "seed": 42}},
    {"name": "degree_distribution"}
  ]
}
```

Response:

```json
{
  "columns": ["u", "v"],
  "rows": [...],
  "analytics": {
    "leiden": {"communities": [...], "modularity": 0.61, "count": 142},
    "degree_distribution": {"buckets": [...]}
  }
}
```

Pros:
- backward compatible (existing `POST /cypher` endpoints untouched)
- explicit, self-describing, easy to version
- arbitrary plugin params expressible
- response shape stays clean — analytics live in their own object

Cons:
- new endpoint to maintain
- doubles the cypher path

### B. Reuse `/cypher` with optional JSON envelope

Sniff `Content-Type`: if JSON, treat body as `{cypher, analytics}`; else
raw cypher.

Pros: one endpoint
Cons: bodies that happen to be JSON-shaped break; cognitive overhead

### C. Query-string plugin selection

```
POST /cypher?analytics=leiden,degree_distribution
```

Body still raw cypher. Plugin params via headers or extra query keys.

Pros: minimal surface change
Cons: param expression is awful, query strings have length limits, can't
nest

**Picking A.** Cleanest for the spike and the production shape.

## Plugin contract

```go
package analytics

// Plugin is an opt-in computation that runs after a Cypher query produces
// a Result. It may augment the result with additional data, but must not
// mutate the existing rows.
type Plugin interface {
    Name() string
    // Compute receives the read-only result and decoded params.
    // It returns an opaque value that gets serialised back to the client
    // under analytics.<name>.
    Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error)
}

// Registry is the plugin registry — one per server.
type Registry struct {
    plugins map[string]Plugin
}

func (r *Registry) Register(p Plugin) error
func (r *Registry) Lookup(name string) (Plugin, bool)
```

Streaming concerns: for v1 we run all plugins after the full result is in
memory. Streaming plugins (e.g. degree counter that consumes rows
incrementally) is a Phase 2 concern. The interface above can grow a
streaming variant without breaking compat.

## Where views fit

A "view" in DB terms = saved query, optionally materialised.

- **Virtual view:** named query, expanded inline at execution time. Just
  syntactic sugar.
- **Materialised view:** precomputed result, refreshed on a schedule or
  on write.

For loveliness, the most useful version is **materialised view with
analytics**:

```
CREATE VIEW user_communities AS
  MATCH (u:User)-[:KNOWS]->(v:User)
  RETURN u, v
WITH ANALYTICS leiden(gamma=1.0)
REFRESH ON WRITE TO User, KNOWS;
```

That's exactly what the system plan needs: a precomputed Leiden snapshot
the viz can fetch as Arrow.

But: views need DDL syntax, which means parser work, which means it's not
a spike — it's a feature. **Spike scope: prove the plugin contract, then
treat materialised views as a downstream consumer of the same plugin
registry.** A view is just "a saved query + selected analytics, refreshed
on some trigger."

## Acceptance criteria for the spike

- [ ] One plugin registry with two registered plugins (one trivial, one
      graph-shape).
- [ ] `POST /db/{name}/query` accepting `{cypher, analytics[]}` JSON.
- [ ] Trivial plugin: `count_by_label` — counts rows grouped by a label
      column.
- [ ] Graph plugin: `connected_components` — Leiden stand-in via union-find.
- [ ] Bench: plugin overhead <50ms at 10K rows for the trivial plugin.
- [ ] No regression on the existing `/cypher` endpoint.

## Non-goals

- Cypher CALL syntax
- View DDL
- Plugin sandboxing / process isolation
- Plugin auth (assume same auth scope as the underlying query)
- Streaming/incremental plugins
- Cross-shard plugin merging (single-node spike)
