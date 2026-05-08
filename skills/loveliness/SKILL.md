---
name: loveliness
description: Query, write, and introspect a Loveliness graph database through its MCP server. Triggers when the user asks to run Cypher against Loveliness, load CSV data into the graph, inspect node/edge tables, check shard or cluster health, or otherwise drive a Loveliness cluster from an LLM. Also triggers on phrases like "loveliness query", "loveliness schema", "load nodes into loveliness", "loveliness cluster status", or any Cypher-shaped task targeted at a graph called Loveliness.
---

# Loveliness

Loveliness is a clustered graph database with a Cypher query layer. This skill drives it through the `loveliness` MCP server (run by `loveliness-mcp` or `loveliness mcp`).

## Tools

Read-only:

- `schema` — node + edge tables with property names, types, and primary keys. Cached 30s.
- `cypher_read` — run a Cypher read query. Rejects writes (`CREATE`/`MERGE`/`SET`/`DELETE`/`DROP`/`REMOVE`/`LOAD`/`COPY`/`ALTER`/`DETACH`) and mutating `CALL` procedures.
- `cluster_status` — leader, peers, per-shard assignment, registered schema.
- `list_databases` — databases in the cluster catalog with state, shard count, and creation time.
- `describe_schema` — schema joined with annotations in one payload. **Use this first** when you need to plan a query — it shows what each table holds and the example queries the operator left behind.
- `list_annotations` / `get_annotation` — read schema annotations (descriptions, query examples, tags) for an element.

Write (skipped when the server is launched with `--readonly`):

- `cypher_write` — Cypher writes / DDL.
- `create_node_table`, `create_edge_table`, `drop_table` — typed schema management. Builds the DDL from structured input and runs it; bust the schema cache so the next `schema` call is fresh. Prefer these over hand-built `cypher_write` DDL.
- `create_database`, `drop_database`, `start_database`, `stop_database` — database lifecycle. Wraps `/admin/cypher`. Must hit the leader (call `cluster_status` if you get `NOT_LEADER`).
- `admin_cypher` — escape hatch for raw admin commands.
- `bulk_nodes`, `bulk_edges` — synchronous CSV load. Provide either inline `csv_data` or a host-readable `csv_path`.
- `ingest_nodes`, `ingest_edges` — async CSV load. Returns a `job_id` to poll with `ingest_status`.
- `set_annotation`, `delete_annotation` — attach or remove a description, query examples, and tags on a schema target. Latest-wins (a SET replaces the previous body). Replicated through Raft alongside the schema, so annotations survive restart and follow the data.

Resources (read-only blobs the client can fetch directly):

- `loveliness://schema` — same payload as `schema`.
- `loveliness://cluster` — same payload as `cluster_status`.

## Working pattern

1. **Always start with `describe_schema`.** It returns the schema and the cluster's annotations (descriptions, query examples) joined. If `describe_schema` is missing, fall back to `schema` then `list_annotations`.
2. **Use `params`, not string interpolation.** The `query` field accepts `$name` placeholders that map to the `params` object. Loveliness inlines parameters as escaped Cypher literals, so user-controlled strings stay safely quoted. Don't build queries with string concatenation.
3. **Pick the right write tool:**
   - Schema (DDL): `create_node_table` / `create_edge_table` / `drop_table` over `cypher_write`. They validate identifiers and bust the schema cache.
   - Database lifecycle: `list_databases`, `create_database`, `drop_database`. Skip if the cluster is single-DB.
   - One-off data mutation: `cypher_write`.
   - Synchronous CSV ≤100K rows: `bulk_nodes` / `bulk_edges`.
   - Async / large CSV: `ingest_nodes` / `ingest_edges`, then poll `ingest_status` every few seconds until the job is `done`.
4. **When something fails:** call `cluster_status` first. A `503` or "shard unavailable" error usually means a peer is down or the shard map is mid-rebalance.

## Common shapes

Read by primary key:

```cypher
MATCH (p:Person {name: $name}) RETURN p
```
With `params: {"name": "Alice"}`.

Write a node:

```cypher
CREATE (p:Person {name: $name, age: $age}) RETURN p
```

Define a node table:

```
create_node_table(table="Person", properties=[
  {"name": "name", "type": "STRING", "primary_key": true},
  {"name": "age",  "type": "INT64"}
])
```

Define an edge table:

```
create_edge_table(table="KNOWS", from="Person", to="Person",
                  properties=[{"name": "since", "type": "DATE"}])
```

Bulk-load nodes (synchronous):

```
bulk_nodes(table="Person", csv_data="name,age\nAlice,30\nBob,25\n")
```

Async edge load:

```
ingest_edges(rel_table="KNOWS", from_table="Person", to_table="Person", csv_path="/data/knows.csv")
→ {"job_id": "abc123", "status": "queued"}

ingest_status(job_id="abc123")
→ {"job": {"status": "running", "loaded": 1200000, ...}}
```

Annotate a table so future LLM turns know what it holds:

```
set_annotation(
  target="db:default/table:Person",
  description="People in the social graph. Each row is a unique human. PII.",
  examples=[
    {"title": "by name",  "query": "MATCH (p:Person {name: $name}) RETURN p", "params": {"name": "Alice"}},
    {"title": "top by friends", "query": "MATCH (p:Person)-[:KNOWS]->(f) RETURN p, count(f) AS n ORDER BY n DESC LIMIT $k", "params": {"k": 10}}
  ],
  tags=["core", "pii"]
)
```

## Annotation targets

`set_annotation` takes a hierarchical `target` string:

- `cluster` — top-level "what is this cluster" description.
- `db:<name>` — what a database holds.
- `db:<name>/table:<name>` — node table description + example queries.
- `db:<name>/table:<name>/property:<name>` — column-level note (units, format, deprecated, etc.).
- `db:<name>/edge:<name>` — relationship table.
- `db:<name>/edge:<name>/property:<name>` — edge column.
- `db:<name>/saved_query:<id>` — a named, parameterized query template.

For single-database clusters use `db:default`. Annotations are latest-wins (a SET replaces the body) and replicated through Raft like the schema and catalog — they survive restarts and follow snapshots.

## Common errors

- `cypher parse error` — Cypher didn't parse. Re-read `schema` to confirm table/column names; LadybugDB is case-sensitive.
- `cypher_read rejects write statements` — switch to `cypher_write`, or rephrase as a `MATCH ... RETURN` if you really meant a read.
- `schema propagation in progress, retry` — another node is broadcasting a DDL change. Wait a moment and retry.
- `shard unavailable` — call `cluster_status`; the peer for that shard is down or replaying.
- `query exceeded timeout` — narrow the query (add `LIMIT`, tighter `WHERE`) or relaunch the server with a longer `--timeout`.

## Don't

- Don't paste user-controlled input into the `query` string. Use `params`.
- Don't loop `ingest_status` faster than every 1–2 seconds — async ingest is throughput-oriented, not real-time.
- Don't call `cypher_write` for bulk data. The bulk endpoints are an order of magnitude faster.
- Don't call mutating `CALL` procedures (`drop_table`, `export_database`, …) through `cypher_read`; the server rejects them. Use `cypher_write` if you really intend a mutation.

## Configuration

The server is configured by env vars / flags at launch:

- `LOVELINESS_URL` (default `http://localhost:8080`)
- `LOVELINESS_TOKEN` — bearer token for auth-enabled clusters
- `LOVELINESS_TIMEOUT` (default `30s`)
- `LOVELINESS_READONLY=true` — register read-only tools only

These are set in the MCP client config (e.g. `~/.claude.json`), not at tool-call time.
