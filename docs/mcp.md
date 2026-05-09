# MCP (Model Context Protocol) server

Loveliness ships a first-class MCP server so LLM agents (Claude Code,
Claude Desktop, Cursor, Zed, custom harnesses) can drive the database
through typed, schema-aware tools instead of string-building raw Cypher.

The [Model Context Protocol](https://modelcontextprotocol.io) is the
emerging standard for exposing tools to LLM clients over stdio or HTTP.
The Loveliness MCP server speaks JSON-RPC over stdio, connects to a
running cluster via HTTP, and exposes a small opinionated tool surface.

## Install

### Claude Code plugin (recommended)

```bash
make install-plugin
```

This runs `scripts/install-plugin.sh`, which:

1. Builds `loveliness-mcp` into `$GOBIN` (or `$(go env GOPATH)/bin`).
2. Registers this repo as a Claude Code plugin marketplace
   (`claude plugin marketplace add <repo>`).
3. Installs the plugin at user scope
   (`claude plugin install loveliness@loveliness --scope user`).

The plugin ships `.claude-plugin/plugin.json`, `.mcp.json`, and the
`skills/loveliness/` skill, so the MCP server and skill are visible
from every Claude Code project after a restart.

Pass `FLAGS=--no-build` to skip the Go build, or `FLAGS=--update` to
refresh an already-registered marketplace.

### One-shot MCP-only installer

```bash
make install-mcp
```

This runs `scripts/install-mcp.sh`, which:

1. Builds `loveliness-mcp` into `$GOBIN` (or `$(go env GOPATH)/bin`).
2. Registers it with `claude mcp add --scope user` so the server is
   visible from every Claude Code project; falls back to printing the
   manual `~/.claude.json` snippet if the `claude` CLI is missing.
3. Symlinks the bundled skill at `skills/loveliness/SKILL.md` into
   `~/.claude/skills/loveliness/` so Claude Code gets prose guidance
   on how to use the tools.

Honors `LOVELINESS_URL` and `LOVELINESS_TOKEN` from the environment.
Pass `FLAGS=--local` to register at project scope instead, or
`FLAGS=--no-skill` / `FLAGS=--no-register` to skip steps.

Use `install-plugin` if you want the same setup wrapped as a Claude
Code plugin (single uninstall via `claude plugin uninstall`).

### Manual install

Two equivalent ways to run the server:

```bash
# standalone binary (shipped in releases alongside `loveliness`)
loveliness-mcp --url http://localhost:8080

# same thing, from the main CLI
loveliness mcp --url http://localhost:8080
```

### Claude Code

```bash
claude mcp add --scope user loveliness -- loveliness-mcp
# or with config:
claude mcp add --scope user loveliness -e LOVELINESS_URL=http://localhost:8080 -- loveliness-mcp
```

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```jsonc
{
  "mcpServers": {
    "loveliness": {
      "command": "loveliness-mcp",
      "env": { "LOVELINESS_URL": "http://localhost:8080" }
    }
  }
}
```

### Zed

Edit `~/.config/zed/settings.json`:

```jsonc
{
  "context_servers": {
    "loveliness": {
      "command": "loveliness-mcp",
      "env": { "LOVELINESS_URL": "http://localhost:8080" }
    }
  }
}
```

## Configuration

Flag precedence is `flag > env > default`.

| Variable | Flag | Default | Purpose |
|----------|------|---------|---------|
| `LOVELINESS_URL` | `--url` | `http://localhost:8080` | HTTP endpoint of a Loveliness node. |
| `LOVELINESS_TOKEN` | `--token` | *(none)* | Bearer token forwarded as `Authorization: Bearer <token>`. |
| `LOVELINESS_TIMEOUT` | `--timeout` | `30s` | Per-request HTTP timeout. |
| `LOVELINESS_READONLY` | `--readonly` | `false` | When true, register read-only tools only. |

## Tools

All tools share the same error envelope: on failure the tool result is
returned with `isError=true` and a text message like `error -32602: …`.
Numeric codes follow the JSON-RPC convention:

- `-32602` — invalid params (Cypher parse error, missing shard key, …)
- `-32000` — server error (shard unavailable, schema propagation, …)
- `-32001` — timeout

### `cypher_read`

Run a read-only Cypher query. Statically rejects any statement whose
first non-comment keyword is `CREATE`, `MERGE`, `SET`, `DELETE`, `DROP`,
`REMOVE`, `LOAD`, `COPY`, `ALTER`, or `DETACH`.

```jsonc
{
  "query":  "MATCH (p:Person {name: $name}) RETURN p.age",
  "params": { "name": "Alice" }
}
```

Returns `{columns, rows, stats}`.

#### Arrow output (`format: "arrow"`)

Add `"format": "arrow"` to receive the result as an Apache Arrow IPC
stream attached to the tool result as an embedded resource (MIME type
`application/vnd.apache.arrow.stream`):

```jsonc
{
  "query":  "MATCH (p:Person) RETURN p LIMIT 25000",
  "format": "arrow"
}
```

Tool result shape (abridged):

```jsonc
{
  "content": [
    { "type": "text", "text": "Arrow IPC stream attached (524288 bytes). MIME type application/vnd.apache.arrow.stream." },
    { "type": "resource", "resource": {
        "uri":      "loveliness:cypher-result.arrows",
        "mimeType": "application/vnd.apache.arrow.stream",
        "blob":     "<base64-encoded IPC stream>"
    }}
  ]
}
```

Use this from MCP clients that can ingest Arrow directly
(DuckDB-WASM `read_arrow`, pyarrow `ipc.open_stream`, polars
`read_ipc_stream`, …) — it skips the JSON marshal/unmarshal
round-trip and lands in the consumer's columnar memory layout
without an intermediate parse step. The schema follows the
[Cypher → Arrow type mapping](arrow-mapping.md). The default
(`format` omitted or `"json"`) keeps the existing
`{columns, rows, stats}` structured-content path that LLMs read
directly.

### `cypher_write`

Same input shape as `cypher_read`, including the `format` field, no
keyword gate. Registered only when `--readonly=false`. Clients that
want per-tool gating (Claude Code's `/permissions`) can opt-in this
tool specifically. The Arrow path on `cypher_write` is mostly useful
for write-then-`RETURN` queries where the returned rows feed an
analytics consumer.

### `create_node_table` / `create_edge_table` / `drop_table`

Typed wrappers over the equivalent `CREATE NODE TABLE` / `CREATE REL
TABLE` / `DROP TABLE` Cypher DDL. Build the DDL from a structured
input — table name, property list with types and primary key flags —
and bust the schema cache on success so a follow-up `schema` call is
fresh. Registered only when `--readonly=false`.

```jsonc
// create_node_table
{
  "table": "Person",
  "properties": [
    {"name": "name", "type": "STRING", "primary_key": true},
    {"name": "age",  "type": "INT64"}
  ]
}

// create_edge_table
{
  "table": "KNOWS",
  "from":  "Person",
  "to":    "Person",
  "properties": [{"name": "since", "type": "DATE"}]
}
```

Identifiers (table, property names, source/destination) are required
to match `[A-Za-z_][A-Za-z0-9_]*`. Property types pass through
permissive char-class validation; the cluster does the real type
check. Edge tables cannot have primary keys.

### `list_databases` / `create_database` / `drop_database` / `start_database` / `stop_database`

Database catalog management. Wraps `/admin/cypher`. Must hit the
leader — if you get `NOT_LEADER`, check `cluster_status`.
`list_databases` is read-only and always registered; the rest are
gated by `--readonly`.

```jsonc
// create_database
{ "name": "scratch", "shard_count": 4 }

// list_databases  → { "databases": [{ "name": "...", "state": "running", ... }] }
```

### `admin_cypher`

Escape hatch — sends a raw admin command (CREATE/STOP/START/DROP
DATABASE, SHOW DATABASES) to `/admin/cypher`. Prefer the typed tools.

### `schema`

Return node and edge tables with property names and types. Cached for
30 seconds in-process to avoid hammering the cluster on every turn.
Cache is invalidated automatically when `create_node_table`,
`create_edge_table`, or `drop_table` succeed.

```jsonc
{
  "node_tables": [
    { "name": "Person", "primary_key": "name",
      "properties": [{"name":"name","type":"STRING","primary_key":true},
                     {"name":"age","type":"INT64"}] }
  ],
  "edge_tables": [
    { "name": "KNOWS", "from": "Person", "to": "Person", "properties": [] }
  ]
}
```

### `bulk_nodes` / `bulk_edges`

Synchronous bulk load — wrap `POST /bulk/nodes` and `POST /bulk/edges`.
Provide either inline `csv_data` or a `csv_path` on the MCP server host.

`bulk_nodes`: `{table, csv_data | csv_path}`
`bulk_edges`: `{rel_table, from_table, to_table, csv_data | csv_path, skip_refs?}`

Returns `{table, loaded, errors?}`.

### `ingest_nodes` / `ingest_edges`

Async equivalents — return a `job_id` immediately. Poll with
`ingest_status`.

### `ingest_status`

Input: `{job_id}`. Returns the raw job record (status, loaded count,
errors).

### `cluster_status`

Return leader, peer list, per-shard assignment, and registered schema
from `GET /cluster`. Always registered.

### `list_annotations` / `get_annotation` / `set_annotation` / `delete_annotation` / `describe_schema`

Schema annotations are first-class state in Loveliness, persisted
through Raft alongside the shard map and database catalog. They hold
the natural-language descriptions and parameterized query examples
that bridge LLM intent to Cypher.

Targets follow a path-shaped scheme:

- `cluster`
- `db:<name>`
- `db:<name>/table:<name>` and `db:<name>/edge:<name>`
- `db:<name>/table:<name>/property:<name>` and edge equivalent
- `db:<name>/saved_query:<id>`

`describe_schema` is the primary read tool — it joins the schema with
all annotations for a database in one round trip:

```jsonc
{
  "cluster":  { "target": "cluster",         "description": "..." },
  "database": { "target": "db:default",      "description": "..." },
  "node_tables": [
    { "node_table": { "name": "Person", ... },
      "annotation": { "description": "people", "examples": [...] } }
  ],
  "edge_tables": [...]
}
```

`set_annotation` is gated by the same write protections as `cypher_write`
(`--readonly` skips it). Latest-wins semantics: a SET replaces the
entire body. The cluster auto-initializes the registry on first write.

```jsonc
// set_annotation
{
  "target": "db:default/table:Person",
  "description": "People in the social graph. PII.",
  "examples": [
    { "title": "by name",
      "query":  "MATCH (p:Person {name: $name}) RETURN p",
      "params": {"name": "Alice"} }
  ],
  "tags": ["core", "pii"]
}
```

The HTTP shape is `GET/POST/DELETE /annotations[/{target}]` with the
target encoded directly into the path (slashes preserved by the
`{target...}` route).

## Resources

Two read-only resources, same payload as the equivalent tools:

- `loveliness://schema`
- `loveliness://cluster`

## Readonly pattern

Run with `--readonly` (or `LOVELINESS_READONLY=true`) to only register
the read-only tools (`cypher_read`, `schema`, `cluster_status`,
`list_databases`, `describe_schema`, `list_annotations`,
`get_annotation`) and read-only resources. Mutating tools
(`cypher_write`, `create_*`, `drop_*`, `start_*`, `stop_*`,
`bulk_*`, `ingest_*`, `admin_cypher`, `set_annotation`,
`delete_annotation`) are not registered. Use this for CI analysis
agents, or when the agent should be allowed to explore the graph but
not mutate it.

```bash
loveliness-mcp --readonly --url https://prod-cluster.internal
```

## Auth

When the cluster has `LOVELINESS_AUTH_TOKEN` set, pass the same token
to the MCP server via `--token` or `LOVELINESS_TOKEN`. It is forwarded
as a Bearer token on every HTTP request.

```bash
LOVELINESS_TOKEN=secret loveliness-mcp --url https://cluster.internal
```

## Underlying HTTP surface

See [api.md](api.md) for the HTTP endpoints this MCP server wraps.
