# MCP (Model Context Protocol) server

Loveliness ships a first-class MCP server so LLM agents (Claude Code,
Claude Desktop, Cursor, Zed, custom harnesses) can drive the database
through typed, schema-aware tools instead of string-building raw Cypher.

The [Model Context Protocol](https://modelcontextprotocol.io) is the
emerging standard for exposing tools to LLM clients over stdio or HTTP.
The Loveliness MCP server speaks JSON-RPC over stdio, connects to a
running cluster via HTTP, and exposes a small opinionated tool surface.

## Install

### One-shot installer

```bash
make install-mcp
```

This runs `scripts/install-mcp.sh`, which:

1. Builds `loveliness-mcp` into `$GOBIN` (or `$(go env GOPATH)/bin`).
2. Registers it with `claude mcp add` if the `claude` CLI is on PATH;
   otherwise prints the manual `~/.claude.json` snippet.
3. Symlinks the bundled skill at `skills/loveliness/SKILL.md` into
   `~/.claude/skills/loveliness/` so Claude Code gets prose guidance
   on how to use the tools.

Honors `LOVELINESS_URL` and `LOVELINESS_TOKEN` from the environment.
Pass `FLAGS=--no-skill` or `FLAGS=--no-register` to skip steps.

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
claude mcp add loveliness -- loveliness-mcp
# or with config:
claude mcp add loveliness -e LOVELINESS_URL=http://localhost:8080 -- loveliness-mcp
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

### `cypher_write`

Same input shape as `cypher_read`, no keyword gate. Registered only
when `--readonly=false`. Clients that want per-tool gating (Claude
Code's `/permissions`) can opt-in this tool specifically.

### `schema`

Return node and edge tables with property names and types. Cached for
30 seconds in-process to avoid hammering the cluster on every turn.

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

## Resources

Two read-only resources, same payload as the equivalent tools:

- `loveliness://schema`
- `loveliness://cluster`

## Readonly pattern

Run with `--readonly` (or `LOVELINESS_READONLY=true`) to only register
`cypher_read`, `schema`, and `cluster_status`. Use this for CI
analysis agents, or when the agent should be allowed to explore the
graph but not mutate it.

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
