# MCP Server Design

**Date**: 2026-04-30
**Status**: Draft
**Scope**: new `cmd/loveliness-mcp/` binary and new `pkg/mcp/` package

## Problem

There's no first-class way for LLM agents (Claude Code, Claude Desktop, Cursor, Zed, custom harnesses) to talk to a Loveliness cluster. Today an agent has to shell out to `loveliness query ...` or curl `/cypher` — both work, but the agent has no schema introspection, no typed tool surface, no separation of read vs write, and no async ingest affordance. That pushes Cypher string-building and error handling into every prompt.

The Model Context Protocol (MCP) is the emerging standard for exposing tools to LLM clients over stdio or HTTP. Giving Loveliness a native MCP server lets any MCP-aware client drive the database with schema-aware, permission-gated tools and zero glue code on the caller side.

## Goal

`loveliness mcp` (and a standalone `loveliness-mcp` binary) starts an MCP server that speaks JSON-RPC over stdio, connects to a running Loveliness cluster via HTTP, and exposes a small, opinionated set of tools that cover the 80% use case: querying the graph, writing to it, introspecting schema, bulk/async ingest, and checking cluster health.

Concretely, after this lands:

```jsonc
// ~/.claude.json (user scope)
{
  "mcpServers": {
    "loveliness": {
      "command": "loveliness-mcp",
      "env": { "LOVELINESS_URL": "http://localhost:8080" }
    }
  }
}
```

…gives Claude Code six tools (`cypher_read`, `cypher_write`, `schema`, `bulk_nodes`, `ingest_nodes`, `cluster_status`) and one readable resource (`loveliness://schema`).

## Design

### New binary

| Path | Role |
|------|------|
| `cmd/loveliness-mcp/main.go` | Thin entrypoint. Parses flags + env, constructs `pkg/mcp.Server`, runs it on stdio until EOF. |
| `cmd/loveliness/main.go` | Existing CLI gets a new `mcp` subcommand that calls into the same `pkg/mcp.Server` — so `loveliness mcp` and `loveliness-mcp` are equivalent. |

Both ship from the existing `.goreleaser.yml` build matrix.

### New package: `pkg/mcp/`

```
pkg/mcp/
  server.go            — MCP server setup, tool + resource registration, stdio loop
  client.go            — HTTP client adapter over /cypher, /bulk/*, /ingest/*, /status
  tools_cypher.go      — cypher_read, cypher_write
  tools_schema.go      — schema (CALL SHOW_TABLES + property types)
  tools_bulk.go        — bulk_nodes, bulk_edges (synchronous)
  tools_ingest.go      — ingest_nodes, ingest_edges (async, returns job ID)
  tools_cluster.go     — cluster_status
  resources.go         — loveliness://schema, loveliness://cluster
  errors.go            — map Loveliness HTTP errors → MCP error payloads
  *_test.go            — table-driven tests against a fake HTTP backend (httptest.Server)
```

SDK: `github.com/modelcontextprotocol/go-sdk` (official Go MCP SDK). Adds one top-level dep to `go.mod`.

### Transport

**stdio only in v1.** This is what Claude Code, Claude Desktop, Cursor, and Zed all use. HTTP/SSE transport is a v2 concern (deploying MCP servers behind a load balancer), not needed for local use.

### Configuration

Env vars (precedence: flag > env > default):

| Variable | Flag | Default | Purpose |
|----------|------|---------|---------|
| `LOVELINESS_URL` | `--url` | `http://localhost:8080` | HTTP endpoint of a Loveliness node. |
| `LOVELINESS_TOKEN` | `--token` | *(none)* | Bearer token, forwarded as `Authorization: Bearer <token>`. Wired through `pkg/auth` if auth is enabled on the cluster. |
| `LOVELINESS_TIMEOUT` | `--timeout` | `30s` | Per-request HTTP timeout. Cypher queries that exceed this return an MCP timeout error. |
| `LOVELINESS_READONLY` | `--readonly` | `false` | When true, `cypher_write`, `bulk_*`, and `ingest_*` tools are not registered. Useful for read-only agents. |

No config file in v1 — the server is meant to be ephemeral (client launches it per session).

### Tools

Each tool's name, input schema, and behavior. Schemas are JSON Schema as the SDK expects.

#### `cypher_read`

Run a read-only Cypher query. Statically rejects any statement whose first non-comment keyword is `CREATE`, `MERGE`, `SET`, `DELETE`, `DROP`, `REMOVE`, or `LOAD`. Parameters are bound via the JSON-RPC request, never string-interpolated.

```jsonc
{
  "name": "cypher_read",
  "input": {
    "query":  "MATCH (p:Person {name: $name}) RETURN p.age",
    "params": { "name": "Alice" }
  },
  "output": {
    "columns": ["p.age"],
    "rows":    [[30]],
    "stats":   { "rows_returned": 1, "ms": 2 }
  }
}
```

#### `cypher_write`

Mirror of `cypher_read` without the statement gate. Registered only when `LOVELINESS_READONLY=false`. Clients that want permission gating (Claude Code's `/permissions`) can opt-in per-tool.

#### `schema`

Returns the node and edge table catalog with property names and types. Internally runs `CALL SHOW_TABLES()` and per-table `CALL TABLE_INFO(name)`, fans out, and structures the result:

```jsonc
{
  "node_tables": [
    { "name": "Person", "primary_key": "name",
      "properties": [{"name":"name","type":"STRING"},{"name":"age","type":"INT64"}] }
  ],
  "edge_tables": [
    { "name": "KNOWS", "from": "Person", "to": "Person", "properties": [] }
  ]
}
```

Cached in-process for 30s with a sync.Mutex — schema is called on almost every agent turn for context, and hammering the cluster for unchanged data is wasteful.

#### `bulk_nodes` / `bulk_edges`

Wrap `POST /bulk/nodes` and `POST /bulk/edges`. Input is either an inline CSV string (`csv_data`) or a file path (`csv_path`). File path is resolved on the MCP server host, so only files readable by the loveliness-mcp process are accessible. Required header `X-Table` is taken from the tool input `table`.

#### `ingest_nodes` / `ingest_edges`

Same input surface as the bulk tools but hit `POST /ingest/nodes` — returns the 202 job ID as tool output. A separate `ingest_status` tool takes a job ID and returns progress.

#### `cluster_status`

Returns shard count, peer list, leader node, and per-shard health from `GET /status`. Read-only; always registered.

### Resources

MCP resources are read-only, URL-addressed blobs the client can fetch. Two:

- `loveliness://schema` — the same payload as the `schema` tool, for clients that prefer resource polling to tool calls.
- `loveliness://cluster` — the same as `cluster_status`.

Resources are cheap to add and some clients (e.g. Claude Desktop) surface them in UI.

### Error mapping

Every HTTP error becomes a structured MCP error so the LLM gets actionable feedback:

| Loveliness response | MCP error code | Message |
|---------------------|----------------|---------|
| 400 with parser error | `-32602` (invalid params) | Cypher parse error + line/column |
| 409 `BROADCAST_PARTIAL` | `-32000` (server error) | "Schema propagation in progress, retry" |
| 503 (shard down) | `-32000` | "Shard N unavailable, check cluster_status" |
| Timeout | `-32001` | "Query exceeded timeout of Xs" |

### Testing

- **Unit:** `httptest.NewServer` stub returns canned JSON; each tool has a table-driven test covering happy path, validation failures, HTTP error mapping.
- **Integration:** `test/mcp/e2e_test.go` spins `loveliness up 1` in a subprocess, launches `loveliness-mcp`, drives it with a real MCP Go client, exercises `schema → cypher_write → cypher_read → cluster_status`. Gated behind `-tags=integration` so `make test` stays fast.
- **Linter:** new package gets added to `.golangci.yml`'s coverage list.

### Documentation

New file `docs/mcp.md`:

1. What MCP is (one paragraph + link to spec).
2. Install snippet for Claude Code (`claude mcp add loveliness -- loveliness-mcp`), Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`), and Zed.
3. Tool reference: name, input schema, example.
4. Readonly-mode pattern for CI / untrusted agents.
5. Auth token pattern.
6. Pointer to `docs/api.md` for the underlying HTTP surface the MCP server wraps.

README gets a new section "MCP (LLM agents)" above the Kubernetes section, with the 4-line install snippet and a link to `docs/mcp.md`.

## Files changed

- `cmd/loveliness-mcp/main.go` — new, ~80 LOC.
- `cmd/loveliness/main.go` — add `mcp` subcommand, ~15 LOC.
- `pkg/mcp/*.go` — new package, ~800 LOC incl. tests.
- `go.mod` / `go.sum` — add `github.com/modelcontextprotocol/go-sdk`.
- `docs/mcp.md` — new.
- `docs/api.md` — add a one-line pointer back to the MCP doc.
- `README.md` — new "MCP (LLM agents)" section.
- `.goreleaser.yml` — add `loveliness-mcp` binary to the build matrix.
- `Makefile` — `make mcp` target that builds just the MCP binary for iteration.

## Out of scope

- **HTTP/SSE MCP transport.** v1 is stdio. Adding HTTP transport is a natural follow-up once there's a real deployment story (e.g. "MCP gateway for a shared Loveliness cluster").
- **Bolt-based transport.** The MCP server talks HTTP to Loveliness. Bolt would be faster but forces a Bolt driver dep and doesn't unlock anything the HTTP path can't do today.
- **Write-path gating beyond read/write split.** Fine-grained per-table ACLs, row-level filters, query budgeting — belongs in `pkg/auth`, not the MCP layer.
- **LLM prompt engineering / tool descriptions for specific models.** Tool descriptions are written to be model-agnostic. Claude / GPT-specific tuning happens on the client side.
- **`cypher_read` → structured graph objects.** v1 returns raw columnar rows from `/cypher`. A future `graph_query` tool could return typed `{nodes, edges, paths}` objects after shape detection, but the underlying API needs shape hints first.
- **Streaming results.** Large result sets are truncated at 10k rows in v1 with a `truncated: true` flag. MCP does support streaming responses; wire it up once a real use case appears.
