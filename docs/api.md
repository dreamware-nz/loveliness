# HTTP API Reference

All endpoints are on the HTTP port (default `:8080`).

> For LLM agents, prefer the [MCP server](mcp.md) — it wraps these
> endpoints in a typed, schema-aware tool surface.

## Query Endpoint

`POST /cypher` — send raw Cypher as the request body, get JSON results back.

For Cypher + post-execution analytics (community detection, plateau
discovery, etc.) in a single round-trip, see
[Analytics Plugins](analytics.md) — `POST /db/{name}/query` is a
strict superset of `/db/{name}/cypher`.

## Schema

LadybugDB requires schema before inserting data. The `PRIMARY KEY` becomes the **shard key**.

```bash
# Create a node table — sharded on 'name'
curl -s localhost:8080/cypher -d "CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))"

# Create a relationship table
curl -s localhost:8080/cypher -d "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)"
```

Schema DDL is automatically broadcast to all shards.

## Queries

**Point lookup (single shard via Bloom filter):**
```bash
curl -s localhost:8080/cypher -d "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age, p.city"
```

**Scan (scatter-gather):**
```bash
curl -s localhost:8080/cypher -d "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 10"
```

**Traversal:**
```bash
curl -s localhost:8080/cypher -d "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name"
```

**Response format:**
```json
{
  "columns": ["p.name", "p.age", "p.city"],
  "rows": [{"p.name": "Alice", "p.age": 30, "p.city": "Auckland"}],
  "stats": {"compile_time_ms": 0.12, "exec_time_ms": 0.45}
}
```

### Apache Arrow output

`/cypher` will return [Apache Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#format-ipc)
when the client asks for it via the `Accept` header. Arrow responses
skip the JSON encode/decode round-trip and stay zero-copy across the
network and pandas / polars / DuckDB on the consumer side, which is a
material throughput win on result sets above ~10k rows.

The full Cypher→Arrow type mapping is documented in
[`arrow-mapping.md`](arrow-mapping.md) — that doc is the contract for
schema metadata, mid-stream error handling, and version negotiation.

| Accept value | Response Content-Type | Format |
|---|---|---|
| `application/json` *(default)* | `application/json` | JSON |
| `application/vnd.apache.arrow.stream` | `application/vnd.apache.arrow.stream` | Arrow IPC stream (schema → batches → EOS) |
| `application/vnd.apache.arrow.file` | `application/vnd.apache.arrow.file` | Arrow IPC file (random-access, `ARROW1` magic) |
| `*/*` or `application/*` | `application/json` | JSON (fallback) |
| anything else (concrete) | — | `406 Not Acceptable` |

Stream and file are **not** byte-interchangeable. Use `pyarrow.ipc.open_stream` /
`ipc.NewReader` / DuckDB-WASM `read_arrow` for the stream variant, and
`pyarrow.ipc.open_file` / `ipc.NewFileReader` for the file variant.

Quality values (`q=0.9`) are honored; ties broken by header order.

```bash
# Stream variant — best for piping into Python / Polars
curl -s localhost:8080/cypher \
  -H 'Accept: application/vnd.apache.arrow.stream' \
  -d 'MATCH (p:Person) RETURN p.name, p.age LIMIT 10000' \
  | python3 -c '
import pyarrow.ipc, sys
reader = pyarrow.ipc.open_stream(sys.stdin.buffer)
print(reader.read_all().to_pandas().head())
'
```

#### Type mapping

Each result column is classified by walking every value once and
picking the narrowest Arrow type that fits:

| Result values | Arrow type |
|---|---|
| all booleans | `bool` |
| all integers | `int64` |
| mix of integers and floats | `float64` |
| all strings | `utf8` |
| anything else (lists, maps, nodes, mixed types) | `utf8` (JSON-encoded) |

A column is nullable if any row has the field missing or `null`.
Heterogeneous and complex columns currently fall back to JSON-encoded
`utf8` — proper struct/list types are tracked as a follow-up to #27.

## Writes

```bash
# Create a node
curl -s localhost:8080/cypher -d "CREATE (p:Person {name: 'Alice', age: 30, city: 'Auckland'})"

# Create an edge (routed to source node's shard)
curl -s localhost:8080/cypher -d "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2024}]->(b)"
```

## Bulk Loading

**Synchronous bulk load:**
```bash
# Load nodes from CSV
curl -s localhost:8080/bulk/nodes \
  -H "Content-Type: text/csv" \
  -H "X-Table: Person" \
  --data-binary @persons.csv

# Two-pass edge loading for cross-shard performance
# Pass 1: Create reference nodes
curl -s localhost:8080/bulk/edges \
  -H "X-Rel-Table: KNOWS" -H "X-From-Table: Person" -H "X-To-Table: Person" \
  -H "X-Refs-Only: true" \
  --data-binary @edges.csv

# Pass 2: Load edges (refs already exist)
curl -s localhost:8080/bulk/edges \
  -H "X-Rel-Table: KNOWS" -H "X-From-Table: Person" -H "X-To-Table: Person" \
  -H "X-Skip-Refs: true" \
  --data-binary @edges.csv
```

**Async ingest queue:**
```bash
# Submit — returns immediately with job ID
curl -s -X POST localhost:8080/ingest/nodes \
  -H "X-Table: Person" \
  --data-binary @persons.csv
# → {"job_id": "20260327-120000-abc123", "status": "pending"}

# Poll status
curl -s localhost:8080/ingest/jobs/20260327-120000-abc123
# → {"status": "completed", "loaded": 5000000, ...}

# List all jobs
curl -s localhost:8080/ingest/jobs
```

The ingest queue spools the CSV to disk, returns 202 Accepted, and processes jobs sequentially in the background. Jobs survive server restarts.

## Disaster Recovery

**Backup and restore:**
```bash
curl -s localhost:8080/backup -o backup.tar.gz
curl -s -X POST localhost:8080/restore --data-binary @backup.tar.gz
curl -s localhost:8080/wal/status
```

**CSV export:**
```bash
curl -s localhost:8080/export/Person
curl -s localhost:8080/export/Person/edges/KNOWS
```

S3 scheduled backups are configured via environment variables (see [Configuration](configuration.md)).

## Write Consistency

| Level | Behavior | Use case |
|---|---|---|
| `ONE` | Ack after primary write, async replicate | Fast writes, acceptable loss window |
| `QUORUM` | Ack after primary + 1 replica (default) | Safe default for most workloads |
| `ALL` | Ack after all replicas confirm | Maximum durability |

## Cluster Management

```bash
# Health check (always public, no auth required)
curl -s localhost:8080/health | jq

# Discovery info (always public, no auth required — used by DNS peer discovery)
curl -s localhost:8080/discovery | jq
# → {"node_id":"node-1","raft_addr":":9000","grpc_addr":":9001","http_addr":":8080","bolt_addr":":7687"}

# Cluster status
curl -s localhost:8080/cluster | jq

# Generate a join token (leader only, single-use, 10 min TTL)
curl -s -X POST localhost:8080/join-token
# → {"token": "a1b2c3...", "expires_at": "2026-03-27T12:10:00Z"}

# Add a node with join token
curl -s localhost:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000",
  "grpc_addr": "node4:9001",
  "http_addr": "node4:8080",
  "bolt_addr": "node4:7687",
  "join_token": "a1b2c3..."
}'
```

## Schema Annotations

Schema annotations attach descriptions, parameterized query examples,
and tags to schema elements. They are first-class state in the
cluster — replicated through Raft alongside the shard map and the
database catalog, and survive restart and snapshot.

Targets follow a path-shaped scheme: `cluster`, `db:<name>`,
`db:<name>/table:<name>`, `db:<name>/table:<name>/property:<name>`,
`db:<name>/edge:<name>`, `db:<name>/saved_query:<id>`.

```bash
# List all annotations (optionally filter by prefix).
curl -s 'localhost:8080/annotations?prefix=db:default/' | jq

# Read a single annotation.
curl -s 'localhost:8080/annotations/db:default/table:Person' | jq

# Set an annotation. Latest-wins — the body replaces any existing one.
# Must hit the leader.
curl -s -X POST localhost:8080/annotations -H 'Content-Type: application/json' -d '{
  "target": "db:default/table:Person",
  "description": "People in the social graph. PII.",
  "examples": [
    {"title": "by name",
     "query": "MATCH (p:Person {name: $name}) RETURN p",
     "params": {"name": "Alice"}}
  ],
  "tags": ["core", "pii"]
}'

# Delete an annotation.
curl -s -X DELETE 'localhost:8080/annotations/db:default/table:Person'
```
