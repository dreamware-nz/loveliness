# Loveliness

A clustered graph database built on [LadybugDB](https://github.com/LadybugDB/ladybug) — like Elasticsearch is to Lucene, Loveliness is to LadybugDB.

> *A "loveliness" is the collective noun for a group of ladybugs.*

Loveliness takes LadybugDB (a Kuzu fork — fast, embedded, columnar graph engine with Cypher support) and adds clustering: hash-based sharding, Raft consensus, scatter-gather query routing, async replication, and automatic failover. Every node runs a single Go binary.

## Architecture

```
                    ┌─────────────────────────────────────┐
    HTTP Client ──► │           Loveliness Node            │
                    │                                      │
                    │  ┌──────────┐  ┌──────────────────┐  │
                    │  │ HTTP API │  │   Raft Consensus  │  │
                    │  └────┬─────┘  └────────┬─────────┘  │
                    │       │                 │             │
                    │  ┌────▼─────────────────▼──────────┐ │
                    │  │         Query Router             │ │
                    │  │   parse → resolve → execute      │ │
                    │  └──┬──────────┬──────────┬────────┘ │
                    │     │          │          │           │
                    │  ┌──▼──┐  ┌───▼──┐  ┌───▼──┐        │
                    │  │Shard│  │Shard │  │Shard │         │
                    │  │  0  │  │  1   │  │  2   │         │
                    │  └─────┘  └──────┘  └──────┘         │
                    │  LadybugDB  LadybugDB  LadybugDB     │
                    └─────────────────────────────────────┘
```

- **Hash-based sharding**: `shard = fnv32a(node_id) % num_shards`
- **Edges stored on source node's shard**
- **Scatter-gather**: queries without shard keys fan out to all shards, results merged
- **Cross-shard traversals**: multi-hop resolution across shard boundaries
- **Raft consensus** (hashicorp/raft): cluster state, shard map, node membership
- **Async replication**: primary + 1 replica per shard
- **Automatic failover**: Raft detects dead nodes, promotes replicas

## Quick Start

### Prerequisites

- Go 1.25+
- [LadybugDB](https://ladybugdb.com/) shared library installed
- Docker + Docker Compose (for cluster mode)

### Single Node

```bash
# Build
make build

# Run with defaults (3 shards, port 8080, bootstrap mode)
LOVELINESS_BOOTSTRAP=true ./loveliness
```

### 3-Node Cluster (Docker Compose)

```bash
# Start the cluster
docker compose up --build

# Node 1 (leader): http://localhost:8080
# Node 2:          http://localhost:8081
# Node 3:          http://localhost:8082
```

### From Source

```bash
git clone https://github.com/dreamware-nz/loveliness.git
cd loveliness
make test    # run all tests
make build   # build the binary
```

## Usage

### Create a Node Table

LadybugDB requires schema definition before inserting data:

```bash
curl -s localhost:8080/query -d '{
  "cypher": "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))"
}'
```

### Create Nodes

```bash
curl -s localhost:8080/query -d '{
  "cypher": "CREATE (p:Person {name: '\''Alice'\'', age: 30})"
}'

curl -s localhost:8080/query -d '{
  "cypher": "CREATE (p:Person {name: '\''Bob'\'', age: 25})"
}'
```

### Query by Key (Single Shard)

When you query with a property value, Loveliness extracts it as a shard key and routes directly to the owning shard:

```bash
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person {name: '\''Alice'\''}) RETURN p"
}'
```

### Query All (Scatter-Gather)

Queries without a shard key fan out to all shards and merge results:

```bash
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person) RETURN p"
}'
```

### Health Check

```bash
curl -s localhost:8080/health
# {"status":"ok","role":"leader","node_id":"node-1"}
```

### Cluster Status

```bash
curl -s localhost:8080/cluster
# {"is_leader":true,"leader":"...","node_id":"node-1","nodes":{...},"shard_map":{...}}
```

### Join a Node

On the leader, add a new node to the cluster:

```bash
curl -s localhost:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000"
}'
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `LOVELINESS_NODE_ID` | `node-1` | Unique node identifier |
| `LOVELINESS_BIND_ADDR` | `:8080` | HTTP API listen address |
| `LOVELINESS_RAFT_ADDR` | `:9000` | Raft consensus address |
| `LOVELINESS_GRPC_ADDR` | `:9001` | Internal gRPC address |
| `LOVELINESS_DATA_DIR` | `./data` | Base directory for shard data and Raft state |
| `LOVELINESS_SHARD_COUNT` | `3` | Total number of shards |
| `LOVELINESS_BOOTSTRAP` | `false` | Bootstrap a new cluster (first node only) |
| `LOVELINESS_PEERS` | *(empty)* | Comma-separated list of peer Raft addresses |
| `LOVELINESS_MAX_CONCURRENT_QUERIES` | `16` | Max concurrent CGo calls per shard |
| `LOVELINESS_QUERY_TIMEOUT_MS` | `30000` | Per-shard query timeout in milliseconds |

## API Reference

### `POST /query`

Execute a Cypher query. The router parses the query, extracts shard keys, and routes to the appropriate shard(s).

**Request:**
```json
{"cypher": "MATCH (p:Person {name: 'Alice'}) RETURN p"}
```

**Response (200):**
```json
{
  "columns": ["p"],
  "rows": [{"name": "Alice", "age": 30}],
  "stats": {"compile_time_ms": 0.12, "exec_time_ms": 0.45}
}
```

**Partial response (200, scatter-gather with shard failures):**
```json
{
  "columns": ["p"],
  "rows": [...],
  "partial": true,
  "errors": [{"shard_id": 2, "error": "shard 2 is unhealthy"}]
}
```

**Error codes:**

| Code | HTTP Status | Meaning |
|---|---|---|
| `CYPHER_PARSE_ERROR` | 400 | Empty or unparseable query |
| `MISSING_SHARD_KEY` | 400 | Write query has no shard key for routing |
| `SHARD_UNAVAILABLE` | 503 | Target shard is unhealthy |
| `QUERY_TIMEOUT` | 504 | Query exceeded timeout |
| `BROADCAST_PARTIAL` | 500 | Schema DDL failed on some shards |
| `QUERY_ERROR` | 500 | LadybugDB execution error |

### `GET /health`

Node health check. Returns leader/follower role.

### `GET /cluster`

Cluster status: shard map, node list, current leader.

### `POST /join`

Join a new node to the cluster. Must be called on the leader.

## Supported Cypher

Loveliness passes all Cypher through to LadybugDB — the router only classifies queries for routing, it doesn't validate syntax.

| Category | Clauses | Routing |
|---|---|---|
| **Reads** | `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `CALL`, `RETURN`, `ORDER BY`, `LIMIT` | Shard key → single shard; no key → scatter-gather |
| **Writes** | `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE` | Shard key → single shard; no key → **rejected** (`MISSING_SHARD_KEY`) |
| **Schema DDL** | `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, `ALTER` | Broadcast to all shards |

**Why are keyless writes rejected?** Scatter-gathering a `SET` or `DELETE` across all shards would mutate data on shards that don't own it. Reads are safe to scatter-gather; writes are not. Always include a shard key (inline property or WHERE equality) in write queries.

## Project Structure

```
cmd/loveliness/main.go     Entry point — config, shard init, Raft, HTTP server, auto-join
pkg/
  logging/logging.go        Structured JSON logging setup (log/slog)
  config/config.go          Configuration from environment variables
  shard/
    store.go                Store interface (abstracts LadybugDB)
    lbug_store.go           Production Store via go-ladybug CGo bindings
    memory_store.go         In-memory mock Store for testing
    shard.go                Shard wrapper with semaphore + panic recovery
  router/
    parser.go               Cypher subset parser, shard key extraction
    router.go               Query routing: single-shard, scatter-gather
  cluster/
    fsm.go                  Raft FSM — shard map, node membership
    cluster.go              Raft lifecycle — bootstrap, join, failover
  api/
    api.go                  HTTP endpoints — /query, /health, /cluster, /join
Dockerfile                  Multi-stage build with LadybugDB
docker-compose.yml          3-node cluster for local testing
Makefile                    Build, test, run helpers
```

## How It Works

1. **Startup**: Node reads config from env, opens N LadybugDB instances (one per shard), starts Raft, starts HTTP server
2. **Query arrives** at `POST /query`
3. **Parser** extracts shard routing keys from inline properties `{name: 'Alice'}` or WHERE equality `WHERE p.name = 'Alice'`
4. **Router** hashes the key to a shard ID via FNV-32a
5. **Single-shard**: query goes directly to the owning shard
6. **Scatter-gather**: no shard key → fan out to all shards, merge results, return partial on timeout
7. **CGo safety**: each shard has a semaphore limiting concurrent LadybugDB calls, plus panic recovery that marks the shard unhealthy

## License

MIT
