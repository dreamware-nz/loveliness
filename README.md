# Loveliness

A clustered graph database built on [LadybugDB](https://github.com/LadybugDB/ladybug) — like Elasticsearch is to Lucene, Loveliness is to LadybugDB.

> *A "loveliness" is the collective noun for a group of ladybugs.*

## What is this?

LadybugDB is a Kuzu fork — a fast, embedded, columnar graph engine with Cypher support. It's excellent on a single machine but has no clustering story. Loveliness wraps LadybugDB in a distributed layer: declared shard keys, Raft consensus, cross-node query forwarding, write replication with tunable consistency, automatic shard rebalancing, and locality-aware placement.

Every node runs a single Go binary. No external dependencies besides LadybugDB itself.

## Architecture

```
                         ┌─────────────────────────────────────────────┐
         HTTP Client ──► │              Loveliness Node                │
                         │                                             │
                         │  ┌──────────┐  ┌───────────┐  ┌─────────┐  │
                         │  │ HTTP API │  │   Raft    │  │Transport│  │
                         │  │ /query   │  │ Consensus │  │ Client  │  │
                         │  └────┬─────┘  └─────┬─────┘  └────┬────┘  │
                         │       │              │              │       │
                         │  ┌────▼──────────────▼──────────────▼────┐  │
                         │  │           Query Router                 │  │
                         │  │  parse → schema lookup → route/forward │  │
                         │  └──┬──────────────────────────────┬─────┘  │
                         │     │ local                  remote│        │
                         │  ┌──▼───┐  ┌──────┐         POST /internal │
                         │  │Shard │  │Shard │         /query → peer  │
                         │  │  0   │  │  5   │                        │
                         │  └──────┘  └──────┘                        │
                         │  (only shards assigned to this node)       │
                         └─────────────────────────────────────────────┘
```

**Key properties:**

- **Declared shard keys** — each node table has a PRIMARY KEY that determines shard routing. `Person(name STRING, PRIMARY KEY(name))` means all Person queries route on `name`, not on arbitrary string values.
- **Shard placement** — shards are assigned to specific nodes (primary + replica). Nodes only open their assigned shards, not all shards.
- **Cross-node query forwarding** — if the target shard is on another node, the query is transparently forwarded via internal HTTP.
- **Write replication** — after writing to the primary shard, the write fans out to replicas with tunable consistency (ONE, QUORUM, ALL).
- **Automatic rebalancing** — when nodes join or leave, the leader computes a migration plan and redistributes shards.
- **Locality-aware placement** — tracks cross-shard traversal statistics and suggests colocation to reduce network hops.
- **Raft consensus** — cluster state (shard map, node membership, schema) is replicated via hashicorp/raft.

## Quick Start

### Prerequisites

- Go 1.25+
- [LadybugDB](https://ladybugdb.com/) shared library installed (`curl -fsSL https://install.ladybugdb.com | sh`)
- Docker + Docker Compose (for cluster mode)

### Run a Single Node

```bash
git clone https://github.com/dreamware-nz/loveliness.git
cd loveliness

# Build (requires LadybugDB installed locally)
make build

# Start a single-node cluster
make run
# → listening on :8080
```

### Run a 3-Node Cluster

```bash
# Start all three nodes (node1 bootstraps, node2/node3 auto-join)
make docker

# Nodes:
#   node1 (leader): http://localhost:8080
#   node2:          http://localhost:8081
#   node3:          http://localhost:8082

# Tear down
make docker-down
```

### Run Tests

```bash
make test          # verbose, all packages
make test-short    # quiet
make race          # with race detector
make cover         # coverage report
```

## How To Use It

### 1. Define Your Schema

LadybugDB requires schema before inserting data. The `PRIMARY KEY` you declare becomes the **shard key** — this is the value Loveliness hashes to decide which shard owns a node.

```bash
# Create a Person table — sharded on 'name'
curl -s localhost:8080/query -d '{
  "cypher": "CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))"
}'

# Create a relationship table
curl -s localhost:8080/query -d '{
  "cypher": "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)"
}'
```

Schema DDL is automatically broadcast to all shards.

### 2. Create Data

```bash
# Alice → shard determined by fnv32a("Alice") % num_shards
curl -s localhost:8080/query -d '{
  "cypher": "CREATE (p:Person {name: '\''Alice'\'', age: 30, city: '\''Auckland'\''})"
}'

# Bob → different shard (probably)
curl -s localhost:8080/query -d '{
  "cypher": "CREATE (p:Person {name: '\''Bob'\'', age: 25, city: '\''Wellington'\''})"
}'

# Create a relationship (routed to Alice's shard — edges live with their source)
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (a:Person {name: '\''Alice'\''}), (b:Person {name: '\''Bob'\''}) CREATE (a)-[:KNOWS {since: 2024}]->(b)"
}'
```

Write queries **must** include the shard key (the PRIMARY KEY property). If you omit it, the router returns `MISSING_SHARD_KEY` because scatter-gathering writes would mutate data on the wrong shards.

### 3. Query Data

**Point lookup (single shard):**
```bash
# Routes directly to Alice's shard — fast, no fan-out
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person {name: '\''Alice'\''}) RETURN p.name, p.age, p.city"
}'
```

**Scan (scatter-gather):**
```bash
# No shard key → fans out to all shards, merges results
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 10"
}'
```

**Traversal:**
```bash
# Who does Alice know?
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (a:Person {name: '\''Alice'\''})-[:KNOWS]->(b) RETURN b.name"
}'
```

**Response format:**
```json
{
  "columns": ["p.name", "p.age", "p.city"],
  "rows": [{"p.name": "Alice", "p.age": 30, "p.city": "Auckland"}],
  "stats": {"compile_time_ms": 0.12, "exec_time_ms": 0.45}
}
```

If some shards fail during a scatter-gather, you get partial results:
```json
{
  "columns": ["p.name"],
  "rows": [...],
  "partial": true,
  "errors": [{"shard_id": 2, "error": "shard 2 is unhealthy"}]
}
```

### 4. Write Consistency

Control how many replicas must acknowledge a write by passing the `consistency` parameter:

| Level | Behavior | Use case |
|---|---|---|
| `ONE` | Ack after primary write, async replicate | Fast writes, acceptable loss window |
| `QUORUM` | Ack after primary + 1 replica (default) | Safe default for most workloads |
| `ALL` | Ack after all replicas confirm | Maximum durability |

### 5. Monitor the Cluster

**Health check:**
```bash
curl -s localhost:8080/health | jq
# {
#   "status": "ok",          # or "degraded" if any shard is unhealthy
#   "role": "leader",        # or "follower"
#   "node_id": "node-1",
#   "shards": {"0": "healthy", "1": "healthy", "2": "healthy"}
# }
```

**Cluster status:**
```bash
curl -s localhost:8080/cluster | jq
# {
#   "is_leader": true,
#   "leader": "node1:9000",
#   "node_id": "node-1",
#   "shard_map": {"0": {"primary": "node-1", "replica": "node-2"}, ...},
#   "nodes": {"node-1": {"alive": true, ...}, ...}
# }
```

**Add a node:**
```bash
# On the leader — add node-4 to the cluster
curl -s localhost:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000",
  "grpc_addr": "node4:9001",
  "http_addr": "node4:8080"
}'
# Rebalancer automatically migrates shards to the new node
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

### Choosing shard count

Shards are fixed at cluster creation — you can't reshard later without rebuilding. Overprovision:

| Data scale | Recommended shards |
|---|---|
| < 10M nodes | 16 |
| 10M–100M | 64 |
| 100M+ | 128–256 |

Rule: your shard count is the maximum number of nodes you can ever use. 16 shards = up to 16 nodes.

## API Reference

### `POST /query`

Execute a Cypher query. The router parses the query, looks up the table's shard key from the schema registry, and routes to the appropriate shard(s).

**Request:**
```json
{"cypher": "MATCH (p:Person {name: 'Alice'}) RETURN p"}
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

Node health with per-shard status. Returns `"status": "degraded"` if any shard is unhealthy.

### `GET /cluster`

Cluster status: shard map (which node owns which shard), node list, current leader.

### `POST /join`

Join a new node to the cluster. Must be called on the leader. Triggers automatic shard rebalancing.

### `POST /internal/query`

Internal endpoint for node-to-node query forwarding. Not intended for external clients.

## Supported Cypher

Loveliness passes all Cypher through to LadybugDB — the router only classifies queries for routing, it doesn't validate syntax.

| Category | Clauses | Routing |
|---|---|---|
| **Reads** | `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `CALL`, `RETURN`, `ORDER BY`, `LIMIT` | Shard key → single shard; no key → scatter-gather |
| **Writes** | `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE` | Shard key required; routes to owning shard |
| **Schema DDL** | `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, `ALTER` | Broadcast to all shards |

## Project Structure

```
cmd/loveliness/main.go          Entry point, config, shard init, Raft, HTTP, auto-join, graceful shutdown
pkg/
  config/config.go              Configuration from environment variables
  logging/logging.go            Structured JSON logging (log/slog)
  schema/
    registry.go                 Schema registry — maps tables to shard key properties
    registry_test.go            Tests for DDL parsing and registry operations
  shard/
    store.go                    Store interface (abstracts LadybugDB)
    lbug_store.go               Production Store via go-ladybug CGo bindings
    memory_store.go             In-memory mock Store for testing
    shard.go                    Shard wrapper — semaphore concurrency control + CGo panic recovery
    shard_test.go               Concurrency, health, panic recovery tests
    manager.go                  Shard lifecycle manager — opens/closes shards based on assignments
    manager_test.go             Shard placement reconciliation tests
  router/
    parser.go                   Cypher classifier + schema-aware shard key extraction
    parser_test.go              Parsing, classification, key extraction tests
    router.go                   Query routing — single-shard, scatter-gather, broadcast
    router_test.go              Routing, timeout, partial result tests
  transport/
    client.go                   HTTP client pool for peer nodes
    handler.go                  Internal /internal/query endpoint
    transport_test.go           Cross-node forwarding tests
  replication/
    replicator.go               Write fan-out with consistency levels (ONE/QUORUM/ALL)
    replicator_test.go          Replication, timeout, consistency tests
  cluster/
    fsm.go                      Raft FSM — shard map, node membership, shard assignments
    fsm_test.go                 FSM apply, snapshot, restore tests
    cluster.go                  Raft lifecycle — bootstrap, join, failover, leadership
    rebalancer.go               Shard migration planning and execution
    rebalancer_test.go          Rebalance algorithm tests
    partitioner.go              Cross-shard traversal stats + locality suggestions
    partitioner_test.go         Partitioning tests
  api/
    api.go                      HTTP API — /query, /health, /cluster, /join + middleware
    api_test.go                 API endpoint tests
Dockerfile                      Multi-stage build with LadybugDB shared library
docker-compose.yml              3-node cluster for local development
Makefile                        build, test, run, docker, cover, lint, race
ARCHITECTURE.md                 Detailed architecture and design decisions
```

## How It Works

### Query lifecycle

1. Client sends `POST /query {"cypher": "MATCH (p:Person {name: 'Alice'}) RETURN p"}`
2. **Parser** classifies the query (read/write/schema) and extracts labels (`Person`)
3. **Schema registry** looks up Person's shard key → `name`
4. **Parser** finds the `name` property value → `"Alice"` (ignores other properties)
5. **Router** hashes: `fnv32a("Alice") % num_shards` → shard 7
6. **Shard map** lookup: shard 7's primary is `node-2`
7. If local → execute directly on the LadybugDB instance
8. If remote → forward via `POST /internal/query` to node-2
9. Return result to client

### Write lifecycle (with replication)

1. Router sends write to **primary** shard on the owning node
2. Primary executes the Cypher against LadybugDB
3. **Replicator** fans out the same Cypher to replica node(s)
4. With `QUORUM`: wait for primary + 1 replica ACK, then respond
5. With `ONE`: respond immediately, replicate async
6. With `ALL`: wait for all replicas

### Cluster membership

1. First node starts with `LOVELINESS_BOOTSTRAP=true` → becomes Raft leader
2. Additional nodes start with `LOVELINESS_PEERS=node1:9000` → auto-join via HTTP
3. Leader assigns shards round-robin: 16 shards across 3 nodes = ~5 shards each
4. Each shard gets a primary and a replica on different nodes
5. When a node joins/leaves, the **rebalancer** computes migrations and redistributes

### CGo safety

LadybugDB runs via CGo (go-ladybug bindings). Each shard has:
- A **semaphore** limiting concurrent CGo calls (prevents thread exhaustion)
- **Panic recovery** that catches CGo crashes and marks the shard unhealthy instead of crashing the process
- **Thread pool configuration** (`runtime.NumCPU() / shardCount` threads per shard) to prevent CPU oversubscription

## Development

```bash
# Run tests with race detector
make race

# Coverage report
make cover

# Lint (requires golangci-lint)
make lint

# Clean build artifacts and data
make clean
```

## License

MIT
