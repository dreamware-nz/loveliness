# Loveliness

A clustered graph database built on [LadybugDB](https://github.com/LadybugDB/ladybug) — like Elasticsearch is to Lucene, Loveliness is to LadybugDB.

> *A "loveliness" is the collective noun for a group of ladybugs.*

## What is this?

LadybugDB is a Kuzu fork — a fast, embedded, columnar graph engine with Cypher support. It's excellent on a single machine but has no clustering story. Loveliness wraps LadybugDB in a distributed layer: FNV-32a hash-based sharding, Raft consensus, cross-node query forwarding, Bloom filter shard routing, edge-cut replication, WAL-based disaster recovery, and a binary TCP transport for inter-node communication.

Every node runs a single Go binary. No external dependencies besides LadybugDB itself.

## Performance

Benchmarked on Apple M1 Pro, single node, 4 shards.

### At 15.7M nodes / 10M edges

| Query Type | P50 Latency | QPS (8 workers) |
|---|---|---|
| Point lookup (PK) | **425us** | **10,758** |
| Range filter (LIMIT 20) | **1.2ms** | 774 |
| Count all nodes | **2.1ms** | 457 |
| Count filtered (by city) | **56ms** | 15 |
| Aggregation (avg/min/max) | **57ms** | 16 |
| Group-by (10 cities) | **138ms** | 7 |
| 1-hop traversal | **673us** | 1,106 |
| 2-hop traversal | **162ms** | 5 |
| Variable-length path (1..3) | **1.8ms** | 369 |
| Mutual friends (triangle) | **1.1ms** | 838 |
| Shortest path (1..6) | **1.1ms** | 188 |
| Single write | **365us** | 2,526 |
| Merge/upsert | **5.0ms** | 190 |

### Bulk loading throughput

| Operation | Throughput | Notes |
|---|---|---|
| Node loading (COPY FROM) | **70–190K nodes/sec** | Degrades with B-tree depth at scale |
| Edge loading (2-pass) | **9.6K edges/sec** | Pass 1: ref nodes, Pass 2: edges |

### How we compare (estimated, 10–15M node scale)

| Benchmark | Loveliness | Neo4j | Memgraph | TigerGraph | Neptune | JanusGraph |
|---|---|---|---|---|---|---|
| Point lookup P50 | **425us** | 200–500us | 50–150us | 300–800us | 5–15ms | 2–10ms |
| Concurrent read QPS | **10.7K** | 10–30K | 30–80K | 20–50K | 2–5K | 1–3K |
| 1-hop traversal P50 | **673us** | 200–600us | 100–300us | 500us–1ms | 5–20ms | 2–10ms |
| 2-hop traversal P50 | **162ms** | 10–50ms | 5–20ms | 20–80ms | 50–200ms | 50–200ms |
| Single write P50 | **365us** | 1–5ms | 100–300us | 1–2ms | 5–15ms | 5–20ms |
| Bulk load (nodes/sec) | **70–190K** | 30–80K online | 50–100K | 200K–1M | 50–150K | 10–50K |

**Where we win:** Point lookups (Bloom filter routing avoids unnecessary shard hits), single writes (lightweight transaction model), 1-hop traversals (edge-cut replication keeps most hops local), operational simplicity (single binary, no JVM, no external storage).

**Where we lose:** Multi-hop fan-out (scatter-gather + serialization per hop vs native pointer chasing), concurrent throughput ceiling (CGo boundary tax on every query), query optimizer maturity (Neo4j has 15 years of cost-based optimization).

**Where we're different:** Sharded from day one (Neo4j Fabric is an afterthought), columnar storage under the hood (analytics queries are better than expected for a graph DB), horizontal scalability by adding nodes.

### Inter-node transport: TCP+MessagePack vs HTTP+JSON

Internal cluster communication uses a binary TCP transport with MessagePack serialization instead of HTTP+JSON. Micro-benchmarks on 1000-row result sets:

| Scenario | HTTP+JSON | TCP+MsgPack | Speedup |
|---|---|---|---|
| Single query (250 rows) | 551us | 269us | **2.1x** |
| Scatter-gather 4 shards | 1,339us | 658us | **2.0x** |
| 8-worker concurrent | 397us | 94us | **4.2x** |
| Marshal 1000 rows | 481us | 198us | **2.4x** |
| Unmarshal 1000 rows | 1,247us | 442us | **2.8x** |

## Architecture

```
                         ┌──────────────────────────────────────────────────┐
         HTTP Client ──► │                Loveliness Node                   │
        Bolt Client ──►  │                                                  │
                         │  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌──────────────┐  │
                         │  │ HTTP API │  │Bolt :7687│  │   Raft    │  │ TCP Transport│  │
                         │  │  /query  │  │ PackStream│  │ Consensus │  │  MsgPack     │  │
                         │  │  /ingest │  │ Neo4j SDK│  │           │  │  Pool+Framed │  │
                         │  └────┬─────┘  └────┬─────┘  └─────┬─────┘  └──────┬───────┘  │
                         │       │              │                │          │
                         │  ┌────▼──────────────▼────────────────▼───────┐  │
                         │  │              Query Router                   │  │
                         │  │  parse → Bloom filter → route/scatter      │  │
                         │  │                                             │  │
                         │  │  Optimizations:                             │  │
                         │  │   A. Aggregate merging (AVG→SUM+COUNT)     │  │
                         │  │   B. Bloom filter shard routing             │  │
                         │  │   C. Edge-cut replication                   │  │
                         │  │   D. Graph-aware partitioning               │  │
                         │  │   E. Pipeline execution                     │  │
                         │  └──┬──────────────────────────────────┬─────┘  │
                         │     │ local                      remote│        │
                         │  ┌──▼───┐  ┌──────┐          TCP+msgpack       │
                         │  │Shard │  │Shard │          to peer node      │
                         │  │  0   │  │  1   │                            │
                         │  └──────┘  └──────┘                            │
                         │                                                  │
                         │  ┌──────────────────────────────────────────┐   │
                         │  │  WAL │ Backup │ Ingest Queue │ Replicas │   │
                         │  └──────────────────────────────────────────┘   │
                         └──────────────────────────────────────────────────┘
```

**Key properties:**

- **Declared shard keys** — each node table has a PRIMARY KEY that determines shard routing via FNV-32a hashing
- **Bloom filter routing** — per-shard probabilistic index (5M keys, 1% FPR, ~6MB per shard) routes point lookups to a single shard instead of scatter-gathering
- **Edge-cut replication** — border nodes are replicated with full properties across shards so 1-hop traversals resolve locally
- **Aggregate pushdown** — AVG is rewritten to SUM+COUNT per shard, merged at the router; ORDER BY, LIMIT, and DISTINCT applied post-merge
- **Pipeline execution** — multi-hop traversals overlap across shards
- **WAL + backup/restore** — per-shard write-ahead log with backup to local disk or S3
- **Log-backed ingest queue** — async bulk loading with durable job state
- **TCP+MsgPack transport** — binary framed protocol for inter-node queries, 2-4x faster than HTTP+JSON

## Neo4j Bolt Protocol Compatibility

Loveliness speaks the **Neo4j Bolt protocol** (v4.x) on port 7687. Existing Neo4j drivers work with minimal changes — just change the connection URL.

Verified end-to-end with the official `neo4j` Python driver (v6.1.0):

```
=== Neo4j Python Driver → Loveliness Bolt Test ===

1. Testing connectivity...          PASS
2. Creating schema (IF NOT EXISTS)  PASS
3. Inserting nodes with $params     PASS (5 nodes)
4. Inserting edges                  PASS (4 edges)
5. Querying all people              PASS (5 records)
6. Querying with parameter          PASS (1 record)
7. Traversal (who does Alice know?) PASS (2 connections)
8. Aggregation (AVG)                PASS
9. Explicit transaction             PASS
10. Final count                     PASS

ALL TESTS PASSED - Neo4j driver talks to Loveliness!
```

**Python:**
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

with driver.session() as session:
    # Schema — CREATE NODE TABLE IF NOT EXISTS works
    session.run("CREATE NODE TABLE IF NOT EXISTS Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")

    # Write with parameter binding
    session.run("CREATE (p:Person {name: $name, age: $age, city: $city})",
                name="Alice", age=30, city="Auckland")

    # Read
    result = session.run("MATCH (p:Person {name: $name}) RETURN p.name, p.age", name="Alice")
    for record in result:
        print(record["p.name"], record["p.age"])

    # Traversal
    result = session.run("MATCH (a:Person {name: $name})-[:KNOWS]->(b) RETURN b.name", name="Alice")
    for record in result:
        print("Alice knows", record["b.name"])
```

**JavaScript:**
```javascript
const neo4j = require('neo4j-driver');
const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();
const result = await session.run('MATCH (p:Person) RETURN p.name LIMIT 10');
result.records.forEach(r => console.log(r.get('p.name')));
```

**Go:**
```go
driver, _ := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.NoAuth())
session := driver.NewSession(ctx, neo4j.SessionConfig{})
result, _ := session.Run(ctx, "MATCH (p:Person {name: $name}) RETURN p", map[string]any{"name": "Alice"})
```

**What works:**
- Schema DDL (`CREATE NODE TABLE`, `CREATE REL TABLE`, with `IF NOT EXISTS`)
- RUN + PULL with batched streaming (including `PULL {n: 100}` for pagination)
- Parameter binding (`$name`, `$age`) — interpolated into Cypher automatically
- Explicit transactions (BEGIN/COMMIT/ROLLBACK)
- RESET for connection recovery after errors
- ROUTE message for driver routing table discovery
- GOODBYE for clean disconnect
- 10+ concurrent driver connections

**What doesn't (yet):**
- Bolt v5.x features (LOGOFF, TELEMETRY)
- Node/Relationship graph type wrapping (results come back as flat rows, not graph objects)
- Multi-database selection
- Bookmark-based causal consistency

**Configuration:**
- Default port: `:7687` (standard Neo4j Bolt port)
- Set `LOVELINESS_BOLT_ADDR=:7688` to change, or empty string to disable

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
# → listening on :8080 (HTTP), :7687 (Bolt), :9001 (TCP transport)
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

### 3. Bulk Loading

For large datasets, use the bulk loading endpoints or the async ingest queue.

**Synchronous bulk load:**
```bash
# Bulk load nodes from CSV
curl -s localhost:8080/bulk/nodes \
  -H "Content-Type: text/csv" \
  -H "X-Table: Person" \
  --data-binary @persons.csv

# Bulk load edges (two-pass for cross-shard performance)
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

**Async ingest queue (for large loads):**
```bash
# Submit a node ingest job — returns immediately with job ID
curl -s -X POST localhost:8080/ingest/nodes \
  -H "X-Table: Person" \
  --data-binary @persons.csv
# → {"job_id": "20260327-120000-abc123", "status": "pending"}

# Poll job status
curl -s localhost:8080/ingest/jobs/20260327-120000-abc123
# → {"status": "completed", "loaded": 5000000, ...}

# List all jobs
curl -s localhost:8080/ingest/jobs
```

The ingest queue spools the CSV to disk, returns 202 Accepted, and processes jobs sequentially in the background. Jobs survive server restarts.

### 4. Query Data

**Point lookup (single shard):**
```bash
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person {name: '\''Alice'\''}) RETURN p.name, p.age, p.city"
}'
```

**Scan (scatter-gather):**
```bash
curl -s localhost:8080/query -d '{
  "cypher": "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 10"
}'
```

**Traversal:**
```bash
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

### 5. Disaster Recovery

**Backup and restore:**
```bash
# Stream a backup archive
curl -s localhost:8080/backup -o backup.tar.gz

# Restore from archive
curl -s -X POST localhost:8080/restore --data-binary @backup.tar.gz

# WAL status
curl -s localhost:8080/wal/status
```

**CSV export:**
```bash
# Export a node table
curl -s localhost:8080/export/Person

# Export edges
curl -s localhost:8080/export/Person/edges/KNOWS
```

**S3 scheduled backups** are configured via environment variables (see Configuration).

### 6. Write Consistency

Control how many replicas must acknowledge a write by passing the `consistency` parameter:

| Level | Behavior | Use case |
|---|---|---|
| `ONE` | Ack after primary write, async replicate | Fast writes, acceptable loss window |
| `QUORUM` | Ack after primary + 1 replica (default) | Safe default for most workloads |
| `ALL` | Ack after all replicas confirm | Maximum durability |

### 7. Monitor the Cluster

```bash
# Health check
curl -s localhost:8080/health | jq

# Cluster status
curl -s localhost:8080/cluster | jq

# Add a node
curl -s localhost:8080/join -d '{
  "node_id": "node-4",
  "raft_addr": "node4:9000",
  "grpc_addr": "node4:9001",
  "http_addr": "node4:8080"
}'
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `LOVELINESS_NODE_ID` | `node-1` | Unique node identifier |
| `LOVELINESS_BIND_ADDR` | `:8080` | HTTP API listen address |
| `LOVELINESS_RAFT_ADDR` | `:9000` | Raft consensus address |
| `LOVELINESS_GRPC_ADDR` | `:9001` | TCP transport address (msgpack) |
| `LOVELINESS_DATA_DIR` | `./data` | Base directory for shard data and Raft state |
| `LOVELINESS_SHARD_COUNT` | `3` | Total number of shards |
| `LOVELINESS_BOOTSTRAP` | `false` | Bootstrap a new cluster (first node only) |
| `LOVELINESS_PEERS` | *(empty)* | Comma-separated list of peer Raft addresses |
| `LOVELINESS_MAX_CONCURRENT_QUERIES` | `16` | Max concurrent CGo calls per shard |
| `LOVELINESS_QUERY_TIMEOUT_MS` | `30000` | Per-shard query timeout in milliseconds |
| `LOVELINESS_S3_BUCKET` | *(empty)* | S3 bucket for backup storage |
| `LOVELINESS_S3_REGION` | *(empty)* | AWS region for S3 |
| `LOVELINESS_S3_PREFIX` | *(empty)* | Key prefix within the S3 bucket |
| `LOVELINESS_S3_ENDPOINT` | *(empty)* | Custom S3 endpoint (MinIO, R2, etc.) |
| `LOVELINESS_BACKUP_INTERVAL_MIN` | `0` | Minutes between scheduled backups (0 = disabled) |
| `LOVELINESS_BACKUP_RETENTION` | `3` | Number of backups to retain |
| `LOVELINESS_BACKUP_DIR` | *(empty)* | Local directory for backups (when S3 is not configured) |
| `LOVELINESS_BOLT_ADDR` | `:7687` | Neo4j Bolt protocol listen address (empty to disable) |

### Choosing shard count

Shards are fixed at cluster creation — you can't reshard later without rebuilding. Overprovision:

| Data scale | Recommended shards |
|---|---|
| < 10M nodes | 16 |
| 10M–100M | 64 |
| 100M+ | 128–256 |

Rule: your shard count is the maximum number of nodes you can ever use. 16 shards = up to 16 nodes.

## Query Optimization Phases

Loveliness applies five optimization phases to distributed queries:

### Phase A: Aggregate Merging
Rewrites `AVG(x)` to `SUM(x)` + `COUNT(x)` per shard, then merges at the router. Also handles post-merge `ORDER BY`, `LIMIT`, and `DISTINCT`.

### Phase B: Bloom Filter Routing
Per-shard probabilistic index (5M keys, 1% false positive rate, ~6MB each). Point lookups check the Bloom filter first — if only one shard reports "maybe", the query skips the other shards entirely.

### Phase C: Edge-Cut Replication
When an edge crosses shard boundaries, the target node is replicated (with full properties) to the source shard. This means 1-hop traversals almost always resolve locally without cross-shard communication.

**Why doesn't this cascade?** If Alice (shard 0) knows Bob (shard 1), Bob's properties get copied to shard 0. But Bob's *edges* are not copied — only his properties. The ref copy is a read-only property stub, not a full graph participant.

```
Shard 0 (owns Alice)              Shard 1 (owns Bob)              Shard 2 (owns Charlie)
┌──────────────────────┐          ┌──────────────────────┐        ┌──────────────────────┐
│ Alice (primary)      │          │ Bob (primary)        │        │ Charlie (primary)    │
│   edges: → Bob       │          │   edges: → Charlie   │        │   edges: → Dave      │
│                      │          │                      │        │                      │
│ Bob (ref copy)       │          │ Charlie (ref copy)   │        │                      │
│   properties: ✓      │          │   properties: ✓      │        │                      │
│   edges: ✗ (none)    │          │   edges: ✗ (none)    │        │                      │
└──────────────────────┘          └──────────────────────┘        └──────────────────────┘
```

This means:
- **1-hop** (Alice→Bob): Resolves locally on shard 0. Bob's ref copy has his properties. **No cross-shard call.**
- **2-hop** (Alice→Bob→Charlie): Shard 0 resolves Alice→Bob locally, but Bob's ref copy has no edges. To find Bob→Charlie, the router must go to shard 1 where Bob's primary lives. **One cross-shard call.**
- **N-hop**: Each hop beyond the first requires a cross-shard call. Replication does not cascade — shard 0 never gets Charlie or Dave.

This is a deliberate trade-off: ref copies are cheap (one extra MERGE per border node) and make the most common query pattern (1-hop neighborhood) fast, without the storage explosion that full-depth replication would cause. At 15M nodes with 4 shards, ~75% of edges cross shard boundaries, which would mean replicating the entire graph to every shard if edges were included — defeating the purpose of sharding.

### Phase D: Graph-Aware Partitioning
Label propagation community detection identifies tightly-connected subgraphs. Used to suggest shard migrations that reduce cross-shard edge cuts.

### Phase E: Pipeline Execution
Multi-hop traversals overlap across shards — while shard 0 processes hop 2, shard 1 processes hop 1. Reduces serial round-trip latency.

## Project Structure

```
cmd/
  loveliness/main.go              Entry point, config, shard init, Raft, HTTP, TCP, ingest, shutdown
  benchmark/main.go               Benchmark suite (point lookups, traversals, writes, aggregations)
  generate/main.go                Bulk data generator with two-pass edge loading
pkg/
  config/config.go                Configuration from environment variables
  logging/logging.go              Structured JSON logging (log/slog)
  schema/
    registry.go                   Schema registry — maps tables to shard key properties
  shard/
    store.go                      Store interface (abstracts LadybugDB)
    lbug_store.go                 Production Store via go-ladybug CGo bindings
    memory_store.go               In-memory mock Store for testing
    shard.go                      Shard wrapper — semaphore concurrency + CGo panic recovery
    manager.go                    Shard lifecycle manager — opens/closes by assignment
  router/
    parser.go                     Cypher classifier + schema-aware shard key extraction
    router.go                     Query routing — single-shard, scatter-gather, broadcast
    aggregate.go                  Phase A: distributed aggregate merging
    bloom.go                      Phase B: per-shard Bloom filter index
    edgecut.go                    Phase C: edge-cut border node replication
    partition.go                  Phase D: label propagation community detection
    pipeline.go                   Phase E: overlapping multi-hop pipeline execution
  transport/
    codec.go                      MsgPack wire format: length-prefixed frames with type byte
    tcp.go                        TCP server: persistent connections, buffered I/O
    pool.go                       TCP connection pool: lazy dial, auto-eviction
    client.go                     Unified client: TCP+msgpack preferred, HTTP+JSON fallback
    handler.go                    HTTP /internal/query endpoint (legacy fallback)
    shard_set.go                  Lightweight shard slice adapter
  replication/
    wal.go                        Per-shard write-ahead log with fsync and crash recovery
    catchup.go                    Replica WAL catch-up with configurable batch size
    readpref.go                   Read preference routing (PRIMARY/PREFER_REPLICA/NEAREST)
  backup/
    backup.go                     Backup manager: gzip'd tar of shard data + manifest
    export.go                     CSV export with scatter-gather dedup
    s3store.go                    S3 backup store (AWS, MinIO, R2)
    localstore.go                 Local filesystem backup store
    scheduler.go                  Scheduled backup with retention pruning
    store.go                      Backup store interface
  ingest/
    queue.go                      Log-backed ingest queue with durable job state
    worker.go                     Sequential background worker for async bulk loading
    shard_adapter.go              Bridge between ingest worker and shard/router
  bolt/
    packstream.go                 PackStream binary serialization (Neo4j wire format)
    messages.go                   Bolt message types (HELLO, RUN, PULL, SUCCESS, RECORD, etc.)
    chunking.go                   Bolt chunked transfer framing
    server.go                     Bolt protocol state machine and TCP server
    graph_types.go                Node/Relationship packing helpers
    router_adapter.go             Bridges Bolt QueryExecutor to Loveliness Router
  cluster/
    fsm.go                        Raft FSM — shard map, node membership, assignments
    cluster.go                    Raft lifecycle — bootstrap, join, failover, leadership
    rebalancer.go                 Shard migration planning and execution
    partitioner.go                Cross-shard traversal stats + locality suggestions
    transfer.go                   Shard data transfer (export/import as Cypher)
  api/
    api.go                        HTTP API — /query, /health, /cluster, /join
    bulk.go                       Bulk loading endpoints with streaming CSV parse
    bulk_stream.go                Streaming bulk load variant
    ingest.go                     Async ingest queue endpoints
    dr.go                         DR endpoints — backup, restore, export, WAL status
```

## Supported Cypher

Loveliness passes all Cypher through to LadybugDB — the router only classifies queries for routing, it doesn't validate syntax.

| Category | Clauses | Routing |
|---|---|---|
| **Reads** | `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `CALL`, `RETURN`, `ORDER BY`, `LIMIT` | Shard key → single shard; no key → scatter-gather |
| **Writes** | `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE` | Shard key required; routes to owning shard |
| **Schema DDL** | `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, `ALTER` | Broadcast to all shards |
| **Bulk** | `COPY FROM` | Via `/bulk/nodes` or `/bulk/edges` endpoints |

## How It Works

### Query lifecycle

1. Client sends `POST /query {"cypher": "MATCH (p:Person {name: 'Alice'}) RETURN p"}`
2. **Parser** classifies the query (read/write/schema) and extracts labels (`Person`)
3. **Schema registry** looks up Person's shard key → `name`
4. **Bloom filter** checks which shards might have `"Alice"` → shard 2
5. **Router** sends query to shard 2 only (instead of all 4)
6. If shard is local → execute directly via CGo
7. If remote → forward via TCP+msgpack to the owning node
8. Return result to client

### Write lifecycle

1. Router sends write to **primary** shard on the owning node
2. **WAL** appends the Cypher before execution (for crash recovery and replica catch-up)
3. Primary executes the Cypher against LadybugDB
4. **Replicator** fans out to replica node(s) per consistency level
5. Response returned to client

### Edge-cut replication

When Alice (shard 0) creates an edge to Bob (shard 2):
1. A **reference node** for Bob is created on shard 0 with full properties
2. The edge is stored on shard 0 (edges live with their source)
3. `MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)` resolves entirely on shard 0 — no cross-shard hop needed

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

# Run benchmarks at scale
go run ./cmd/generate -nodes 5000000 -edge-ratio 1.0
go run ./cmd/benchmark -skip-seed -nodes 5000000 -edges 5000000

# Clean build artifacts and data
make clean
```

## License

MIT
