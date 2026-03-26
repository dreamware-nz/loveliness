# Loveliness Architecture

## System Overview

Loveliness is a distributed graph database built on LadybugDB (a Kuzu fork). Each node in the cluster runs a single Go binary containing: an HTTP API, a Cypher query router, a schema registry, a shard manager, a Raft consensus participant, a transport client for cross-node communication, and a write replicator.

```
                    ┌──────────────────────────────────────────────────┐
    Client ───────► │                Loveliness Node                   │
                    │                                                  │
                    │  ┌─────────┐  ┌─────────┐  ┌────────────────┐   │
                    │  │HTTP API │  │  Raft   │  │   Transport    │   │
                    │  │ :8080   │  │  :9000  │  │   Client Pool  │   │
                    │  └───┬─────┘  └────┬────┘  └───────┬────────┘   │
                    │      │             │               │            │
                    │  ┌───▼─────────────▼───────────────▼─────────┐  │
                    │  │              Query Router                  │  │
                    │  │                                            │  │
                    │  │  1. Classify (read/write/schema)           │  │
                    │  │  2. Extract labels → schema registry       │  │
                    │  │  3. Look up shard key property             │  │
                    │  │  4. Hash shard key value → shard ID        │  │
                    │  │  5. Check shard map → local or remote?     │  │
                    │  │  6. Execute locally or forward to peer     │  │
                    │  └──┬─────────────────────────────────┬──────┘  │
                    │     │ local                     remote│         │
                    │  ┌──▼───┐ ┌──────┐        POST /internal/query │
                    │  │Shard │ │Shard │        ──────────► peer node │
                    │  │  0   │ │  5   │                             │
                    │  └──────┘ └──────┘                             │
                    │  (only shards assigned to this node)           │
                    └──────────────────────────────────────────────────┘
```

## Core Design Decisions

### Shard keys are declared, not inferred

Every node table has a `PRIMARY KEY` that becomes its shard key. When you run `CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))`, the schema registry records that `Person` is sharded on `name`. All subsequent queries involving `Person` nodes use only the `name` property for routing — other properties like `city` or `age` are ignored for shard resolution.

This prevents the critical bug where two queries about the same node route to different shards because they filter on different properties.

**Implementation:** `pkg/schema/registry.go` holds a `map[string]TableSchema` (table name → shard key property). The parser in `pkg/router/parser.go` calls `ParseWithSchema()` which extracts labels from the query, looks up each label's shard key, then finds that specific property's value in inline properties or WHERE clauses.

### Shards are placed, not replicated everywhere

Each shard has a **primary** (handles reads and writes) and a **replica** (receives replicated writes, can be promoted on failure). A node only opens the LadybugDB instances for shards assigned to it.

**Implementation:** `pkg/shard/manager.go` watches the Raft FSM's shard map. When assignments change (node join, rebalance, failover), it opens newly assigned shards and closes removed ones. `pkg/cluster/fsm.go` stores `ShardAssignment{Primary, Replica}` per shard ID.

### Queries cross node boundaries transparently

When the router determines a query's target shard is on another node, it forwards the request via internal HTTP (`POST /internal/query`). The client sees the same response regardless of which node they hit.

**Implementation:** `pkg/transport/client.go` maintains an HTTP client pool per peer node with keepalive connections. `pkg/transport/handler.go` serves the internal endpoint, executing the query on the local shard and returning the result.

### Write replication is Cypher-level replay

After the primary shard executes a write, the replicator sends the same Cypher statement to the replica node(s). This is simpler than WAL-level replication and correct because LadybugDB is deterministic for the same input on the same schema.

**Implementation:** `pkg/replication/replicator.go` supports three consistency levels:
- `ONE` — ack on primary write, fire-and-forget async to replicas
- `QUORUM` — ack after primary + 1 replica (default)
- `ALL` — ack after all replicas confirm

### Rebalancing is leader-driven

When nodes join or leave, the leader computes a migration plan:
1. Fix primaries on dead nodes (promote replica or assign to least-loaded node)
2. Rebalance overloaded nodes (move shards from over-capacity to under-capacity)
3. Fix replicas (ensure every shard has a replica on a different node than its primary)

**Implementation:** `pkg/cluster/rebalancer.go` produces a `[]Move` plan (pure function, testable), then applies each move via Raft FSM updates.

### Locality optimization is statistics-driven

`pkg/cluster/partitioner.go` tracks cross-shard traversal counts. When shard A frequently traverses edges that land on shard B, the partitioner suggests colocating them on the same node. This is advisory — the rebalancer decides whether to act on suggestions.

## Data Flow

### Read query (point lookup)

```
Client → POST /query {"cypher": "MATCH (p:Person {name: 'Alice'}) RETURN p"}
  → Parser: classify=READ, label=Person
  → Schema: Person.shard_key = "name"
  → Parser: extract name='Alice' from inline properties
  → Router: fnv32a("Alice") % 16 = shard 7
  → Shard map: shard 7 primary = node-2
  → If this IS node-2: execute on local LadybugDB shard 7
  → If this is NOT node-2: transport.QueryRemote("node-2", 7, cypher)
  → Return result
```

### Read query (scatter-gather)

```
Client → POST /query {"cypher": "MATCH (p:Person) RETURN p.name"}
  → Parser: classify=READ, label=Person, no shard key value
  → Router: fan out to ALL shards in parallel
  → Each shard: execute locally or forward to owning node
  → Merge all rows, report partial if any shard timed out
  → Return merged result
```

### Write query (with replication)

```
Client → POST /query {"cypher": "CREATE (p:Person {name: 'Alice', age: 30})"}
  → Parser: classify=WRITE, label=Person
  → Schema: Person.shard_key = "name"
  → Parser: extract name='Alice'
  → Router: shard 7, primary = node-2
  → Execute on primary shard 7
  → Replicator: send same Cypher to shard 7's replica node
  → QUORUM: wait for replica ACK
  → Return result
```

### Schema DDL (broadcast)

```
Client → POST /query {"cypher": "CREATE NODE TABLE Company(id STRING, PRIMARY KEY(id))"}
  → Parser: classify=SCHEMA
  → Schema registry: register Company.shard_key = "id"
  → Router: broadcast to ALL shards in parallel
  → All shards execute the DDL
  → Return success (or BROADCAST_PARTIAL if any fail)
```

## Component Map

| Package | Responsibility | Key types |
|---|---|---|
| `pkg/schema` | Table → shard key mapping, DDL parsing | `Registry`, `TableSchema`, `ParseCreateNodeTable()` |
| `pkg/shard` | LadybugDB lifecycle, concurrency control | `Store` (interface), `LbugStore`, `MemoryStore`, `Shard`, `Manager` |
| `pkg/router` | Cypher classification, shard key extraction, routing | `Parser`, `Router`, `ParsedQuery`, `QueryError` |
| `pkg/transport` | Node-to-node HTTP communication | `Client` (pool), `Handler` (/internal/query) |
| `pkg/replication` | Write fan-out with consistency levels | `Replicator`, `Consistency` (ONE/QUORUM/ALL) |
| `pkg/cluster` | Raft FSM, membership, rebalancing, locality | `FSM`, `Cluster`, `Rebalancer`, `Partitioner`, `CrossShardStats` |
| `pkg/api` | HTTP API, middleware, structured errors | `Server`, request ID middleware, error codes |
| `pkg/config` | Environment variable configuration | `Config`, `FromEnv()`, `Validate()` |
| `pkg/logging` | Structured JSON logging | `Setup()` (configures slog) |

## Raft State Machine

The FSM manages cluster-wide state via four command types:

| Command | Payload | Effect |
|---|---|---|
| `CmdAssignShard` | `{shard_id, primary, replica}` | Set shard ownership |
| `CmdJoinNode` | `{node_info}` | Register a node with its addresses |
| `CmdRemoveNode` | `{node_id}` | Mark a node as dead |
| `CmdPromoteReplica` | `{shard_id, new_primary}` | Promote replica to primary, clear replica slot |

State is snapshotted as JSON and can be restored on any node.

## CGo Safety Model

LadybugDB runs via CGo through go-ladybug bindings. Three layers of protection:

1. **Semaphore per shard** — bounded channel limits concurrent CGo calls, preventing goroutine/thread exhaustion
2. **Panic recovery** — `defer recover()` catches CGo crashes, marks the shard unhealthy, and returns an error instead of crashing the process
3. **Thread pool sizing** — `runtime.NumCPU() / shardCount` threads per LadybugDB instance prevents CPU oversubscription when multiple shards share a node

## Future Work

- **Intent log for offline replicas** — queue writes when a replica is temporarily down, replay on reconnect
- **Shard transfer protocol** — stream LadybugDB directories between nodes during rebalancing (currently rebalancer updates assignments but doesn't transfer data)
- **gRPC transport** — replace internal HTTP with gRPC for lower-latency cross-node communication
- **Background locality optimization** — periodic job on leader that acts on partitioner suggestions
- **Read replicas** — route read-only queries to replicas to offload primaries
