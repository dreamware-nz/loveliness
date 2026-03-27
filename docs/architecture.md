# Architecture

## Overview

```mermaid
graph TB
    subgraph Client Layer
        HTTP[HTTP Client :8080]
        Bolt[Bolt Client :7687]
    end

    subgraph Node["Loveliness Node"]
        API[HTTP API<br/>/query /ingest /bulk]
        BoltSrv[Bolt Server<br/>PackStream v4.x]
        Router[Query Router<br/>parse → Bloom → route/scatter]
        Raft[Raft Consensus]
        TCP[TCP Transport<br/>MsgPack + Pool]

        subgraph Optimizations
            A[A. Aggregate Merging]
            B[B. Bloom Filter Routing]
            C[C. Edge-Cut Replication]
            D[D. Graph-Aware Partitioning]
            E[E. Pipeline Execution]
        end

        subgraph Storage
            S0[Shard 0<br/>LadybugDB]
            S1[Shard 1<br/>LadybugDB]
            S2[Shard N<br/>LadybugDB]
        end

        subgraph DR["Disaster Recovery"]
            WAL[WAL]
            Backup[Backup]
            Ingest[Ingest Queue]
        end
    end

    HTTP --> API
    Bolt --> BoltSrv
    API --> Router
    BoltSrv --> Router
    Router --> Optimizations
    Optimizations --> S0
    Optimizations --> S1
    Optimizations --> S2
    Router -- remote shards --> TCP
    TCP -- peer nodes --> Raft
```

## Key Properties

- **Declared shard keys** — each node table has a PRIMARY KEY that determines shard routing via FNV-32a hashing
- **Bloom filter routing** — per-shard probabilistic index (5M keys, 1% FPR, ~6MB per shard) routes point lookups to a single shard instead of scatter-gathering
- **Edge-cut replication** — border nodes are replicated with full properties across shards so 1-hop traversals resolve locally
- **Aggregate pushdown** — AVG is rewritten to SUM+COUNT per shard, merged at the router; ORDER BY, LIMIT, and DISTINCT applied post-merge
- **Pipeline execution** — multi-hop traversals overlap across shards
- **WAL + backup/restore** — per-shard write-ahead log with backup to local disk or S3
- **Log-backed ingest queue** — async bulk loading with durable job state
- **TCP+MsgPack transport** — binary framed protocol for inter-node queries, 2-4x faster than HTTP+JSON

## Query Lifecycle

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API / Bolt
    participant R as Router
    participant B as Bloom Filter
    participant S as Shard

    C->>A: MATCH (p:Person {name: 'Alice'}) RETURN p
    A->>R: classify query (read, label=Person)
    R->>R: schema lookup → shard key = name
    R->>B: which shards have "Alice"?
    B-->>R: shard 2 only
    R->>S: execute on shard 2 (CGo)
    S-->>R: rows
    R-->>A: result
    A-->>C: JSON / PackStream response
```

## Write Lifecycle

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Router
    participant W as WAL
    participant S as Primary Shard
    participant Rep as Replica

    C->>R: CREATE (p:Person {name: 'Alice', ...})
    R->>R: resolve shard for "Alice"
    R->>W: append Cypher to WAL (fsync)
    W-->>R: seq=42
    R->>S: execute Cypher via CGo
    S-->>R: ok
    R->>Rep: async replicate (per consistency level)
    R-->>C: success
```

## Edge-Cut Replication

When an edge crosses shard boundaries, the target node is replicated (with full properties) to the source shard. This means 1-hop traversals resolve locally.

**Why doesn't this cascade?** Ref copies are property stubs — they have the node's data but not its edges.

```mermaid
graph LR
    subgraph Shard 0
        A[Alice<br/>primary]
        B_ref[Bob<br/>ref copy<br/>props ✓ edges ✗]
        A -->|KNOWS| B_ref
    end

    subgraph Shard 1
        B[Bob<br/>primary]
        C_ref[Charlie<br/>ref copy<br/>props ✓ edges ✗]
        B -->|KNOWS| C_ref
    end

    subgraph Shard 2
        C[Charlie<br/>primary]
        C -->|KNOWS| D[Dave]
    end
```

This means:
- **1-hop** (Alice→Bob): Resolves locally on shard 0. Bob's ref copy has his properties. **No cross-shard call.**
- **2-hop** (Alice→Bob→Charlie): Shard 0 resolves Alice→Bob locally, but Bob's ref copy has no edges. To find Bob→Charlie, the router must go to shard 1. **One cross-shard call.**
- **N-hop**: Each hop beyond the first requires a cross-shard call. Replication does not cascade.

This is a deliberate trade-off: ref copies are cheap (one extra MERGE per border node) and make the most common query pattern (1-hop neighborhood) fast, without the storage explosion that full-depth replication would cause. At 15M nodes with 4 shards, ~75% of edges cross shard boundaries, which would mean replicating the entire graph to every shard if edges were included.

## Query Optimization Phases

### Phase A: Aggregate Merging
Rewrites `AVG(x)` to `SUM(x)` + `COUNT(x)` per shard, then merges at the router. Also handles post-merge `ORDER BY`, `LIMIT`, and `DISTINCT`.

### Phase B: Bloom Filter Routing
Per-shard probabilistic index (5M keys, 1% false positive rate, ~6MB each). Point lookups check the Bloom filter first — if only one shard reports "maybe", the query skips the other shards entirely.

### Phase C: Edge-Cut Replication
See [above](#edge-cut-replication).

### Phase D: Graph-Aware Partitioning
Label propagation community detection identifies tightly-connected subgraphs. Used to suggest shard migrations that reduce cross-shard edge cuts.

### Phase E: Pipeline Execution
Multi-hop traversals overlap across shards — while shard 0 processes hop 2, shard 1 processes hop 1. Reduces serial round-trip latency.

## CGo Safety

LadybugDB runs via CGo (go-ladybug bindings). Each shard has:
- A **semaphore** limiting concurrent CGo calls (prevents thread exhaustion)
- **Panic recovery** that catches CGo crashes and marks the shard unhealthy instead of crashing the process
- **Thread pool configuration** (`runtime.NumCPU() / shardCount` threads per shard) to prevent CPU oversubscription

## Supported Cypher

Loveliness passes all Cypher through to LadybugDB — the router only classifies queries for routing, it doesn't validate syntax.

| Category | Clauses | Routing |
|---|---|---|
| **Reads** | `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `CALL`, `RETURN`, `ORDER BY`, `LIMIT` | Shard key → single shard; no key → scatter-gather |
| **Writes** | `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE` | Shard key required; routes to owning shard |
| **Schema DDL** | `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, `ALTER` | Broadcast to all shards |
| **Bulk** | `COPY FROM` | Via `/bulk/nodes` or `/bulk/edges` endpoints |
