# Loveliness Architecture — Full Distributed System

## Overview

Six phases to take Loveliness from PoC to a correct distributed graph database.

## Phase 1: Declared Shard Keys

**Problem:** Router grabs the first string literal as shard key. Two queries about the same node route to different shards if they filter on different properties.

**Solution:** Schema registry maps each node table to its shard key property. The shard key is extracted from `CREATE NODE TABLE ... PRIMARY KEY(prop)` automatically.

**Implementation:**
- `pkg/schema/registry.go` — thread-safe registry: `table name → shard key property`
- Registry state replicated via Raft FSM (new command: `CmdRegisterTable`)
- Parser intercepts `CREATE NODE TABLE` to extract PRIMARY KEY → registers in schema
- Parser enhanced: given a query like `MATCH (p:Person {city: 'Auckland'})`, look up Person's shard key (`name`), and only use the `name` property value for routing — ignore `city`
- If the shard key property isn't in the query → scatter-gather (reads) or reject (writes)

**Files:**
```
pkg/schema/registry.go       Schema registry (table → shard key)
pkg/schema/registry_test.go  Tests
pkg/router/parser.go          Enhanced to use schema registry
pkg/cluster/fsm.go            New CmdRegisterTable command
```

## Phase 2: Shard Placement

**Problem:** Every node opens all shards. This is full replication, not distributed storage.

**Solution:** Each shard is assigned to specific nodes (primary + replica). Nodes only open their assigned shards.

**Implementation:**
- `pkg/shard/manager.go` — watches FSM shard map, opens/closes local shards as assignments change
- On bootstrap, leader assigns shards round-robin: 16 shards across N nodes
- ShardManager exposes `GetLocalShard(id) → *Shard` (returns nil if not local)
- Router checks shard placement: local → execute directly; remote → forward to owner
- Config change: default shard count → 16 (from 3)

**Files:**
```
pkg/shard/manager.go          Shard lifecycle manager
pkg/shard/manager_test.go     Tests
cmd/loveliness/main.go        Use ShardManager instead of manual shard init
```

## Phase 3: Inter-Node Query Forwarding

**Problem:** Router can only query local shards. If the target shard is on another node, the query fails.

**Solution:** Internal HTTP API for node-to-node query forwarding. Router checks shard placement, forwards to the owning node if remote.

**Implementation:**
- `pkg/transport/client.go` — HTTP client pool for peer nodes; `QueryRemote(nodeAddr, shardID, cypher) → Result`
- `pkg/transport/handler.go` — internal endpoint `POST /internal/query` accepts `{shard_id, cypher}`, executes on local shard
- Router enhanced: `Execute()` checks shard map → if local, query directly; if remote, forward via transport client
- Connection pool with keepalive to each peer node
- Internal endpoints protected: only accept from cluster peers (simple shared secret or peer IP allowlist)

**Files:**
```
pkg/transport/client.go        Peer HTTP client with connection pooling
pkg/transport/handler.go       Internal query handler
pkg/transport/client_test.go   Tests
pkg/router/router.go           Enhanced with remote forwarding
pkg/api/api.go                 Register internal endpoints
```

## Phase 4: Write Replication

**Problem:** Writes only go to the primary shard. If the primary dies, data is lost.

**Solution:** After writing to primary, replicate to replica shard. Configurable consistency: ONE, QUORUM, ALL.

**Implementation:**
- `pkg/replication/replicator.go` — handles write fan-out to replicas
- Write path: `client → router → primary shard (write) → replicator → replica shard (write) → ack`
- Consistency levels in query request:
  - `ONE` — ack after primary write (fast, small loss window)
  - `QUORUM` — ack after primary + 1 replica (safe, +1 RTT)
  - `ALL` — ack after all replicas (safest, slowest)
- Default consistency: `QUORUM`
- Replication is Cypher-level replay: same query sent to replica's node via transport
- Write-ahead intent log: if replica is temporarily down, queue writes and replay on reconnect
- API change: `POST /query` accepts optional `"consistency": "quorum"` field

**Files:**
```
pkg/replication/replicator.go       Write fan-out and consistency enforcement
pkg/replication/replicator_test.go  Tests
pkg/replication/intent_log.go       WAL for queued replica writes
pkg/router/router.go                Integrate replicator into write path
pkg/api/api.go                      Accept consistency parameter
```

## Phase 5: Shard Rebalancing

**Problem:** When nodes join or leave, shards don't move. Manual intervention required.

**Solution:** Leader automatically rebalances shards when cluster membership changes.

**Implementation:**
- `pkg/cluster/rebalancer.go` — runs on leader, triggered by node join/leave events
- Algorithm: compute ideal shard distribution (equal shards per node), generate migration plan
- Migration: snapshot source shard → transfer to destination node → destination opens shard → update FSM → source closes shard
- Shard transfer via HTTP: `POST /internal/shard-transfer` streams the LadybugDB directory as tar
- Rate limiting: migrate one shard at a time to avoid overwhelming the cluster
- During migration, the source shard remains primary (reads/writes continue); cutover is atomic via FSM update

**Files:**
```
pkg/cluster/rebalancer.go          Rebalance algorithm and orchestration
pkg/cluster/rebalancer_test.go     Tests
pkg/transport/handler.go           Add shard transfer endpoint
pkg/shard/manager.go               Accept incoming shard transfers
```

## Phase 6: Locality-Aware Sharding

**Problem:** Hash-based sharding ignores graph topology. Densely connected subgraphs are split across shards, causing expensive cross-shard traversals.

**Solution:** Track cross-shard edge statistics. Periodically re-partition to colocate connected nodes.

**Implementation:**
- `pkg/shard/stats.go` — per-shard cross-edge counter: tracks how many traversal results pointed to a different shard
- `pkg/cluster/partitioner.go` — simplified label propagation algorithm:
  1. Each node starts with its shard ID as label
  2. Each node adopts the most frequent label among its neighbors
  3. Converge → natural communities form
  4. Assign communities to shards, migrate outliers
- Runs as background job on leader, hourly or configurable
- Only migrates nodes that would reduce cross-shard edges by > threshold
- Integrated with rebalancer for actual data movement

**Files:**
```
pkg/shard/stats.go                 Cross-shard edge tracking
pkg/cluster/partitioner.go         Label propagation graph partitioning
pkg/cluster/partitioner_test.go    Tests
```

## Dependency Graph

```
Phase 1 (Shard Keys) ─────┐
                           ├──► Phase 3 (Query Forwarding) ──► Phase 4 (Write Replication)
Phase 2 (Shard Placement) ─┘                                          │
                                                                       ▼
                                                          Phase 5 (Rebalancing)
                                                                       │
                                                                       ▼
                                                          Phase 6 (Locality)
```

Phases 1 and 2 are independent and can be built in parallel.
Phase 3 requires both 1 and 2.
Phase 4 requires 3.
Phases 5 and 6 require 4.
