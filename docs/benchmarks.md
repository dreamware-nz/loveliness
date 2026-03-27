# Benchmarks

Benchmarked on Apple M1 Pro, single node, 4 shards.

## At 15.7M nodes / 10M edges

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

## Bulk Loading Throughput

| Operation | Throughput | Notes |
|---|---|---|
| Node loading (COPY FROM) | **70–190K nodes/sec** | Degrades with B-tree depth at scale |
| Edge loading (2-pass) | **9.6K edges/sec** | Pass 1: ref nodes, Pass 2: edges |

## How We Compare (estimated, 10–15M node scale)

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

## Inter-Node Transport: TCP+MessagePack vs HTTP+JSON

Internal cluster communication uses a binary TCP transport with MessagePack serialization instead of HTTP+JSON. Micro-benchmarks on 1000-row result sets:

| Scenario | HTTP+JSON | TCP+MsgPack | Speedup |
|---|---|---|---|
| Single query (250 rows) | 551us | 269us | **2.1x** |
| Scatter-gather 4 shards | 1,339us | 658us | **2.0x** |
| 8-worker concurrent | 397us | 94us | **4.2x** |
| Marshal 1000 rows | 481us | 198us | **2.4x** |
| Unmarshal 1000 rows | 1,247us | 442us | **2.8x** |
