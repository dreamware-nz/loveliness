# Metrics & label budget

Loveliness exposes Prometheus text-format metrics at `GET /metrics`. The
exposition is hand-rolled (no `prometheus/client_golang` dependency) so
the dependency surface stays small and the label budget is governed
explicitly here, not by collector defaults.

## Label policy

- Never label by anything unbounded (`user_id`, `query_id`, raw HTTP
  status, raw Cypher text, IP).
- Bucket high-cardinality dimensions before labeling: HTTP status →
  `ok` / `client_error` / `server_error`; Cypher → `read` / `write` /
  `schema` / `unknown`.
- The total active label cardinality across all series on a node must
  stay ≤ ~10 000. The table below is the per-metric budget that
  enforces it.

## Series catalogue

| Metric | Type | Labels | Cardinality bound |
|---|---|---|---|
| `loveliness_uptime_seconds` | gauge | — | 1 |
| `loveliness_local_shards` | gauge | — | 1 |
| `loveliness_shard_healthy` | gauge | `shard_id` | ≤ 256 |
| `loveliness_raft_state` | gauge | `node_id`, `state` | 1 × 5 = **5** |
| `loveliness_query_total` | counter | `query_type`, `status` | 4 × 4 = **16** |
| `loveliness_query_duration_seconds` | histogram | `query_type`, `status` | 4 × 4 × (13 buckets + `_sum` + `_count` + `+Inf`) ≈ **256** |
| `loveliness_bulk_load_rows_total` | counter | `table` | ≤ tables (~hundreds) |
| `loveliness_wal_global_sequence` | gauge | — | 1 |
| `loveliness_wal_head_sequence` | gauge | `shard_id` | ≤ 256 |
| `loveliness_replication_lag_entries` | gauge | `shard_id`, `replica_id` | ≤ 256 × RF |
| `loveliness_replication_lag_bytes` | gauge | `shard_id`, `replica_id` | ≤ 256 × RF |
| `loveliness_replication_lag_seconds` | gauge | `shard_id`, `replica_id` | ≤ 256 × RF |
| `loveliness_router_remote_rtt_seconds` | histogram | `shard_id` | ≤ 256 × (14 buckets + `_sum` + `_count` + `+Inf`) ≈ **4 352** |
| `loveliness_router_remote_errors_total` | counter | `code` | ≤ 7 (closed set) |
| `loveliness_router_bloom_skip_total` | counter | `shard_id` | ≤ 256 |
| `loveliness_go_goroutines` | gauge | — | 1 |
| `loveliness_go_memstats_alloc_bytes` | gauge | — | 1 |
| `loveliness_go_memstats_sys_bytes` | gauge | — | 1 |
| `loveliness_go_memstats_heap_inuse_bytes` | gauge | — | 1 |
| `loveliness_go_memstats_heap_objects` | gauge | — | 1 |
| `loveliness_go_gc_count_total` | counter | — | 1 |
| `loveliness_go_gc_pause_seconds_total` | counter | — | 1 |

At the documented bounds (`shard_count = 256`, `replication_factor = 3`,
`tables ≤ 200`):

```
fixed series         ≈ 30
query_total          = 16
query_duration       ≈ 256
bulk_load_rows_total ≤ 200
wal_head_sequence    ≤ 256
replication_lag × 3  ≤ 256 × 3 × 3 = 2304
router_remote_rtt    ≤ 256 × 17 ≈ 4 352
router_remote_errors ≤ 7
router_bloom_skip    ≤ 256
─────────────────────
total                ≲ 7 700
```

That leaves ~2 300 of headroom under the 10 000 budget for additional
per-shard gauges (e.g. RSS, vertex/edge count) when those land.

## Label values

### `query_type` (≤ 4)

`read` · `write` · `schema` · `unknown`. Computed by
`classifyQueryType` in `pkg/api/query_metrics.go` from the Cypher
prefix without paying the shard-key extraction cost.

### `status` (≤ 4)

`ok` (2xx) · `client_error` (4xx) · `server_error` (5xx) · `unknown`
(anything else, including 1xx and 3xx). Computed by `statusBucket`.

### `state` (raft, exactly 5)

`leader` · `follower` · `candidate` · `shutdown` · `unknown`. Emitted
as a one-hot gauge: at every scrape, exactly one series for this node
has value 1, the rest are 0. This keeps dashboards continuous if the
upstream raft library adds a state.

### `shard_id`

Stringified integer in `[0, shard_count)`. Bounded by configuration.

### `replica_id`

The node ID assigned to that shard's replica slot in the placement
plan. Bounded by `RF × shard_count`.

### `table`

Catalogued node/edge table name. Cardinality is bounded by the schema —
production deployments typically have ≤ ~200 tables.

### `code` (router remote error, closed set ≤ 7)

`timeout` · `canceled` · `conn_refused` · `conn_reset` · `broken_pipe` ·
`eof` · `other`. Computed by `classifyRemoteError` in
`pkg/router/metrics.go` from the unwrap chain of the transport error
(`errors.Is`, `errors.As` on `*net.OpError`). The `other` bucket
collapses every protocol/query error into a single label so a flood of
unique error strings can't blow up the scrape.

## Histogram buckets

`loveliness_query_duration_seconds` uses these upper bounds (seconds):

```
0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01,
0.025,   0.05,   0.1,   0.25,  0.5,   1.0, 2.5
```

Aligned with the Loveliness latency targets — most reads sit in the
first six buckets, most writes in the next three; the long tail covers
slow scans without burning a bucket on every order of magnitude.

`loveliness_router_remote_rtt_seconds` uses these upper bounds (seconds):

```
0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
0.025,   0.05,   0.1,   0.25,   0.5,   1.0, 2.5, 5.0
```

The lower end (250 µs) covers in-process loopback (single-host
multi-shard); the upper end (5 s) brackets the worst-case scatter
timeout. Widened compared to the query histogram because cross-node
latency has a longer tail than local execution.

## Adding a new metric

1. Decide whether the dimension you want to label by is bounded. If
   not, find a bucketed projection of it that is.
2. Document the new series in the table above before merging.
3. Emit through the helpers in `pkg/api/metrics.go`
   (`emitHelp`, `fprintf`, `formatGaugeFloat`) so the exposition format
   stays consistent.
4. Add a unit test that asserts both the `# HELP` / `# TYPE` lines and
   at least one full sample line — that's how we catch silent format
   drift across releases.
