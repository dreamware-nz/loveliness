# Cross-node transport — prior-art survey

One-sentence cells per item × system. Docs-only sources (linked at the bottom); no source-reading. Bias is toward what each system **does today** in user-facing docs, not what its design papers aspire to.

## Systems

- **Neo4j Fabric** — Neo4j's federated query layer; uses the Bolt protocol over TCP to talk to remote DBMSes inside one Fabric query plan.
- **DGraph** — distributed graph DB; scatter-gather to Alpha nodes over gRPC.
- **MemGraph** — Bolt-protocol graph DB; high-availability via Raft replication and Bolt routing.
- **CockroachDB** — distributed SQL; KV and DistSQL flows over gRPC with explicit deadline + retry + admission-control machinery.

## 4 × 10 matrix

| # | Gap | Neo4j Fabric | DGraph | MemGraph | CockroachDB |
|---|---|---|---|---|---|
| 1 | Teardown unblocks in-flight reads | Bolt server closes active sessions on shutdown via `dbms.shutdown_transaction_end_timeout` rather than waiting on a per-conn read deadline. | gRPC server's `GracefulStop` cancels in-flight RPCs via context, then `Stop` force-closes; no per-conn idle wait. | Documented `--bolt-session-inactivity-timeout` is independent of shutdown path; HA replicas drain before exit. | gRPC `GracefulStop` plus per-server shutdown hooks; `server.shutdown.drain_wait` and `.query_wait` give operators explicit control. |
| 2 | Pool sizing vs scatter cap | Driver-side: `dbms.connector.bolt.thread_pool_max_size` + per-driver `maxConnectionPoolSize`; users tune both. | HTTP/2 multiplexing through gRPC means a single connection carries many concurrent streams, so pool size is decoupled from fan-out. | Bolt routing connections sized per replica; client-side pool config (`MEMGRAPH_BOLT_NUM_CONNECTIONS`). | Per-node RPC connection class plus HTTP/2 multiplexing; pool size isn't the scatter bound. |
| 3 | Deadline propagation to remote | Fabric forwards transaction timeout to remote DBMSes via Bolt `tx_timeout` metadata. | gRPC `context.Deadline` propagates through call metadata to Alpha-server handlers. | Bolt `tx_timeout` honored end-to-end on the replica that owns the read. | SQL `statement_timeout` propagates into KV layer via `roachpb.Header.Timestamp` and per-request context deadlines. |
| 4 | Whole-query retry budget | No explicit budget; driver-level retry helpers (e.g. `Session.executeRead`) are per-tx with attempt count + backoff. | gRPC retry policy is per-call; no documented scatter-level budget. | Bolt driver retries are per-tx; HA routing retries on a different replica without a global cap. | DistSQL caps per-flow retries; admission control + queue-depth budgets the host, not the query, but `kv.transaction.max_refresh_spans_bytes` and friends bound retry amplification. |
| 5 | Streaming / chunked responses | Bolt is record-streaming by design — `PULL n` pulls in chunks; no buffered "all rows then send". | gRPC server-streaming RPCs are first-class; `Query` returns a stream of result chunks. | Bolt streaming as in Neo4j (same protocol family). | DistSQL flows stream rows between nodes over gRPC streaming RPCs. |
| 6 | Cancel / partial-result signalling | Bolt `RESET` cancels the current statement on the open connection; `<INTERRUPT>` aborts. | gRPC client `context.Cancel()` propagates to server via `ctx.Done()`; server cleans up its flow. | Bolt `RESET` as in Neo4j. | gRPC cancellation tears down DistSQL flow; `pg_cancel_backend()` for SQL-layer cancel. |
| 7 | Per-RPC correlation ID | Bolt connection ID + transaction ID written to query logs; correlation across logs is documented. | OpenTelemetry trace + span IDs propagated through gRPC metadata. | Bolt session ID logged on every statement; mirrors Neo4j. | TraceID propagated via gRPC metadata; surfaces in `EXPLAIN ANALYZE (DEBUG)` and statement bundles. |
| 8 | mTLS exercised / cert rotation | Java SSLContext on both ends; Neo4j Ops Manual documents rolling cert rotation with reload-without-restart. | TLS docs cover client-cert auth; rotation requires Alpha restart per docs (no live-reload). | mTLS supported; cert rotation requires reload per docs. | First-class: `cockroach cert create-node`, online rotation via `SIGHUP` reloads cert dir. |
| 9 | Admission control / QoS | Procedure-level `dbms.transaction.timeout` + a per-database transaction concurrency limit; no priority classes. | Per-Alpha rate limit (`--limit`) and pending-transaction caps; no explicit priority classes. | No documented admission framework; rely on Bolt session caps. | Explicit AC framework: priority classes (`background`/`normal`/`high`), elastic queues per resource (CPU, IO, KV), tunable via cluster settings. |
| 10 | Proactive heartbeat / health-check | Bolt `NOOP` chunks used as keepalive; driver `connectionLivenessCheckTimeout` polls idle conns. | HTTP/2 PING frames; gRPC client keepalive (`grpc.keepalive_time_ms`). | Bolt NOOP keepalive; HA layer adds Raft heartbeats. | gRPC keepalive + cluster gossip for node liveness; failed nodes marked draining within a few heartbeats. |

## Sources

- Neo4j: <https://neo4j.com/docs/operations-manual/current/fabric/>, <https://neo4j.com/docs/bolt/current/>
- DGraph: <https://dgraph.io/docs/deploy/>, <https://dgraph.io/docs/clients/>
- MemGraph: <https://memgraph.com/docs/clustering/high-availability>, <https://memgraph.com/docs/getting-started/connect-to-memgraph/drivers>
- CockroachDB: <https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer.html>, <https://www.cockroachlabs.com/docs/stable/admission-control.html>, <https://www.cockroachlabs.com/docs/stable/cockroach-cert.html>

## What the consensus says

- **Streaming is universal.** Every system in the survey streams results by default. We are the outlier on item 5.
- **Deadline propagation is universal.** Every system carries a deadline on the wire and respects it server-side. We are the outlier on item 3.
- **Cancel is universal.** Bolt has `RESET`; gRPC has context cancellation. We are the outlier on item 6.
- **Retry budgets are not universal.** Only CockroachDB documents a budget-shaped story; others retry per-call with no global cap. Our gap on item 4 is real but the prior art is thinner.
- **Correlation IDs are universal.** Trace IDs (gRPC) or session/tx IDs (Bolt) — every system surfaces them in logs.
- **mTLS rotation varies.** Neo4j + CockroachDB do live reload; DGraph + MemGraph require restart. Item 8 is "good enough to ship" in the survey set, not "best in class".
- **Admission control is rare.** Only CockroachDB has a real framework; the others rely on session/connection caps. Item 9 is genuinely a frontier item.
- **Heartbeats are universal.** Every system has either Bolt NOOP or gRPC keepalive. Our `MsgPing/MsgPong` exists but isn't wired into a loop — easy delta.
