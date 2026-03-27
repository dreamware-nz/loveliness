# Project Structure

```
.golangci.yml                       Linter config: errcheck exclusions, staticcheck tuning
.github/
  workflows/
    ci.yml                          CI pipeline: test, lint, Docker build
    release.yml                     Release: GoReleaser binaries + Docker Hub push
deploy/
  k8s/
    namespace.yml                   Kubernetes namespace
    statefulset.yml                 3-replica StatefulSet with PVCs and health probes
    service.yml                     Headless service + LoadBalancer
    bootstrap-job.yml               Raft bootstrap helper job
cmd/
  loveliness/main.go              Entry point, config, shard init, Raft, HTTP, Bolt, TCP, ingest, shutdown
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
  bolt/
    packstream.go                 PackStream binary serialization (Neo4j wire format)
    messages.go                   Bolt message types (HELLO, RUN, PULL, SUCCESS, RECORD, etc.)
    chunking.go                   Bolt chunked transfer framing
    server.go                     Bolt protocol state machine and TCP server
    graph_types.go                Node/Relationship packing helpers
    router_adapter.go             Bridges Bolt QueryExecutor to Loveliness Router
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
  auth/
    auth.go                       Token-based authentication: HTTP middleware + Bolt credential check
  tlsutil/
    tlsutil.go                    Shared TLS config: server TLS, mTLS, client TLS
  cluster/
    fsm.go                        Raft FSM — shard map, node membership, assignments
    cluster.go                    Raft lifecycle — bootstrap, join, failover, leadership
    rebalancer.go                 Shard migration planning and execution
    partitioner.go                Cross-shard traversal stats + locality suggestions
    transfer.go                   Shard data transfer (export/import as Cypher)
    join_token.go                 Single-use, time-limited join tokens for secure cluster join
  api/
    api.go                        HTTP API — /cypher, /health, /cluster, /join
    bulk.go                       Bulk loading endpoints with streaming CSV parse
    bulk_stream.go                Streaming bulk load variant
    ingest.go                     Async ingest queue endpoints
    dr.go                         DR endpoints — backup, restore, export, WAL status
```
