# Large Graph Seeding Design

**Date**: 2026-03-31
**Status**: Approved
**Scope**: `cmd/benchmark/main.go` only

## Problem

The benchmark tool builds the entire node/edge CSV in a single `bytes.Buffer` then fires one HTTP POST to `/bulk/nodes`. At 10M nodes (~300MB payload) the connection resets. At 1B+ nodes it OOMs before even sending. The server-side streaming and bulk endpoints already handle large loads correctly — the problem is entirely in the benchmark seeder.

## Goal

`./benchmark -nodes 1_000_000_000 -edges 5_000_000_000` completes successfully, from both inside and outside the Fly.io cluster, without crashing or running out of memory.

## Design

### New flags

| Flag | Default | Description |
|------|---------|-------------|
| `-seed-workers` | `8` | Parallel goroutines for seeding |
| `-chunk-size` | `500_000` | Nodes or edges per HTTP POST |

Peak memory: `seed-workers × chunk-size × ~35 bytes ≈ 140MB` at defaults.

### Node seeding

1. Partition `[0, nodes)` into chunks of `chunk-size`, emit as `(start, end)` pairs on a channel.
2. `seed-workers` goroutines each pull a chunk, generate its rows into a local `bytes.Buffer`, POST to `/bulk/nodes`.
3. Each POST retries up to 3× with 2s backoff on connection error or 5xx.
4. An `atomic.Int64` accumulates total loaded; errors are collected and printed at the end.
5. A progress goroutine prints `loaded X / N nodes (Y nodes/sec)` every 5s.

`nodeName(i)` is deterministic and collision-free across partitions — no coordination needed between workers.

### Edge seeding

Edges are random pairs from `[0, nodes)`. Divide total edge count evenly across `seed-workers`. Each worker:

1. Uses its own `rand.New(rand.NewSource(workerID))` — independent sequences, no lock contention.
2. Generates `edges/seed-workers` pairs in `chunk-size` batches, POSTing each to `/bulk/edges`.
3. Same retry and progress pattern as nodes.

Duplicate edges across workers are acceptable for benchmark data.

### Schema DDL retry

`CREATE NODE TABLE` can fail with `BROADCAST_PARTIAL` due to a distributed race on schema propagation. Fix: retry up to 5× with 2s backoff before exiting. No other schema changes.

## Files changed

- `cmd/benchmark/main.go` — `seedLoveliness()` and related helpers only. No server-side changes.

## Out of scope

- Server-side `refTracker` memory growth at 1B+ nodes (separate issue — the in-memory key map grows unbounded; fix independently).
- Production ingest pipeline changes.
- Neo4j seeding path.
