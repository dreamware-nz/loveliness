# TODOS

## Configure LadybugDB internal thread pool per shard
**Priority:** Medium
**Why:** LadybugDB uses morsel-driven parallelism internally, spawning its own thread pool per database instance. With multiple shards on one node (e.g., 3 shards on a 16-core machine), each shard's default thread pool will oversubscribe CPU, causing contention and degraded performance under load.
**What to do:** During the go-ladybug spike, investigate how to set LadybugDB's thread count when opening a database. Set `threads = total_cores / num_local_shards`. The go-ladybug bindings likely expose a `DatabaseConfig` or similar struct with a threads option (Kuzu had `SystemConfig.numThreads`).
**Depends on:** go-ladybug spike (Next Steps #2)
**Added:** 2026-03-26 by /plan-eng-review
