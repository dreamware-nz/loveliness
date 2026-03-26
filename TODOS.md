# TODOS

*No open items.*

## Completed

### Configure LadybugDB internal thread pool per shard
**Status:** Done
**Resolution:** `cmd/loveliness/main.go` calculates `threadsPerShard = runtime.NumCPU() / shardCount` and passes it to `NewLbugStore()`, which sets `cfg.MaxNumThreads` on the LadybugDB `SystemConfig`. This prevents CPU oversubscription when multiple shards share a node.
