package ingest

import (
	"fmt"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// ShardAdapter implements ShardRouter and ShardWriter using the real shard
// and router types. It bridges the ingest worker to the actual storage layer.
type ShardAdapter struct {
	shards    []*shard.Shard
	resolveFn func(key string) int
}

// NewShardAdapter creates a shard adapter.
func NewShardAdapter(shards []*shard.Shard, resolveKey func(key string) int) *ShardAdapter {
	return &ShardAdapter{
		shards:    shards,
		resolveFn: resolveKey,
	}
}

func (a *ShardAdapter) ResolveShardForKey(key string) int {
	return a.resolveFn(key)
}

func (a *ShardAdapter) ShardCount() int {
	return len(a.shards)
}

func (a *ShardAdapter) CopyFrom(shardID int, tableName, csvPath string) (int64, error) {
	if shardID >= len(a.shards) {
		return 0, fmt.Errorf("shard %d out of range", shardID)
	}
	cypher := fmt.Sprintf("COPY %s FROM '%s' (HEADER=true)", tableName, csvPath)
	_, err := a.shards[shardID].Query(cypher)
	if err != nil {
		return 0, err
	}
	// LadybugDB COPY FROM doesn't return row count directly,
	// so we estimate from the batch size.
	return -1, nil
}
