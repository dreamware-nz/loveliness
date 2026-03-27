package transport

import "github.com/johnjansen/loveliness/pkg/shard"

// ShardSet wraps a slice of shards to satisfy the interface needed by TCPServer
// without requiring a full shard.Manager. This is useful when main.go creates
// shards directly rather than through the Manager.
type ShardSet struct {
	shards []*shard.Shard
}

// NewShardSet creates a ShardSet from a slice of shards.
func NewShardSet(shards []*shard.Shard) *ShardSet {
	return &ShardSet{shards: shards}
}

// GetShard returns the shard with the given ID, or nil.
func (ss *ShardSet) GetShard(id int) *shard.Shard {
	if id < 0 || id >= len(ss.shards) {
		return nil
	}
	return ss.shards[id]
}
