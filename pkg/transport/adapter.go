package transport

import (
	"context"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// RouterAdapter wraps a transport Client to satisfy router.RemoteQuerier
// and router.CtxRemoteQuerier (#84). It converts transport.QueryResponse
// to shard.QueryResponse.
type RouterAdapter struct {
	client *Client
}

// NewRouterAdapter creates a RouterAdapter wrapping the given Client.
func NewRouterAdapter(c *Client) *RouterAdapter {
	return &RouterAdapter{client: c}
}

// QueryRemoteShardCtx forwards a query to a remote node and converts the
// response, propagating the caller's context deadline onto the wire (#84).
// Prefer this entry point from any path that already carries a context.
func (a *RouterAdapter) QueryRemoteShardCtx(ctx context.Context, nodeID string, shardID int, cypher string) (*shard.QueryResponse, error) {
	return convertResp(a.client.QueryRemoteCtx(ctx, nodeID, shardID, cypher))
}

// QueryRemoteShard forwards a query to a remote node and converts the response.
func (a *RouterAdapter) QueryRemoteShard(nodeID string, shardID int, cypher string) (*shard.QueryResponse, error) {
	return convertResp(a.client.QueryRemote(nodeID, shardID, cypher))
}

func convertResp(resp *QueryResponse, err error) (*shard.QueryResponse, error) {
	if err != nil {
		return nil, err
	}
	return &shard.QueryResponse{
		Columns: resp.Columns,
		Rows:    resp.Rows,
		Stats: shard.QueryStats{
			CompileTimeMs: resp.Stats.CompileTimeMs,
			ExecTimeMs:    resp.Stats.ExecTimeMs,
		},
	}, nil
}
