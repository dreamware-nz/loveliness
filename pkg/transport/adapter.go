package transport

import "github.com/johnjansen/loveliness/pkg/shard"

// RouterAdapter wraps a transport Client to satisfy router.RemoteQuerier.
// It converts transport.QueryResponse to shard.QueryResponse.
type RouterAdapter struct {
	client *Client
}

// NewRouterAdapter creates a RouterAdapter wrapping the given Client.
func NewRouterAdapter(c *Client) *RouterAdapter {
	return &RouterAdapter{client: c}
}

// QueryRemoteShard forwards a query to a remote node and converts the response.
func (a *RouterAdapter) QueryRemoteShard(nodeID string, shardID int, cypher string) (*shard.QueryResponse, error) {
	resp, err := a.client.QueryRemote(nodeID, shardID, cypher)
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
