package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ClusterOutput is the structured cluster status.
type ClusterOutput struct {
	Leader   string         `json:"leader"`
	NodeID   string         `json:"node_id"`
	IsLeader bool           `json:"is_leader"`
	ShardMap map[string]any `json:"shard_map,omitempty"`
	Nodes    any            `json:"nodes,omitempty"`
	Schema   map[string]any `json:"schema,omitempty"`
}

func registerClusterTool(s *mcp.Server, c *Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "cluster_status",
		Title:       "Cluster topology and health",
		Description: "Return leader, peer list, per-shard assignment, and registered schema from GET /cluster.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, ClusterOutput, error) {
		st, err := c.Cluster(ctx)
		if err != nil {
			return toolError(err), ClusterOutput{}, nil
		}
		return nil, ClusterOutput{
			Leader:   st.Leader,
			NodeID:   st.NodeID,
			IsLeader: st.IsLeader,
			ShardMap: st.ShardMap,
			Nodes:    st.Nodes,
			Schema:   st.Schema,
		}, nil
	})
}
