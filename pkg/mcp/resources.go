package mcp

import (
	"context"
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerResources adds two read-only URL-addressed resources.
//
//	loveliness://schema   - same payload as the `schema` tool
//	loveliness://cluster  - same payload as the `cluster_status` tool
func registerResources(s *mcp.Server, c *Client, cache *schemaCache) {
	s.AddResource(&mcp.Resource{
		URI:         "loveliness://schema",
		Name:        "loveliness-schema",
		Title:       "Loveliness graph schema",
		Description: "Node and edge tables with property names and types.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		out, err := cache.get(ctx, c)
		if err != nil {
			return nil, err
		}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      "loveliness://schema",
				MIMEType: "application/json",
				Text:     string(b),
			}},
		}, nil
	})

	s.AddResource(&mcp.Resource{
		URI:         "loveliness://cluster",
		Name:        "loveliness-cluster",
		Title:       "Loveliness cluster status",
		Description: "Leader, nodes, shard assignment, registered schema.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		st, err := c.Cluster(ctx)
		if err != nil {
			return nil, err
		}
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      "loveliness://cluster",
				MIMEType: "application/json",
				Text:     string(b),
			}},
		}, nil
	})
}
