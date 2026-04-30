package mcp

import (
	"context"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// BulkNodesInput is the input schema for bulk_nodes.
//
// Exactly one of csv_data / csv_path must be set. When csv_path is given,
// the file is read from the MCP server's host — only files readable by
// the loveliness-mcp process are accessible.
type BulkNodesInput struct {
	Table    string `json:"table" jsonschema:"Node table name (required)."`
	CSVData  string `json:"csv_data,omitempty" jsonschema:"CSV body with a header row. Mutually exclusive with csv_path."`
	CSVPath  string `json:"csv_path,omitempty" jsonschema:"Path to a CSV file on the MCP server host. Mutually exclusive with csv_data."`
}

// BulkEdgesInput is the input schema for bulk_edges.
type BulkEdgesInput struct {
	RelTable  string `json:"rel_table" jsonschema:"Relationship table name (required)."`
	FromTable string `json:"from_table" jsonschema:"Source node table (required)."`
	ToTable   string `json:"to_table" jsonschema:"Destination node table (required)."`
	CSVData   string `json:"csv_data,omitempty"`
	CSVPath   string `json:"csv_path,omitempty"`
	SkipRefs  bool   `json:"skip_refs,omitempty" jsonschema:"Skip reference validation for faster loads."`
}

// BulkOutput mirrors the /bulk/* response shape.
type BulkOutput struct {
	Table  string   `json:"table"`
	Loaded int64    `json:"loaded"`
	Errors []string `json:"errors,omitempty"`
}

func loadCSV(data, path string) (string, error) {
	if data != "" && path != "" {
		return "", fmt.Errorf("csv_data and csv_path are mutually exclusive")
	}
	if data == "" && path == "" {
		return "", fmt.Errorf("one of csv_data or csv_path is required")
	}
	if data != "" {
		return data, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read csv_path: %w", err)
	}
	return string(b), nil
}

func registerBulkTools(s *mcp.Server, c *Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "bulk_nodes",
		Title:       "Bulk-load nodes from CSV",
		Description: "Synchronously load a CSV of node rows via POST /bulk/nodes. Provide either inline csv_data or a host csv_path.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in BulkNodesInput) (*mcp.CallToolResult, BulkOutput, error) {
		csv, err := loadCSV(in.CSVData, in.CSVPath)
		if err != nil {
			return toolError(err), BulkOutput{}, nil
		}
		if in.Table == "" {
			return toolError(fmt.Errorf("table is required")), BulkOutput{}, nil
		}
		res, err := c.BulkNodes(ctx, in.Table, csv)
		if err != nil {
			return toolError(err), BulkOutput{}, nil
		}
		return nil, BulkOutput{Table: res.Table, Loaded: res.Loaded, Errors: res.Errors}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "bulk_edges",
		Title:       "Bulk-load edges from CSV",
		Description: "Synchronously load a CSV of relationship rows via POST /bulk/edges. Provide either inline csv_data or a host csv_path.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in BulkEdgesInput) (*mcp.CallToolResult, BulkOutput, error) {
		csv, err := loadCSV(in.CSVData, in.CSVPath)
		if err != nil {
			return toolError(err), BulkOutput{}, nil
		}
		if in.RelTable == "" || in.FromTable == "" || in.ToTable == "" {
			return toolError(fmt.Errorf("rel_table, from_table, and to_table are required")), BulkOutput{}, nil
		}
		res, err := c.BulkEdges(ctx, in.RelTable, in.FromTable, in.ToTable, csv, in.SkipRefs)
		if err != nil {
			return toolError(err), BulkOutput{}, nil
		}
		return nil, BulkOutput{Table: res.Table, Loaded: res.Loaded, Errors: res.Errors}, nil
	})
}
