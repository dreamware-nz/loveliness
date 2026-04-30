package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// IngestNodesInput mirrors BulkNodesInput for the async /ingest path.
type IngestNodesInput struct {
	Table   string `json:"table"`
	CSVData string `json:"csv_data,omitempty"`
	CSVPath string `json:"csv_path,omitempty"`
}

// IngestEdgesInput mirrors BulkEdgesInput for the async /ingest path.
type IngestEdgesInput struct {
	RelTable  string `json:"rel_table"`
	FromTable string `json:"from_table"`
	ToTable   string `json:"to_table"`
	CSVData   string `json:"csv_data,omitempty"`
	CSVPath   string `json:"csv_path,omitempty"`
	SkipRefs  bool   `json:"skip_refs,omitempty"`
}

// IngestJobRef is the output for the enqueue tools: a job ID plus status.
type IngestJobRef struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// IngestStatusInput takes a single job ID.
type IngestStatusInput struct {
	JobID string `json:"job_id" jsonschema:"The async ingest job ID returned by ingest_nodes / ingest_edges."`
}

// IngestStatusOutput is the raw job record from /ingest/jobs/{id}.
type IngestStatusOutput struct {
	Job map[string]any `json:"job"`
}

func registerIngestTools(s *mcp.Server, c *Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "ingest_nodes",
		Title:       "Async bulk-load nodes",
		Description: "Enqueue an async node load. Returns a job_id; poll with ingest_status.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in IngestNodesInput) (*mcp.CallToolResult, IngestJobRef, error) {
		csv, err := loadCSV(in.CSVData, in.CSVPath)
		if err != nil {
			return toolError(err), IngestJobRef{}, nil
		}
		if in.Table == "" {
			return toolError(fmt.Errorf("table is required")), IngestJobRef{}, nil
		}
		res, err := c.IngestNodes(ctx, in.Table, csv)
		if err != nil {
			return toolError(err), IngestJobRef{}, nil
		}
		return nil, IngestJobRef{JobID: res.JobID, Status: res.Status}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "ingest_edges",
		Title:       "Async bulk-load edges",
		Description: "Enqueue an async relationship load. Returns a job_id; poll with ingest_status.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in IngestEdgesInput) (*mcp.CallToolResult, IngestJobRef, error) {
		csv, err := loadCSV(in.CSVData, in.CSVPath)
		if err != nil {
			return toolError(err), IngestJobRef{}, nil
		}
		if in.RelTable == "" || in.FromTable == "" || in.ToTable == "" {
			return toolError(fmt.Errorf("rel_table, from_table, and to_table are required")), IngestJobRef{}, nil
		}
		res, err := c.IngestEdges(ctx, in.RelTable, in.FromTable, in.ToTable, csv, in.SkipRefs)
		if err != nil {
			return toolError(err), IngestJobRef{}, nil
		}
		return nil, IngestJobRef{JobID: res.JobID, Status: res.Status}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "ingest_status",
		Title:       "Check async ingest job status",
		Description: "Return the current status / progress record for an async ingest job.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in IngestStatusInput) (*mcp.CallToolResult, IngestStatusOutput, error) {
		if in.JobID == "" {
			return toolError(fmt.Errorf("job_id is required")), IngestStatusOutput{}, nil
		}
		job, err := c.IngestStatus(ctx, in.JobID)
		if err != nil {
			return toolError(err), IngestStatusOutput{}, nil
		}
		return nil, IngestStatusOutput{Job: job}, nil
	})
}
