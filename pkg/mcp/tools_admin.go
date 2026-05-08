package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DatabaseInfo describes a single database in the cluster catalog.
type DatabaseInfo struct {
	Name       string `json:"name"`
	State      string `json:"state,omitempty"`
	ShardCount int    `json:"shard_count,omitempty"`
	CreatedAt  any    `json:"created_at,omitempty"`
}

// ListDatabasesOutput is the structured response for list_databases.
type ListDatabasesOutput struct {
	Databases []DatabaseInfo `json:"databases"`
}

// CreateDatabaseInput is the input for create_database.
type CreateDatabaseInput struct {
	Name       string `json:"name" jsonschema:"Database name (identifier)."`
	ShardCount int    `json:"shard_count,omitempty" jsonschema:"Optional shard count; defaults to the cluster default when omitted or zero."`
}

// DropDatabaseInput is the input for drop_database.
type DropDatabaseInput struct {
	Name string `json:"name" jsonschema:"Database name to drop. Destructive — the database and all its data are removed."`
}

// SetDatabaseStateInput is the input for start_database / stop_database.
type SetDatabaseStateInput struct {
	Name string `json:"name" jsonschema:"Database name to start or stop."`
}

// AdminCypherInput is the input for the admin_cypher escape hatch.
type AdminCypherInput struct {
	Query string `json:"query" jsonschema:"Admin command (CREATE/STOP/START/DROP DATABASE, SHOW DATABASES). Other statements are rejected by the cluster."`
}

// AdminCypherOutput is the raw response payload from /admin/cypher.
type AdminCypherOutput struct {
	Result map[string]any `json:"result"`
}

// SimpleStatusOutput is the typed response for create/drop/start/stop database.
type SimpleStatusOutput struct {
	Status     string `json:"status"`
	Name       string `json:"name"`
	ShardCount int    `json:"shard_count,omitempty"`
}

func registerAdminTools(s *mcp.Server, c *Client, readonly bool) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_databases",
		Title:       "List databases in the cluster catalog",
		Description: "Return all databases registered in the cluster catalog with state, shard count, and creation timestamp. Wraps SHOW DATABASES via /admin/cypher.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, ListDatabasesOutput, error) {
		res, err := c.AdminCypher(ctx, "SHOW DATABASES")
		if err != nil {
			return toolError(err), ListDatabasesOutput{}, nil
		}
		out := ListDatabasesOutput{Databases: []DatabaseInfo{}}
		rows, _ := res["rows"].([]any)
		for _, r := range rows {
			row, ok := r.(map[string]any)
			if !ok {
				continue
			}
			info := DatabaseInfo{
				Name:      strOr(row["name"], ""),
				State:     strOr(row["state"], ""),
				CreatedAt: row["created_at"],
			}
			if n, ok := row["shard_count"].(float64); ok {
				info.ShardCount = int(n)
			}
			out.Databases = append(out.Databases, info)
		}
		return nil, out, nil
	})

	if readonly {
		return
	}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "create_database",
		Title:       "Create a database",
		Description: "Create a new database in the cluster catalog. Wraps CREATE DATABASE <name> [SHARDS n] via /admin/cypher. Must be sent to the leader.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CreateDatabaseInput) (*mcp.CallToolResult, SimpleStatusOutput, error) {
		if !validIdent(in.Name) {
			return toolError(fmt.Errorf("invalid database name: %q", in.Name)), SimpleStatusOutput{}, nil
		}
		stmt := "CREATE DATABASE " + in.Name
		if in.ShardCount > 0 {
			stmt += fmt.Sprintf(" SHARDS %d", in.ShardCount)
		}
		res, err := c.AdminCypher(ctx, stmt)
		if err != nil {
			return toolError(err), SimpleStatusOutput{}, nil
		}
		return nil, simpleStatus(res), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "drop_database",
		Title:       "Drop a database",
		Description: "Drop a database from the cluster catalog. Wraps DROP DATABASE <name> via /admin/cypher. Destructive — the database and all its data are removed. Must be sent to the leader.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in DropDatabaseInput) (*mcp.CallToolResult, SimpleStatusOutput, error) {
		if !validIdent(in.Name) {
			return toolError(fmt.Errorf("invalid database name: %q", in.Name)), SimpleStatusOutput{}, nil
		}
		res, err := c.AdminCypher(ctx, "DROP DATABASE "+in.Name)
		if err != nil {
			return toolError(err), SimpleStatusOutput{}, nil
		}
		return nil, simpleStatus(res), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "start_database",
		Title:       "Start a stopped database",
		Description: "Start a previously-stopped database. Wraps START DATABASE <name> via /admin/cypher. Must be sent to the leader.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SetDatabaseStateInput) (*mcp.CallToolResult, SimpleStatusOutput, error) {
		if !validIdent(in.Name) {
			return toolError(fmt.Errorf("invalid database name: %q", in.Name)), SimpleStatusOutput{}, nil
		}
		res, err := c.AdminCypher(ctx, "START DATABASE "+in.Name)
		if err != nil {
			return toolError(err), SimpleStatusOutput{}, nil
		}
		return nil, simpleStatus(res), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "stop_database",
		Title:       "Stop a running database",
		Description: "Stop a running database (catalog stays; data is unloaded). Wraps STOP DATABASE <name> via /admin/cypher. Must be sent to the leader.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SetDatabaseStateInput) (*mcp.CallToolResult, SimpleStatusOutput, error) {
		if !validIdent(in.Name) {
			return toolError(fmt.Errorf("invalid database name: %q", in.Name)), SimpleStatusOutput{}, nil
		}
		res, err := c.AdminCypher(ctx, "STOP DATABASE "+in.Name)
		if err != nil {
			return toolError(err), SimpleStatusOutput{}, nil
		}
		return nil, simpleStatus(res), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "admin_cypher",
		Title:       "Run an admin command (escape hatch)",
		Description: "Send a raw admin command to /admin/cypher. Accepts only CREATE/STOP/START/DROP DATABASE and SHOW DATABASES; the cluster rejects anything else. Prefer the typed list_databases / create_database / drop_database / start_database / stop_database tools.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in AdminCypherInput) (*mcp.CallToolResult, AdminCypherOutput, error) {
		if strings.TrimSpace(in.Query) == "" {
			return toolError(fmt.Errorf("query is required")), AdminCypherOutput{}, nil
		}
		res, err := c.AdminCypher(ctx, in.Query)
		if err != nil {
			return toolError(err), AdminCypherOutput{}, nil
		}
		return nil, AdminCypherOutput{Result: res}, nil
	})
}

// simpleStatus extracts the {status, name, shard_count} fields from a
// /admin/cypher mutation response. Missing fields are zero-valued.
func simpleStatus(res map[string]any) SimpleStatusOutput {
	out := SimpleStatusOutput{
		Status: strOr(res["status"], ""),
		Name:   strOr(res["name"], ""),
	}
	if n, ok := res["shard_count"].(float64); ok {
		out.ShardCount = int(n)
	}
	return out
}
