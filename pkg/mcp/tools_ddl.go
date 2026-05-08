package mcp

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// PropertyInput is the typed shape of a single property/column for
// create_node_table / create_edge_table. The cluster validates the
// type string at DDL time; we only check that name and type are
// non-empty here.
type PropertyInput struct {
	Name       string `json:"name" jsonschema:"Property name (identifier)."`
	Type       string `json:"type" jsonschema:"LadybugDB type (e.g. STRING, INT64, DOUBLE, BOOL, DATE, TIMESTAMP, UUID, BLOB)."`
	PrimaryKey bool   `json:"primary_key,omitempty" jsonschema:"Mark this property as the primary key. Required on exactly one property of a node table."`
}

// CreateNodeTableInput is the input for create_node_table.
type CreateNodeTableInput struct {
	Table      string          `json:"table" jsonschema:"Node table name (identifier)."`
	Properties []PropertyInput `json:"properties" jsonschema:"Ordered list of properties; exactly one must be marked primary_key."`
}

// CreateEdgeTableInput is the input for create_edge_table.
type CreateEdgeTableInput struct {
	Table      string          `json:"table" jsonschema:"Relationship table name (identifier)."`
	From       string          `json:"from" jsonschema:"Source node table."`
	To         string          `json:"to" jsonschema:"Destination node table."`
	Properties []PropertyInput `json:"properties,omitempty" jsonschema:"Optional ordered list of edge properties (no primary key)."`
}

// DropTableInput is the input for drop_table.
type DropTableInput struct {
	Table string `json:"table" jsonschema:"Node or relationship table name to drop."`
}

// DDLOutput is the output for the schema mutation tools.
type DDLOutput struct {
	Statement string `json:"statement"`
	Stats     map[string]any `json:"stats,omitempty"`
}

// identRE matches a Cypher/Kuzu unquoted identifier. The DDL builders
// reject anything else to keep injection out of the generated string.
// Properly escaping identifiers would mean re-implementing Kuzu's
// identifier rules; rejecting non-conformant input is safer.
var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validIdent(s string) bool { return identRE.MatchString(s) }

// validType applies a permissive sanity check to a property type. We
// do not enumerate the full LadybugDB type grammar (parameterized
// numerics, arrays, maps, etc.) — instead we forbid characters that
// could escape the DDL string. The cluster does the real validation.
func validType(s string) bool {
	if s = strings.TrimSpace(s); s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z',
			r >= 'a' && r <= 'z',
			r >= '0' && r <= '9',
			r == '_', r == '(', r == ')', r == ',', r == ' ',
			r == '[', r == ']', r == '<', r == '>', r == ':':
			continue
		}
		return false
	}
	return true
}

func buildCreateNodeTableDDL(in CreateNodeTableInput) (string, error) {
	if !validIdent(in.Table) {
		return "", fmt.Errorf("invalid table name: %q", in.Table)
	}
	if len(in.Properties) == 0 {
		return "", fmt.Errorf("at least one property is required")
	}
	pkCount := 0
	parts := make([]string, 0, len(in.Properties)+1)
	for _, p := range in.Properties {
		if !validIdent(p.Name) {
			return "", fmt.Errorf("invalid property name: %q", p.Name)
		}
		if !validType(p.Type) {
			return "", fmt.Errorf("invalid property type: %q", p.Type)
		}
		col := p.Name + " " + p.Type
		if p.PrimaryKey {
			col += " PRIMARY KEY"
			pkCount++
		}
		parts = append(parts, col)
	}
	if pkCount != 1 {
		return "", fmt.Errorf("node table requires exactly one primary key property; got %d", pkCount)
	}
	return fmt.Sprintf("CREATE NODE TABLE %s(%s)", in.Table, strings.Join(parts, ", ")), nil
}

func buildCreateEdgeTableDDL(in CreateEdgeTableInput) (string, error) {
	if !validIdent(in.Table) {
		return "", fmt.Errorf("invalid table name: %q", in.Table)
	}
	if !validIdent(in.From) {
		return "", fmt.Errorf("invalid from table: %q", in.From)
	}
	if !validIdent(in.To) {
		return "", fmt.Errorf("invalid to table: %q", in.To)
	}
	parts := []string{"FROM " + in.From + " TO " + in.To}
	for _, p := range in.Properties {
		if !validIdent(p.Name) {
			return "", fmt.Errorf("invalid property name: %q", p.Name)
		}
		if !validType(p.Type) {
			return "", fmt.Errorf("invalid property type: %q", p.Type)
		}
		if p.PrimaryKey {
			return "", fmt.Errorf("relationship tables cannot have primary key properties")
		}
		parts = append(parts, p.Name+" "+p.Type)
	}
	return fmt.Sprintf("CREATE REL TABLE %s(%s)", in.Table, strings.Join(parts, ", ")), nil
}

func buildDropTableDDL(table string) (string, error) {
	if !validIdent(table) {
		return "", fmt.Errorf("invalid table name: %q", table)
	}
	return "DROP TABLE " + table, nil
}

func registerDDLTools(s *mcp.Server, c *Client, cache *schemaCache) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "create_node_table",
		Title:       "Create a node table",
		Description: "Create a node table with the given properties. Builds and runs a CREATE NODE TABLE DDL statement. Exactly one property must be marked primary_key. Invalidates the schema cache on success.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CreateNodeTableInput) (*mcp.CallToolResult, DDLOutput, error) {
		ddl, err := buildCreateNodeTableDDL(in)
		if err != nil {
			return toolError(err), DDLOutput{}, nil
		}
		res, err := c.Cypher(ctx, ddl, nil)
		if err != nil {
			return toolError(err), DDLOutput{Statement: ddl}, nil
		}
		cache.invalidate()
		return nil, DDLOutput{Statement: ddl, Stats: res.Stats}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "create_edge_table",
		Title:       "Create a relationship (edge) table",
		Description: "Create a relationship table from `from` to `to`. Builds and runs a CREATE REL TABLE DDL statement. Edge tables cannot have primary keys. Invalidates the schema cache on success.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CreateEdgeTableInput) (*mcp.CallToolResult, DDLOutput, error) {
		ddl, err := buildCreateEdgeTableDDL(in)
		if err != nil {
			return toolError(err), DDLOutput{}, nil
		}
		res, err := c.Cypher(ctx, ddl, nil)
		if err != nil {
			return toolError(err), DDLOutput{Statement: ddl}, nil
		}
		cache.invalidate()
		return nil, DDLOutput{Statement: ddl, Stats: res.Stats}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "drop_table",
		Title:       "Drop a node or edge table",
		Description: "Drop the named node or relationship table. Runs DROP TABLE <name>. Destructive — the table and all its data are removed. Invalidates the schema cache on success.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in DropTableInput) (*mcp.CallToolResult, DDLOutput, error) {
		ddl, err := buildDropTableDDL(in.Table)
		if err != nil {
			return toolError(err), DDLOutput{}, nil
		}
		res, err := c.Cypher(ctx, ddl, nil)
		if err != nil {
			return toolError(err), DDLOutput{Statement: ddl}, nil
		}
		cache.invalidate()
		return nil, DDLOutput{Statement: ddl, Stats: res.Stats}, nil
	})
}
