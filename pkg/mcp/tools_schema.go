package mcp

import (
	"context"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// SchemaOutput is the structured schema output.
type SchemaOutput struct {
	NodeTables []NodeTable `json:"node_tables"`
	EdgeTables []EdgeTable `json:"edge_tables"`
}

// NodeTable describes a node table.
type NodeTable struct {
	Name       string     `json:"name"`
	PrimaryKey string     `json:"primary_key,omitempty"`
	Properties []Property `json:"properties"`
}

// EdgeTable describes a relationship table.
type EdgeTable struct {
	Name       string     `json:"name"`
	From       string     `json:"from,omitempty"`
	To         string     `json:"to,omitempty"`
	Properties []Property `json:"properties"`
}

// Property is a single column in a node/edge table.
type Property struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key,omitempty"`
}

// schemaCache is an in-process schema cache with a 30s TTL.
//
// schema() is called on almost every agent turn, and hammering the
// cluster for unchanged data is wasteful. Errors are not cached — a
// transient cluster blip should not lock out schema lookups for the
// full TTL window.
type schemaCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	cachedAt time.Time
	cached   *SchemaOutput
}

func newSchemaCache(ttl time.Duration) *schemaCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &schemaCache{ttl: ttl}
}

func (sc *schemaCache) get(ctx context.Context, c *Client) (*SchemaOutput, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.cached != nil && time.Since(sc.cachedAt) < sc.ttl {
		return sc.cached, nil
	}
	out, err := fetchSchema(ctx, c)
	if err != nil {
		return nil, err
	}
	sc.cached = out
	sc.cachedAt = time.Now()
	return out, nil
}

// fetchSchema runs CALL show_tables() and CALL table_info(<name>) per
// table, structuring the result.
func fetchSchema(ctx context.Context, c *Client) (*SchemaOutput, error) {
	tablesRes, err := c.Cypher(ctx, "CALL show_tables() RETURN *", nil)
	if err != nil {
		return nil, err
	}

	out := &SchemaOutput{}
	for _, row := range tablesRes.Rows {
		name, _ := row["name"].(string)
		ttype, _ := row["type"].(string)
		if name == "" {
			continue
		}
		infoRes, err := c.Cypher(ctx, "CALL table_info('"+escapeSingle(name)+"') RETURN *", nil)
		if err != nil {
			// Propagate but still include what we have — a half-schema
			// is worse than none for LLM reasoning.
			return nil, err
		}
		switch ttype {
		case "NODE":
			nt := NodeTable{Name: name}
			for _, p := range infoRes.Rows {
				prop := Property{
					Name: strOr(p["name"], ""),
					Type: strOr(p["type"], ""),
				}
				if asBool(p["primary key"]) {
					prop.PrimaryKey = true
					nt.PrimaryKey = prop.Name
				}
				if prop.Name != "" {
					nt.Properties = append(nt.Properties, prop)
				}
			}
			out.NodeTables = append(out.NodeTables, nt)
		case "REL":
			et := EdgeTable{Name: name}
			// LadybugDB's show_tables result also carries "source/destination"
			// for REL tables in some versions. Best-effort extraction.
			et.From = strOr(row["src table"], strOr(row["source table"], ""))
			et.To = strOr(row["dst table"], strOr(row["destination table"], ""))
			for _, p := range infoRes.Rows {
				prop := Property{
					Name: strOr(p["name"], ""),
					Type: strOr(p["type"], ""),
				}
				if prop.Name != "" {
					et.Properties = append(et.Properties, prop)
				}
			}
			out.EdgeTables = append(out.EdgeTables, et)
		}
	}
	return out, nil
}

func strOr(v any, def string) string {
	if s, ok := v.(string); ok {
		return s
	}
	return def
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case int64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}

func escapeSingle(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\\', '\'')
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

func registerSchemaTool(s *mcp.Server, c *Client, cache *schemaCache) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "schema",
		Title:       "Introspect the graph schema",
		Description: "Return node and edge tables with property names and types. Result is cached for 30s on the MCP server.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, SchemaOutput, error) {
		out, err := cache.get(ctx, c)
		if err != nil {
			return toolError(err), SchemaOutput{}, nil
		}
		return nil, *out, nil
	})
}
