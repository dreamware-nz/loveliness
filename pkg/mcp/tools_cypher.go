package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// CypherInput is the input schema for cypher_read / cypher_write.
type CypherInput struct {
	Query  string         `json:"query" jsonschema:"Cypher statement to execute."`
	Params map[string]any `json:"params,omitempty" jsonschema:"Optional parameter bindings (referenced as $name in the query)."`
}

// CypherOutput is the structured output.
type CypherOutput struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   map[string]any   `json:"stats,omitempty"`
}

// writeKeywords are statements rejected by cypher_read.
//
// The spec names CREATE, MERGE, SET, DELETE, DROP, REMOVE, LOAD. The
// task brief extends this with COPY, ALTER, and DETACH to cover the
// LadybugDB surface (COPY FROM loads bulk data; DETACH DELETE is a
// common write form that starts with DETACH).
var writeKeywords = []string{
	"CREATE", "MERGE", "SET", "DELETE", "DROP", "REMOVE",
	"LOAD", "COPY", "ALTER", "DETACH",
}

// firstKeyword returns the first non-comment, non-whitespace token of q,
// uppercased. It handles /* ... */ block comments and // line comments,
// plus arbitrary leading whitespace.
func firstKeyword(q string) string {
	s := q
	for {
		s = strings.TrimLeftFunc(s, func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
		})
		switch {
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s[2:], "*/")
			if end < 0 {
				return ""
			}
			s = s[2+end+2:]
		case strings.HasPrefix(s, "//"):
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		default:
			// Extract keyword: run of letters.
			i := 0
			for i < len(s) && isLetter(s[i]) {
				i++
			}
			return strings.ToUpper(s[:i])
		}
	}
}

func isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isWrite returns true if q's first keyword is a known write form.
func isWrite(q string) bool {
	kw := firstKeyword(q)
	for _, w := range writeKeywords {
		if kw == w {
			return true
		}
	}
	return false
}

func registerCypherTools(s *mcp.Server, c *Client, readonly bool) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "cypher_read",
		Title:       "Run a read-only Cypher query",
		Description: "Execute a Cypher read query (MATCH/RETURN/CALL/...). Rejects write keywords (CREATE, MERGE, SET, DELETE, DROP, REMOVE, LOAD, COPY, ALTER, DETACH). Use $name placeholders with params for safe substitution.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CypherInput) (*mcp.CallToolResult, CypherOutput, error) {
		if strings.TrimSpace(in.Query) == "" {
			return toolError(fmt.Errorf("query is required")), CypherOutput{}, nil
		}
		if isWrite(in.Query) {
			return toolError(fmt.Errorf("cypher_read rejects write statements; use cypher_write for %s", firstKeyword(in.Query))), CypherOutput{}, nil
		}
		res, err := c.Cypher(ctx, in.Query, in.Params)
		if err != nil {
			return toolError(err), CypherOutput{}, nil
		}
		return nil, CypherOutput{Columns: res.Columns, Rows: res.Rows, Stats: res.Stats}, nil
	})

	if readonly {
		return
	}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "cypher_write",
		Title:       "Run a Cypher write query",
		Description: "Execute a Cypher write statement (CREATE/MERGE/SET/DELETE/etc) or DDL. Not registered when --readonly is set.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CypherInput) (*mcp.CallToolResult, CypherOutput, error) {
		if strings.TrimSpace(in.Query) == "" {
			return toolError(fmt.Errorf("query is required")), CypherOutput{}, nil
		}
		res, err := c.Cypher(ctx, in.Query, in.Params)
		if err != nil {
			return toolError(err), CypherOutput{}, nil
		}
		return nil, CypherOutput{Columns: res.Columns, Rows: res.Rows, Stats: res.Stats}, nil
	})
}
