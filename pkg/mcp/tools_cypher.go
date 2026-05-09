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
	// Format selects the response wire format. "json" (default) returns
	// rows as structured output for the LLM to read directly. "arrow"
	// returns an Apache Arrow IPC stream as an embedded resource on
	// the tool result, paired with a brief text summary — use this
	// when the downstream consumer (DuckDB-WASM, pyarrow, polars) can
	// ingest Arrow bytes without a JSON parse step.
	Format string `json:"format,omitempty" jsonschema:"Result format: 'json' (default) or 'arrow' (IPC stream attached as embedded resource)."`
}

// CypherOutput is the structured output for the JSON path.
type CypherOutput struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   map[string]any   `json:"stats,omitempty"`
}

// arrowResourceURI is the URI we attach to embedded Arrow result
// blobs. It is informational — clients identify the payload by MIME
// type, not URI — but a stable scheme makes it easier to spot in
// logs and traces.
const arrowResourceURI = "loveliness:cypher-result.arrows"

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

// writeProcedures are LadybugDB CALL procedures that mutate state or
// touch the host filesystem. cypher_read additionally rejects CALL
// statements whose target procedure name appears here, so an LLM can't
// sneak past the keyword gate with `CALL drop_table('Person')`.
//
// Names are matched case-insensitively against the identifier that
// follows the leading CALL token.
var writeProcedures = map[string]struct{}{
	"drop_table":              {},
	"create_node_table":       {},
	"create_rel_table":        {},
	"create_node_table_group": {},
	"create_rel_table_group":  {},
	"create_project_graph":    {},
	"drop_project_graph":      {},
	"alter_table_add_column":  {},
	"alter_table_drop_column": {},
	"alter_table_rename":      {},
	"copy_from":               {},
	"copy_to":                 {},
	"copy_npy":                {},
	"export_database":         {},
	"import_database":         {},
	"install":                 {},
	"load_extension":          {},
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

// isWrite returns true if q's first keyword is a known write form, or
// if it is a CALL whose target procedure is a known mutating procedure.
func isWrite(q string) bool {
	kw := firstKeyword(q)
	for _, w := range writeKeywords {
		if kw == w {
			return true
		}
	}
	if kw == "CALL" {
		if proc := callTarget(q); proc != "" {
			if _, mut := writeProcedures[strings.ToLower(proc)]; mut {
				return true
			}
		}
	}
	return false
}

// callTarget returns the procedure identifier that follows a leading
// CALL keyword in q, or "" if it can't be parsed. Comments and
// whitespace before/between tokens are skipped using the same logic as
// firstKeyword.
func callTarget(q string) string {
	s := skipLeading(q)
	if !strings.HasPrefix(strings.ToUpper(s), "CALL") {
		return ""
	}
	s = s[4:]
	s = skipLeading(s)
	i := 0
	for i < len(s) && (isLetter(s[i]) || s[i] == '_' || (i > 0 && s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	return s[:i]
}

// skipLeading strips whitespace and /* */ // comments from the start
// of s. Mirrors firstKeyword's loop body.
func skipLeading(s string) string {
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
			return s
		}
	}
}

func registerCypherTools(s *mcp.Server, c *Client, readonly bool) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "cypher_read",
		Title:       "Run a read-only Cypher query",
		Description: "Execute a Cypher read query (MATCH/RETURN/CALL/...). Rejects write keywords (CREATE, MERGE, SET, DELETE, DROP, REMOVE, LOAD, COPY, ALTER, DETACH). Use $name placeholders with params for safe substitution. Set format='arrow' to receive an Apache Arrow IPC stream as an embedded resource (skip JSON parsing for analytics consumers).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CypherInput) (*mcp.CallToolResult, CypherOutput, error) {
		if strings.TrimSpace(in.Query) == "" {
			return toolError(fmt.Errorf("query is required")), CypherOutput{}, nil
		}
		if isWrite(in.Query) {
			return toolError(fmt.Errorf("cypher_read rejects write statements; use cypher_write for %s", firstKeyword(in.Query))), CypherOutput{}, nil
		}
		return runCypher(ctx, c, in)
	})

	if readonly {
		return
	}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "cypher_write",
		Title:       "Run a Cypher write query",
		Description: "Execute a Cypher write statement (CREATE/MERGE/SET/DELETE/etc) or DDL. Not registered when --readonly is set. Set format='arrow' to receive an Apache Arrow IPC stream as an embedded resource (mostly useful for write-then-RETURN queries).",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CypherInput) (*mcp.CallToolResult, CypherOutput, error) {
		if strings.TrimSpace(in.Query) == "" {
			return toolError(fmt.Errorf("query is required")), CypherOutput{}, nil
		}
		return runCypher(ctx, c, in)
	})
}

// runCypher dispatches a Cypher request to the right code path based
// on the requested response format. JSON is the default (LLM-friendly
// structured output); Arrow attaches an IPC stream blob to the tool
// result so analytics consumers can skip a JSON parse round-trip.
func runCypher(ctx context.Context, c *Client, in CypherInput) (*mcp.CallToolResult, CypherOutput, error) {
	switch strings.ToLower(strings.TrimSpace(in.Format)) {
	case "", "json":
		res, err := c.Cypher(ctx, in.Query, in.Params)
		if err != nil {
			return toolError(err), CypherOutput{}, nil
		}
		return nil, CypherOutput{Columns: res.Columns, Rows: res.Rows, Stats: res.Stats}, nil
	case "arrow":
		buf, err := c.CypherArrow(ctx, in.Query, in.Params)
		if err != nil {
			return toolError(err), CypherOutput{}, nil
		}
		return arrowResult(buf), CypherOutput{}, nil
	default:
		return toolError(fmt.Errorf("format must be 'json' or 'arrow', got %q", in.Format)), CypherOutput{}, nil
	}
}

// arrowResult builds a CallToolResult that pairs a brief text
// summary (for the LLM / human reader) with an embedded Arrow IPC
// resource carrying the actual bytes. The text summary intentionally
// stays terse — the whole point of the Arrow path is that the LLM
// shouldn't need to read the full result; an analytics-aware client
// reads the embedded resource instead.
func arrowResult(buf []byte) *mcp.CallToolResult {
	size := int64(len(buf))
	summary := fmt.Sprintf("Arrow IPC stream attached (%d bytes). MIME type %s.", size, ArrowStreamMIME)
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: summary},
			&mcp.EmbeddedResource{
				Resource: &mcp.ResourceContents{
					URI:      arrowResourceURI,
					MIMEType: ArrowStreamMIME,
					Blob:     buf,
				},
			},
		},
	}
}
