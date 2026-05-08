package mcp

import (
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCP-side error codes used when returning a tool error.
//
// The Go SDK renders tool-call failures via CallToolResult.IsError=true
// with textual content; the numeric codes in the design doc map to the
// codes the client should see in the error message text.
const (
	codeInvalidParams = -32602 // parse / validation
	codeServerError   = -32000 // generic server error
	codeTimeout       = -32001 // timeout
)

// toolError wraps an error into an MCP tool failure result.
//
// The Loveliness HTTP error codes are mapped per the spec table:
//
//	400 with parser error       -> -32602 "Cypher parse error..."
//	409 BROADCAST_PARTIAL       -> -32000 "Schema propagation in progress, retry"
//	503 (shard unavailable)     -> -32000 "Shard unavailable, check cluster_status"
//	timeout                     -> -32001 "Query exceeded timeout"
func toolError(err error) *mcp.CallToolResult {
	code, msg := classify(err)
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("error %d: %s", code, msg),
		}},
	}
}

func classify(err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	if IsTimeout(err) {
		return codeTimeout, "query exceeded the configured timeout; increase --timeout or narrow the query"
	}
	var he *HTTPError
	if errors.As(err, &he) {
		switch {
		case he.Status == 400 && (he.Code == "CYPHER_PARSE_ERROR" || he.Code == "MISSING_SHARD_KEY" || he.Code == "BAD_REQUEST" || he.Code == "BAD_CSV" || he.Code == "UNKNOWN_TABLE" || he.Code == "NO_SCHEMA"):
			return codeInvalidParams, fmt.Sprintf("cypher parse error: %s", he.Msg)
		case he.Status == 409 && he.Code == "BROADCAST_PARTIAL":
			return codeServerError, "schema propagation in progress, retry"
		case he.Status == 503:
			return codeServerError, fmt.Sprintf("shard unavailable: %s (check cluster_status)", he.Msg)
		case he.Status == 504 || he.Code == "QUERY_TIMEOUT":
			return codeTimeout, fmt.Sprintf("query exceeded timeout: %s", he.Msg)
		case he.Status == 401 || he.Status == 403:
			return codeServerError, fmt.Sprintf("authentication failed: %s (check LOVELINESS_TOKEN)", he.Msg)
		}
		return codeServerError, he.Error()
	}
	return codeServerError, err.Error()
}
