// Package mcp implements a Model Context Protocol server that exposes
// a running Loveliness cluster to LLM agents over stdio JSON-RPC.
//
// The server talks to Loveliness over its regular HTTP API (/cypher,
// /bulk/*, /ingest/*, /cluster). The HTTP adapter lives in client.go.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is a thin HTTP adapter over the Loveliness HTTP API.
//
// It is deliberately not reusing the in-process router from pkg/api —
// the MCP server is a separate process from the cluster, and talks to
// whichever node it was pointed at via --url / LOVELINESS_URL.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// NewClient creates a new HTTP client.
//
// baseURL is the cluster HTTP endpoint (e.g. http://localhost:8080).
// token is an optional bearer token forwarded via Authorization.
// timeout is the per-request timeout.
func NewClient(baseURL, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: timeout},
	}
}

// CypherResult is the shape returned by POST /cypher.
type CypherResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   map[string]any   `json:"stats,omitempty"`
}

// Cypher executes a Cypher statement against /cypher.
//
// The Loveliness /cypher endpoint takes raw Cypher in the body (text/plain).
// Parameters are not bound at the HTTP layer today — if the caller provided
// params, they are inlined into the query via $-substitution. This is good
// enough for the MCP surface: the LLM sees a typed (query, params) contract,
// we flatten it at the edge.
func (c *Client) Cypher(ctx context.Context, query string, params map[string]any) (*CypherResult, error) {
	expanded, err := inlineParams(query, params)
	if err != nil {
		return nil, err
	}
	body, err := c.post(ctx, "/cypher", "text/plain", strings.NewReader(expanded), nil)
	if err != nil {
		return nil, err
	}
	var out CypherResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("cypher: decode response: %w", err)
	}
	return &out, nil
}

// ClusterStatus is the shape returned by GET /cluster.
type ClusterStatus struct {
	Leader       string         `json:"leader"`
	NodeID       string         `json:"node_id"`
	IsLeader     bool           `json:"is_leader"`
	ShardMap     map[string]any `json:"shard_map"`
	Nodes        any            `json:"nodes"`
	Schema       map[string]any `json:"schema"`
	Raw          []byte         `json:"-"`
}

// Cluster calls GET /cluster.
func (c *Client) Cluster(ctx context.Context) (*ClusterStatus, error) {
	body, err := c.get(ctx, "/cluster", nil)
	if err != nil {
		return nil, err
	}
	var out ClusterStatus
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("cluster: decode response: %w", err)
	}
	out.Raw = body
	return &out, nil
}

// BulkResult is the shape of /bulk/nodes and /bulk/edges responses.
type BulkResult struct {
	Table  string   `json:"table"`
	Loaded int64    `json:"loaded"`
	Errors []string `json:"errors,omitempty"`
}

// BulkNodes posts a CSV to /bulk/nodes with the X-Table header set.
func (c *Client) BulkNodes(ctx context.Context, table, csvData string) (*BulkResult, error) {
	headers := map[string]string{"X-Table": table}
	body, err := c.post(ctx, "/bulk/nodes", "text/csv", strings.NewReader(csvData), headers)
	if err != nil {
		return nil, err
	}
	var out BulkResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bulk/nodes: decode response: %w", err)
	}
	return &out, nil
}

// BulkEdges posts a CSV to /bulk/edges with required headers.
func (c *Client) BulkEdges(ctx context.Context, relTable, fromTable, toTable, csvData string, skipRefs bool) (*BulkResult, error) {
	headers := map[string]string{
		"X-Rel-Table":  relTable,
		"X-From-Table": fromTable,
		"X-To-Table":   toTable,
	}
	if skipRefs {
		headers["X-Skip-Refs"] = "true"
	}
	body, err := c.post(ctx, "/bulk/edges", "text/csv", strings.NewReader(csvData), headers)
	if err != nil {
		return nil, err
	}
	var out BulkResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bulk/edges: decode response: %w", err)
	}
	return &out, nil
}

// IngestResult is the shape of a 202 response from /ingest/nodes or /ingest/edges.
type IngestResult struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

// IngestNodes enqueues an async node ingest.
func (c *Client) IngestNodes(ctx context.Context, table, csvData string) (*IngestResult, error) {
	headers := map[string]string{"X-Table": table}
	body, err := c.post(ctx, "/ingest/nodes", "text/csv", strings.NewReader(csvData), headers)
	if err != nil {
		return nil, err
	}
	var out IngestResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("ingest/nodes: decode response: %w", err)
	}
	return &out, nil
}

// IngestEdges enqueues an async edge ingest.
func (c *Client) IngestEdges(ctx context.Context, relTable, fromTable, toTable, csvData string, skipRefs bool) (*IngestResult, error) {
	headers := map[string]string{
		"X-Rel-Table":  relTable,
		"X-From-Table": fromTable,
		"X-To-Table":   toTable,
	}
	if skipRefs {
		headers["X-Skip-Refs"] = "true"
	}
	body, err := c.post(ctx, "/ingest/edges", "text/csv", strings.NewReader(csvData), headers)
	if err != nil {
		return nil, err
	}
	var out IngestResult
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("ingest/edges: decode response: %w", err)
	}
	return &out, nil
}

// IngestJob is the shape of a single job record returned by /ingest/jobs/{id}.
type IngestJob map[string]any

// IngestStatus fetches the current status of an async ingest job.
func (c *Client) IngestStatus(ctx context.Context, jobID string) (IngestJob, error) {
	body, err := c.get(ctx, "/ingest/jobs/"+jobID, nil)
	if err != nil {
		return nil, err
	}
	var out IngestJob
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("ingest/status: decode response: %w", err)
	}
	return out, nil
}

// HTTPError represents a non-2xx HTTP response from Loveliness.
type HTTPError struct {
	Status int
	Code   string
	Msg    string
	Body   []byte
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("loveliness %d %s: %s", e.Status, e.Code, e.Msg)
	}
	return fmt.Sprintf("loveliness %d: %s", e.Status, truncate(string(e.Body), 256))
}

// IsTimeout returns true if err was a request timeout (context deadline).
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// net/http wraps deadline errors as url.Error.
	var he *HTTPError
	if errors.As(err, &he) && he.Status == http.StatusGatewayTimeout {
		return true
	}
	return false
}

func (c *Client) post(ctx context.Context, path, contentType string, body io.Reader, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	c.applyHeaders(req, headers)
	return c.do(req)
}

func (c *Client) get(ctx context.Context, path string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	c.applyHeaders(req, headers)
	return c.do(req)
}

func (c *Client) applyHeaders(req *http.Request, headers map[string]string) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}

func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20)) // 16 MB cap
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, parseHTTPError(resp.StatusCode, body)
	}
	return body, nil
}

// parseHTTPError turns a non-2xx response into a typed HTTPError.
// The Loveliness API consistently returns {error:{code, message}} JSON.
func parseHTTPError(status int, body []byte) *HTTPError {
	e := &HTTPError{Status: status, Body: body}
	var wire struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(body), &wire); err == nil {
		e.Code = wire.Error.Code
		e.Msg = wire.Error.Message
	}
	if e.Msg == "" {
		e.Msg = strings.TrimSpace(string(body))
	}
	return e
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
