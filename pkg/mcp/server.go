package mcp

import (
	"context"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is the advertised server version.
const Version = "0.1.0"

// Options configures the MCP server.
type Options struct {
	// URL is the HTTP endpoint of a Loveliness node.
	URL string
	// Token is an optional bearer token.
	Token string
	// Timeout is the per-request HTTP timeout.
	Timeout time.Duration
	// Readonly disables mutating tools (cypher_write, bulk_*, ingest_*).
	Readonly bool
}

// Server wraps a configured MCP server with its HTTP client.
type Server struct {
	opts   Options
	client *Client
	sdk    *mcpsdk.Server
}

// New builds a fully-configured MCP server with all tools and resources
// registered, but does not start the transport.
func New(opts Options) *Server {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.URL == "" {
		opts.URL = "http://localhost:8080"
	}
	client := NewClient(opts.URL, opts.Token, opts.Timeout)
	sdk := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "loveliness-mcp",
		Title:   "Loveliness",
		Version: Version,
	}, &mcpsdk.ServerOptions{
		Instructions: "Query, write, and introspect a Loveliness graph database. Use `schema` first to learn table shapes, then `cypher_read` for queries and `cypher_write` for mutations.",
	})

	cache := newSchemaCache(30 * time.Second)
	registerCypherTools(sdk, client, opts.Readonly)
	registerSchemaTool(sdk, client, cache)
	registerClusterTool(sdk, client)
	if !opts.Readonly {
		registerBulkTools(sdk, client)
		registerIngestTools(sdk, client)
	}
	registerResources(sdk, client, cache)

	return &Server{opts: opts, client: client, sdk: sdk}
}

// Run starts the MCP server on stdio and blocks until ctx is cancelled
// or the peer closes the connection.
func (s *Server) Run(ctx context.Context) error {
	return s.sdk.Run(ctx, &mcpsdk.StdioTransport{})
}

// SDK returns the underlying SDK server for advanced use (tests).
func (s *Server) SDK() *mcpsdk.Server {
	return s.sdk
}

// Client returns the underlying HTTP client (tests).
func (s *Server) Client() *Client {
	return s.client
}
