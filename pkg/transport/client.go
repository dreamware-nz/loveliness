package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// QueryRequest is the payload for internal shard queries between nodes.
//
// DeadlineNanos carries the caller's absolute deadline as a Unix
// nanosecond timestamp. Zero means "no deadline" — that's the legacy
// behaviour and what an old (pre-#84) client emits. New servers honor
// the field when non-zero (see TCPServer.handleConn); old servers
// simply ignore the unknown field thanks to msgpack tolerance, so the
// addition is v1-additive and does not require a frame version bump.
// Co-designed with #85 (whole-query retry budget): retries scheduled
// after the propagated deadline must not be issued.
//
// RequestID is the per-RPC correlation ID added in #86 (also
// v1-additive). The client allocates a fresh ID per call from an
// atomic counter; the server logs it on entry and echoes it back on
// the response so any partial-failed scatter can be pivoted from a
// metric to the specific TCP frames involved. Zero means "unset",
// which is what old clients emit; new peers always populate non-zero.
type QueryRequest struct {
	ShardID       int    `json:"shard_id" msgpack:"shard_id"`
	Cypher        string `json:"cypher" msgpack:"cypher"`
	DeadlineNanos uint64 `json:"deadline_nanos,omitempty" msgpack:"deadline_nanos,omitempty"`
	RequestID     uint64 `json:"request_id,omitempty" msgpack:"request_id,omitempty"`
}

// QueryResponse is the result of an internal shard query.
//
// RequestID echoes the value from the originating QueryRequest so the
// client can correlate the response with its in-flight call site and
// any per-call logging. Zero on responses from old (pre-#86) servers.
type QueryResponse struct {
	Columns []string         `json:"columns" msgpack:"columns"`
	Rows    []map[string]any `json:"rows" msgpack:"rows"`
	Stats   struct {
		CompileTimeMs float64 `json:"compile_time_ms,omitempty" msgpack:"compile_time_ms,omitempty"`
		ExecTimeMs    float64 `json:"exec_time_ms,omitempty" msgpack:"exec_time_ms,omitempty"`
	} `json:"stats,omitempty" msgpack:"stats,omitempty"`
	Error     string `json:"error,omitempty" msgpack:"error,omitempty"`
	RequestID uint64 `json:"request_id,omitempty" msgpack:"request_id,omitempty"`
}

// Client manages connections to peer nodes for internal query forwarding.
// It prefers TCP+msgpack when a TCP address is registered, falling back
// to HTTP+JSON for backwards compatibility.
type Client struct {
	mu       sync.RWMutex
	clients  map[string]*http.Client // nodeID → HTTP client
	addrs    map[string]string       // nodeID → HTTP address
	timeout  time.Duration
	tcpPool  *TCPPool
}

// NewClient creates a transport client with the given timeout.
func NewClient(timeout time.Duration) *Client {
	return &Client{
		clients: make(map[string]*http.Client),
		addrs:   make(map[string]string),
		timeout: timeout,
		tcpPool: NewTCPPool(4, timeout),
	}
}

// SetPeer registers or updates a peer node's HTTP address.
func (c *Client) SetPeer(nodeID, httpAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrs[nodeID] = httpAddr
	if _, ok := c.clients[nodeID]; !ok {
		c.clients[nodeID] = &http.Client{
			Timeout: c.timeout,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
}

// SetTLS configures mTLS for outbound TCP connections.
func (c *Client) SetTLS(cfg *tls.Config) {
	c.tcpPool.SetTLS(cfg)
}

// SetKeepalive configures the proactive ping/pong cadence on the
// underlying TCP pool (#87). Pass interval <= 0 to keep the worker
// dormant; the default interval set in NewTCPPool is 15s.
func (c *Client) SetKeepalive(interval, pongWait time.Duration) {
	c.tcpPool.SetKeepalive(interval, pongWait)
}

// KeepaliveSnapshot returns the closed-set counters that feed the
// loveliness_transport_keepalive_total{outcome} metric.
func (c *Client) KeepaliveSnapshot() KeepaliveSnapshot {
	return c.tcpPool.KeepaliveSnapshot()
}

// SetPeerTCP registers a peer's TCP transport address for msgpack comms.
func (c *Client) SetPeerTCP(nodeID, tcpAddr string) {
	c.tcpPool.SetPeer(nodeID, tcpAddr)
}

// RemovePeer removes a peer node from both HTTP and TCP pools.
func (c *Client) RemovePeer(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.addrs, nodeID)
	delete(c.clients, nodeID)
	c.tcpPool.RemovePeer(nodeID)
}

// QueryRemote sends a Cypher query to a specific shard on a remote node.
// Prefers TCP+msgpack when available, falls back to HTTP+JSON.
//
// Equivalent to QueryRemoteCtx with context.Background() — i.e. no
// deadline is propagated to the remote shard. Kept for callers that
// have no context (e.g. background replication fanout); prefer the
// Ctx variant from any path that already carries a context.
func (c *Client) QueryRemote(nodeID string, shardID int, cypher string) (*QueryResponse, error) {
	return c.QueryRemoteCtx(context.Background(), nodeID, shardID, cypher)
}

// QueryRemoteCtx is the context-aware variant of QueryRemote. The
// context's deadline (if any) is encoded into the wire request as
// DeadlineNanos so the remote server can honor the same wallclock —
// this is what makes #84's deadline propagation actually work.
// Cancellation of ctx aborts the local wait but does not (yet) tell
// the remote shard to stop; the remote side's deadline-honoring path
// in TCPServer.handleConn is what bounds the remote's blast radius.
func (c *Client) QueryRemoteCtx(ctx context.Context, nodeID string, shardID int, cypher string) (*QueryResponse, error) {
	// Try TCP first.
	c.tcpPool.mu.RLock()
	_, hasTCP := c.tcpPool.peers[nodeID]
	c.tcpPool.mu.RUnlock()

	if hasTCP {
		return c.tcpPool.QueryRemoteTCPCtx(ctx, nodeID, shardID, cypher)
	}

	// Fall back to HTTP+JSON.
	return c.queryRemoteHTTP(nodeID, shardID, cypher)
}

// queryRemoteHTTP is the original HTTP+JSON transport path.
func (c *Client) queryRemoteHTTP(nodeID string, shardID int, cypher string) (*QueryResponse, error) {
	c.mu.RLock()
	addr, ok := c.addrs[nodeID]
	client, hasClient := c.clients[nodeID]
	c.mu.RUnlock()

	if !ok || !hasClient {
		return nil, fmt.Errorf("unknown peer node: %s", nodeID)
	}

	reqBody, err := json.Marshal(QueryRequest{ShardID: shardID, Cypher: cypher})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/query", addr)
	resp, err := client.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("forward to %s: %w", nodeID, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response from %s: %w", nodeID, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote query on %s returned %d: %s", nodeID, resp.StatusCode, string(body))
	}

	var qr QueryResponse
	if err := json.Unmarshal(body, &qr); err != nil {
		return nil, fmt.Errorf("unmarshal response from %s: %w", nodeID, err)
	}
	if qr.Error != "" {
		return nil, fmt.Errorf("remote shard error on %s: %s", nodeID, qr.Error)
	}

	return &qr, nil
}

// Close shuts down the TCP pool.
func (c *Client) Close() {
	c.tcpPool.Close()
}
