package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// QueryRequest is the payload for internal shard queries between nodes.
type QueryRequest struct {
	ShardID int    `json:"shard_id"`
	Cypher  string `json:"cypher"`
}

// QueryResponse is the result of an internal shard query.
type QueryResponse struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   struct {
		CompileTimeMs float64 `json:"compile_time_ms,omitempty"`
		ExecTimeMs    float64 `json:"exec_time_ms,omitempty"`
	} `json:"stats,omitempty"`
	Error string `json:"error,omitempty"`
}

// Client manages HTTP connections to peer nodes for internal query forwarding.
type Client struct {
	mu      sync.RWMutex
	clients map[string]*http.Client // nodeID → HTTP client
	addrs   map[string]string       // nodeID → HTTP address
	timeout time.Duration
}

// NewClient creates a transport client with the given timeout.
func NewClient(timeout time.Duration) *Client {
	return &Client{
		clients: make(map[string]*http.Client),
		addrs:   make(map[string]string),
		timeout: timeout,
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

// RemovePeer removes a peer node.
func (c *Client) RemovePeer(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.addrs, nodeID)
	delete(c.clients, nodeID)
}

// QueryRemote sends a Cypher query to a specific shard on a remote node.
func (c *Client) QueryRemote(nodeID string, shardID int, cypher string) (*QueryResponse, error) {
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
