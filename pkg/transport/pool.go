package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// connEntry is a pooled TCP connection.
type connEntry struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex // serializes request/response on this connection
}

// TCPPool manages persistent TCP connections to peer nodes.
type TCPPool struct {
	mu        sync.RWMutex
	peers     map[string]string       // nodeID → TCP address
	conns     map[string][]*connEntry  // nodeID → pooled connections
	poolSize  int
	timeout   time.Duration
	tlsConfig *tls.Config

	// requestSeq is the monotonic source for per-RPC correlation IDs
	// (#86). One counter per pool is fine — IDs only need to be
	// unique within a single client process so log correlation
	// works; cross-process uniqueness is not a goal (the (node, id)
	// pair is implicitly unique because each peer has its own pool).
	// Atomic 64-bit counter wraps at 2^64; effectively infinite.
	requestSeq atomic.Uint64
}

// NewTCPPool creates a connection pool.
func NewTCPPool(poolSize int, timeout time.Duration) *TCPPool {
	if poolSize < 1 {
		poolSize = 4
	}
	return &TCPPool{
		peers:    make(map[string]string),
		conns:    make(map[string][]*connEntry),
		poolSize: poolSize,
		timeout:  timeout,
	}
}

// SetTLS configures mTLS for outbound connections.
func (p *TCPPool) SetTLS(cfg *tls.Config) {
	p.tlsConfig = cfg
}

// SetPeer registers a peer's TCP address and pre-warms connections.
func (p *TCPPool) SetPeer(nodeID, tcpAddr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers[nodeID] = tcpAddr
	// Lazy connection — don't pre-warm, connect on first use.
}

// RemovePeer closes all connections to a peer and removes it.
func (p *TCPPool) RemovePeer(nodeID string) {
	p.mu.Lock()
	conns := p.conns[nodeID]
	delete(p.conns, nodeID)
	delete(p.peers, nodeID)
	p.mu.Unlock()

	for _, c := range conns {
		c.conn.Close()
	}
}

// QueryRemoteTCP sends a query to a remote shard over msgpack/TCP.
//
// Equivalent to QueryRemoteTCPCtx with context.Background(); kept for
// legacy callers. New code should plumb a context all the way through
// so the caller's deadline rides the wire to the remote shard (#84).
func (p *TCPPool) QueryRemoteTCP(nodeID string, shardID int, cypher string) (*QueryResponse, error) {
	return p.QueryRemoteTCPCtx(context.Background(), nodeID, shardID, cypher)
}

// QueryRemoteTCPCtx is the context-aware variant. The context's
// deadline (when set) is encoded into the wire QueryRequest as
// DeadlineNanos so the remote server can honor the same wallclock,
// and the per-RPC read/write deadlines are clamped to the context's
// deadline when it's tighter than p.timeout.
//
// Co-designed with #85: the per-RPC retry layer must consult the
// same deadline before scheduling another attempt; this method only
// owns the single-attempt path.
func (p *TCPPool) QueryRemoteTCPCtx(ctx context.Context, nodeID string, shardID int, cypher string) (*QueryResponse, error) {
	ce, err := p.getConn(nodeID)
	if err != nil {
		return nil, err
	}

	ce.mu.Lock()
	defer ce.mu.Unlock()

	req := QueryRequest{ShardID: shardID, Cypher: cypher}
	if dl, ok := ctx.Deadline(); ok {
		req.DeadlineNanos = uint64(dl.UnixNano())
	}
	// Per-RPC correlation ID (#86): zero-on-old-clients, monotonic
	// thereafter. Logged on both sides so a partial-failed scatter
	// can be pivoted from a metric to the specific TCP frames.
	req.RequestID = p.requestSeq.Add(1)
	slog.Debug("tcp rpc dispatched",
		"node", nodeID, "shard", shardID, "request_id", req.RequestID,
		"deadline_nanos", req.DeadlineNanos)

	// Per-RPC wire deadline: the tighter of pool timeout and ctx deadline.
	wireDeadline := time.Now().Add(p.timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(wireDeadline) {
		wireDeadline = dl
	}

	// Write request.
	ce.conn.SetWriteDeadline(wireDeadline)
	if err := WriteFrame(ce.writer, MsgQuery, req); err != nil {
		p.evict(nodeID, ce)
		return nil, fmt.Errorf("write to %s: %w", nodeID, err)
	}
	if err := ce.writer.Flush(); err != nil {
		p.evict(nodeID, ce)
		return nil, fmt.Errorf("flush to %s: %w", nodeID, err)
	}

	// Read response.
	ce.conn.SetReadDeadline(wireDeadline)
	msgType, payload, err := ReadFrame(ce.reader)
	if err != nil {
		p.evict(nodeID, ce)
		return nil, fmt.Errorf("read from %s: %w", nodeID, err)
	}

	var resp QueryResponse
	if err := Decode(payload, &resp); err != nil {
		return nil, fmt.Errorf("decode from %s: %w", nodeID, err)
	}

	if msgType == MsgError || resp.Error != "" {
		slog.Debug("tcp rpc error",
			"node", nodeID, "shard", shardID, "request_id", req.RequestID,
			"echoed_id", resp.RequestID, "err", resp.Error)
		return nil, fmt.Errorf("remote shard error on %s [request_id=%d]: %s", nodeID, req.RequestID, resp.Error)
	}

	slog.Debug("tcp rpc completed",
		"node", nodeID, "shard", shardID, "request_id", req.RequestID,
		"echoed_id", resp.RequestID, "rows", len(resp.Rows))

	return &resp, nil
}

// getConn returns a pooled connection, creating one if needed.
func (p *TCPPool) getConn(nodeID string) (*connEntry, error) {
	p.mu.RLock()
	conns := p.conns[nodeID]
	addr, ok := p.peers[nodeID]
	p.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown peer node: %s", nodeID)
	}

	// Try to find an unlocked connection.
	for _, ce := range conns {
		if ce.mu.TryLock() {
			ce.mu.Unlock()
			return ce, nil
		}
	}

	// All busy or none exist — create a new one if under pool size.
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.conns[nodeID]) >= p.poolSize {
		// Pool full, return the first one (will block on its mutex).
		return p.conns[nodeID][0], nil
	}

	ce, err := p.dial(addr)
	if err != nil {
		return nil, err
	}
	p.conns[nodeID] = append(p.conns[nodeID], ce)
	return ce, nil
}

func (p *TCPPool) dial(addr string) (*connEntry, error) {
	var conn net.Conn
	var err error
	if p.tlsConfig != nil {
		dialer := &tls.Dialer{
			Config: p.tlsConfig,
			NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}
	return &connEntry{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}, nil
}

func (p *TCPPool) evict(nodeID string, bad *connEntry) {
	bad.conn.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	conns := p.conns[nodeID]
	for i, c := range conns {
		if c == bad {
			p.conns[nodeID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
}

// Close shuts down all pooled connections.
func (p *TCPPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for nodeID, conns := range p.conns {
		for _, c := range conns {
			c.conn.Close()
		}
		delete(p.conns, nodeID)
	}
}
