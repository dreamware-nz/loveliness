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

	// lastActivityNanos is the Unix-nanos timestamp of the most
	// recent successful frame read/write on this conn. Read in the
	// keepalive worker (#87) to decide whether to send a proactive
	// MsgPing; written under conn.mu by every RPC path that
	// touches the wire. Stored as an atomic so the worker can
	// snapshot without locking the RPC path.
	lastActivityNanos atomic.Int64
}

// touch stamps the conn's last-activity timestamp. Called from any
// path that successfully exchanges bytes with the peer.
func (c *connEntry) touch() {
	c.lastActivityNanos.Store(time.Now().UnixNano())
}

// idleSince reports how long the conn has been idle. Negative means
// "active right now"; callers should treat zero / negative as
// "not idle long enough to ping".
func (c *connEntry) idleSince(now time.Time) time.Duration {
	last := c.lastActivityNanos.Load()
	if last == 0 {
		return 0
	}
	return now.Sub(time.Unix(0, last))
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

	// Keepalive (#87): proactively ping idle conns so half-dead TCP
	// connections (FIN dropped, NAT timeout, peer kernel panic) are
	// evicted before a real RPC lands on them. Wire format already
	// has MsgPing/MsgPong; this just sequences them.
	//
	// The two duration fields are atomic.Int64 (nanoseconds) so the
	// keepalive worker can read them on every tick without locking
	// while SetKeepalive can swap them from any goroutine. A plain
	// time.Duration field would race under -race.
	keepaliveIntervalNs atomic.Int64
	keepalivePongWaitNs atomic.Int64
	keepaliveStop       chan struct{}
	keepaliveDone       chan struct{}
	keepaliveCounts     keepaliveCounters
}

// keepaliveCounters tracks the closed set of per-ping outcomes used
// for the loveliness_transport_keepalive_total{outcome} metric:
//
//	ok    — peer replied with MsgPong before keepalivePongWait.
//	miss  — pong did not arrive in time; conn was evicted.
//	error — write or read failed at the network layer.
//
// All three are atomic so the worker can update them without taking
// any pool lock. Snapshot returns a stable copy.
type keepaliveCounters struct {
	ok    atomic.Uint64
	miss  atomic.Uint64
	error atomic.Uint64
}

// KeepaliveSnapshot is the deterministic view consumed by the
// metrics writer in pkg/api.
type KeepaliveSnapshot struct {
	OK    uint64
	Miss  uint64
	Error uint64
}

// KeepaliveSnapshot returns the current keepalive outcome counts.
func (p *TCPPool) KeepaliveSnapshot() KeepaliveSnapshot {
	return KeepaliveSnapshot{
		OK:    p.keepaliveCounts.ok.Load(),
		Miss:  p.keepaliveCounts.miss.Load(),
		Error: p.keepaliveCounts.error.Load(),
	}
}

// NewTCPPool creates a connection pool.
//
// Keepalive (#87) is enabled by default at a 15s interval with a 5s
// pong timeout — modest enough that healthy LAN RTT is well under
// the bound, aggressive enough that a half-dead conn is evicted
// within ~20s of going bad. SetKeepalive overrides.
func NewTCPPool(poolSize int, timeout time.Duration) *TCPPool {
	if poolSize < 1 {
		poolSize = 4
	}
	p := &TCPPool{
		peers:         make(map[string]string),
		conns:         make(map[string][]*connEntry),
		poolSize:      poolSize,
		timeout:       timeout,
		keepaliveStop: make(chan struct{}),
		keepaliveDone: make(chan struct{}),
	}
	p.keepaliveIntervalNs.Store(int64(15 * time.Second))
	p.keepalivePongWaitNs.Store(int64(5 * time.Second))
	go p.keepaliveLoop()
	return p
}

// SetKeepalive tunes the proactive ping cadence and pong timeout.
// interval <= 0 disables keepalive (the worker keeps running but
// performs no pings until interval is set non-zero again). Stored
// atomically so it can be called from any goroutine.
func (p *TCPPool) SetKeepalive(interval, pongWait time.Duration) {
	p.keepaliveIntervalNs.Store(int64(interval))
	p.keepalivePongWaitNs.Store(int64(pongWait))
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
		return nil, fmt.Errorf("write to %s [request_id=%d]: %w", nodeID, req.RequestID, err)
	}
	if err := ce.writer.Flush(); err != nil {
		p.evict(nodeID, ce)
		return nil, fmt.Errorf("flush to %s [request_id=%d]: %w", nodeID, req.RequestID, err)
	}

	// Read response.
	ce.conn.SetReadDeadline(wireDeadline)
	msgType, payload, err := ReadFrame(ce.reader)
	if err != nil {
		p.evict(nodeID, ce)
		return nil, fmt.Errorf("read from %s [request_id=%d]: %w", nodeID, req.RequestID, err)
	}

	var resp QueryResponse
	if err := Decode(payload, &resp); err != nil {
		return nil, fmt.Errorf("decode from %s [request_id=%d]: %w", nodeID, req.RequestID, err)
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

	ce.touch()
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
	ce := &connEntry{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}
	ce.touch() // a fresh conn is "active now" so it's not pinged
	// instantly on the next keepalive tick.
	return ce, nil
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
	// Signal the keepalive worker to stop and wait for it before
	// closing conns — otherwise the worker can be mid-ping when
	// connections disappear underneath it.
	if p.keepaliveStop != nil {
		// Guard against double-close from repeated Close() calls.
		select {
		case <-p.keepaliveStop:
		default:
			close(p.keepaliveStop)
		}
		<-p.keepaliveDone
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for nodeID, conns := range p.conns {
		for _, c := range conns {
			c.conn.Close()
		}
		delete(p.conns, nodeID)
	}
}

// keepaliveLoop is the per-pool worker that proactively pings idle
// connections (#87). Runs until Close signals via keepaliveStop. The
// goal is to catch half-dead TCP connections (peer kernel panic, FIN
// dropped, NAT/firewall timeout) before a user RPC lands on one.
//
// Per-tick behaviour: for every connection idle longer than
// keepaliveInterval, attempt to take its mu (skip if busy — an
// in-flight RPC is its own liveness signal), send a MsgPing, and
// read a MsgPong within keepalivePongWait. Miss or transport error
// → evict via the existing eviction path so the next RPC dials a
// fresh conn.
func (p *TCPPool) keepaliveLoop() {
	defer close(p.keepaliveDone)
	// Re-read the interval every iteration via the atomic so a
	// SetKeepalive update takes effect on the very next tick rather
	// than waiting for an initial ticker period to elapse. Replaces
	// time.NewTicker for the same reason; time.After per iteration
	// is fine at the 1Hz-to-1mHz cadences keepalive runs at.
	//
	// When interval <= 0 (keepalive disabled) the worker still loops
	// on a 1s wakeup so it remains responsive to keepaliveStop and
	// can pick up a later SetKeepalive without restarting.
	for {
		interval := time.Duration(p.keepaliveIntervalNs.Load())
		if interval <= 0 {
			select {
			case <-p.keepaliveStop:
				return
			case <-time.After(time.Second):
			}
			continue
		}
		select {
		case <-p.keepaliveStop:
			return
		case <-time.After(interval):
			p.tickKeepalive(time.Now())
		}
	}
}

// tickKeepalive walks every pooled connection and pings the idle
// ones. Extracted from keepaliveLoop so tests can drive a single
// tick deterministically without relying on real wallclock.
func (p *TCPPool) tickKeepalive(now time.Time) {
	// Snapshot the conn map under the read lock so the ping work
	// happens outside the pool lock; eviction reacquires the lock
	// inside p.evict.
	type pair struct {
		nodeID string
		ce     *connEntry
	}
	interval := time.Duration(p.keepaliveIntervalNs.Load())
	if interval <= 0 {
		return
	}
	var idle []pair
	p.mu.RLock()
	for nodeID, conns := range p.conns {
		for _, ce := range conns {
			if ce.idleSince(now) >= interval {
				idle = append(idle, pair{nodeID, ce})
			}
		}
	}
	p.mu.RUnlock()

	for _, pr := range idle {
		p.pingOne(pr.nodeID, pr.ce)
	}
}

// pingOne sends a MsgPing on ce and waits for MsgPong. Skips the
// conn if it's busy (TryLock fails) — an in-flight RPC is its own
// liveness signal, so pinging on top of it would serialise behind
// the RPC and pointlessly delay both. On failure the conn is
// evicted via the same path used by RPC-side failures.
func (p *TCPPool) pingOne(nodeID string, ce *connEntry) {
	if !ce.mu.TryLock() {
		return // busy — not idle in any practical sense
	}
	defer ce.mu.Unlock()

	deadline := time.Now().Add(time.Duration(p.keepalivePongWaitNs.Load()))
	_ = ce.conn.SetWriteDeadline(deadline)
	if err := WriteFrame(ce.writer, MsgPing, nil); err != nil {
		p.keepaliveCounts.error.Add(1)
		slog.Debug("keepalive write failed", "node", nodeID, "err", err)
		p.evict(nodeID, ce)
		return
	}
	if err := ce.writer.Flush(); err != nil {
		p.keepaliveCounts.error.Add(1)
		slog.Debug("keepalive flush failed", "node", nodeID, "err", err)
		p.evict(nodeID, ce)
		return
	}
	_ = ce.conn.SetReadDeadline(deadline)
	msgType, _, err := ReadFrame(ce.reader)
	if err != nil {
		// Timeout on the pong is the canonical "half-dead conn"
		// signal — count separately from raw transport errors so
		// operators can tell apart "peer is gone" from "peer is
		// slow to respond". net.Error.Timeout() differentiates.
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			p.keepaliveCounts.miss.Add(1)
		} else {
			p.keepaliveCounts.error.Add(1)
		}
		slog.Debug("keepalive read failed", "node", nodeID, "err", err)
		p.evict(nodeID, ce)
		return
	}
	if msgType != MsgPong {
		p.keepaliveCounts.error.Add(1)
		slog.Debug("keepalive unexpected msg type", "node", nodeID, "type", msgType)
		p.evict(nodeID, ce)
		return
	}
	p.keepaliveCounts.ok.Add(1)
	ce.touch()
}
