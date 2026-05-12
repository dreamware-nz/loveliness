package transport

import (
	"net"
	"testing"
	"time"
)

// TestKeepalive_PingsIdleConn proves a freshly-pooled conn with no
// RPC traffic gets a proactive ping after the keepalive interval and
// the OK counter ticks up.
func TestKeepalive_PingsIdleConn(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 2*time.Second)
	defer pool.Close()
	// Very fast keepalive so the test stays under a second.
	pool.SetKeepalive(20*time.Millisecond, 200*time.Millisecond)
	pool.SetPeer("node-tcp", addr)

	// Run one real RPC to seed a conn.
	if _, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n"); err != nil {
		t.Fatalf("seed rpc: %v", err)
	}

	// Wait long enough for at least two keepalive ticks beyond the
	// interval — the conn is idle the whole time.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pool.KeepaliveSnapshot().OK > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	snap := pool.KeepaliveSnapshot()
	if snap.OK == 0 {
		t.Errorf("expected at least one ok keepalive, got %+v", snap)
	}
	if snap.Miss > 0 || snap.Error > 0 {
		t.Errorf("expected no misses or errors on healthy conn, got %+v", snap)
	}
}

// silentListener accepts TCP conns but never reads or writes,
// simulating a half-dead peer (FIN dropped, peer kernel panic, NAT
// timeout). The keepalive worker should send a MsgPing and time out
// waiting for the pong, count one "miss", and evict the conn.
type silentListener struct {
	ln net.Listener
}

func newSilentListener(t *testing.T) *silentListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sl := &silentListener{ln: ln}
	go sl.acceptLoop()
	return sl
}

func (s *silentListener) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		// Hold the conn open but never read / never write: any
		// frame the peer sends sits unread in the kernel buffer
		// until our SetReadDeadline fires on the client.
		_ = conn
	}
}

func (s *silentListener) Addr() string { return s.ln.Addr().String() }
func (s *silentListener) Close()       { s.ln.Close() }

// TestKeepalive_EvictsHalfDeadConn proves the worker actually evicts
// a conn whose pong never comes back, and that the miss counter
// reflects the outcome.
func TestKeepalive_EvictsHalfDeadConn(t *testing.T) {
	sl := newSilentListener(t)
	defer sl.Close()

	pool := NewTCPPool(2, 2*time.Second)
	defer pool.Close()
	pool.SetKeepalive(20*time.Millisecond, 50*time.Millisecond)
	pool.SetPeer("silent", sl.Addr())

	// Force a conn into the pool — dial directly because the
	// silent peer never responds to a real RPC either.
	ce, err := pool.getConn("silent")
	if err != nil {
		t.Fatalf("getConn: %v", err)
	}
	// Pretend the conn has been idle for ages so the keepalive
	// worker pings it on the next tick. (touch was called at dial
	// time; rewind it.)
	ce.lastActivityNanos.Store(time.Now().Add(-time.Second).UnixNano())

	// Wait until both the miss counter ticks AND the conn drops out
	// of the pool. pingOne increments the counter just before calling
	// evict, so polling on the counter alone is racy under -race.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		pool.mu.RLock()
		remaining := len(pool.conns["silent"])
		pool.mu.RUnlock()
		if pool.KeepaliveSnapshot().Miss > 0 && remaining == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	snap := pool.KeepaliveSnapshot()
	if snap.Miss == 0 {
		t.Errorf("expected at least one miss keepalive, got %+v", snap)
	}
	// Conn should be evicted from the pool.
	pool.mu.RLock()
	remaining := len(pool.conns["silent"])
	pool.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("expected conn evicted after miss, %d remain", remaining)
	}
}

// TestKeepalive_SkipsBusyConn proves a conn with an in-flight RPC
// (mu held) is not pinged — its in-flight RPC is its own liveness
// signal, so layering a ping on top would just serialise behind the
// RPC and pointlessly delay both.
func TestKeepalive_SkipsBusyConn(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 2*time.Second)
	defer pool.Close()
	pool.SetKeepalive(10*time.Millisecond, 50*time.Millisecond)
	pool.SetPeer("node-tcp", addr)

	// Seed a conn.
	if _, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n"); err != nil {
		t.Fatalf("seed rpc: %v", err)
	}

	// Hold the conn's mutex to simulate an in-flight RPC, and
	// force it to look idle. The keepalive worker should TryLock,
	// fail, and skip the conn entirely.
	pool.mu.RLock()
	ce := pool.conns["node-tcp"][0]
	pool.mu.RUnlock()
	ce.lastActivityNanos.Store(time.Now().Add(-time.Second).UnixNano())
	ce.mu.Lock()
	defer ce.mu.Unlock()

	startSnap := pool.KeepaliveSnapshot()
	time.Sleep(200 * time.Millisecond)
	endSnap := pool.KeepaliveSnapshot()

	if endSnap.OK > startSnap.OK || endSnap.Miss > startSnap.Miss || endSnap.Error > startSnap.Error {
		t.Errorf("busy conn was pinged: start=%+v end=%+v", startSnap, endSnap)
	}
}
