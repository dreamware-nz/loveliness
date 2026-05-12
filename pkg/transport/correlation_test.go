package transport

import (
	"strings"
	"testing"
	"time"
)

// TestCorrelationID_RoundtripOnSuccess verifies #86's central
// invariant: the client's RequestID arrives at the server unchanged
// and the server echoes it back on the success response. Lets logs
// on both sides pivot to the same RPC.
func TestCorrelationID_RoundtripOnSuccess(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 2*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	// First RPC gets request_id=1 (atomic counter), second gets 2, …
	resp1, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("rpc 1 failed: %v", err)
	}
	resp2, err := pool.QueryRemoteTCP("node-tcp", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("rpc 2 failed: %v", err)
	}

	if resp1.RequestID == 0 {
		t.Error("rpc 1 RequestID should be non-zero")
	}
	if resp2.RequestID == 0 {
		t.Error("rpc 2 RequestID should be non-zero")
	}
	if resp1.RequestID == resp2.RequestID {
		t.Errorf("RequestIDs collided: both %d", resp1.RequestID)
	}
	if resp2.RequestID <= resp1.RequestID {
		t.Errorf("RequestID not monotonic: %d → %d", resp1.RequestID, resp2.RequestID)
	}
}

// TestCorrelationID_EchoedOnError verifies the RequestID rides every
// response path, not just success — so log diving from a metric →
// log → connection state is mechanical even on the error path.
// Drives the server's synchronous "unknown shard" branch which
// echoes the RequestID into MsgError.
func TestCorrelationID_EchoedOnError(t *testing.T) {
	srv, addr := setupTCPServer(t)
	defer srv.Stop()

	pool := NewTCPPool(2, 2*time.Second)
	defer pool.Close()
	pool.SetPeer("node-tcp", addr)

	// Shard 99 isn't hosted → server returns MsgError synchronously.
	_, err := pool.QueryRemoteTCP("node-tcp", 99, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for unknown shard")
	}
	// The error message embeds the client-side request_id so logs
	// can pivot to the same RPC.
	if got := err.Error(); !strings.Contains(got, "request_id=") {
		t.Errorf("error should include request_id=N, got: %v", err)
	}
}
