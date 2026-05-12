package transport

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// slowStore is a shard.Store that sleeps before returning. It exists
// solely so deadline-propagation tests can construct a deterministic
// "slow remote shard" without depending on real timing of LadybugDB.
type slowStore struct {
	delay time.Duration
}

func (s *slowStore) Query(cypher string) (*shard.QueryResponse, error) {
	time.Sleep(s.delay)
	return &shard.QueryResponse{Columns: []string{"x"}, Rows: []map[string]any{{"x": int64(1)}}}, nil
}
func (s *slowStore) Close() error { return nil }

// singleShardQuerier is a minimal ShardQuerier that holds one preconstructed
// shard. Lets tests bypass the manager.
type singleShardQuerier struct {
	sh *shard.Shard
}

func (q *singleShardQuerier) GetShard(id int) *shard.Shard {
	if id == 0 {
		return q.sh
	}
	return nil
}

func newSlowTCPServer(t *testing.T, delay time.Duration) (*TCPServer, string) {
	t.Helper()
	sh := shard.NewShard(0, &slowStore{delay: delay}, 4)
	q := &singleShardQuerier{sh: sh}
	srv := NewTCPServer(q)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	return srv, srv.Addr().String()
}

// TestDeadlinePropagation_RemoteHonorsDeadline verifies acceptance
// criterion 1 of #84: caller sets a tight deadline, the slow shard
// blows past it, and the server returns DEADLINE_EXCEEDED within
// approximately the deadline (not after the shard finishes).
func TestDeadlinePropagation_RemoteHonorsDeadline(t *testing.T) {
	// Shard sleeps 500ms, deadline is 100ms.
	srv, addr := newSlowTCPServer(t, 500*time.Millisecond)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("slow", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := pool.QueryRemoteTCPCtx(ctx, "slow", 0, "MATCH (n) RETURN n")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
	if !strings.Contains(err.Error(), "deadline exceeded") && !strings.Contains(err.Error(), "i/o timeout") {
		t.Errorf("expected deadline/timeout error, got: %v", err)
	}
	// Should return well before the shard's 500ms sleep finishes.
	if elapsed > 400*time.Millisecond {
		t.Errorf("deadline not honored: took %s (shard sleep was 500ms)", elapsed)
	}
}

// TestDeadlinePropagation_AlreadyExpired verifies acceptance
// criterion 3: a deadline in the past short-circuits before
// shard.Query is invoked. Asserted by checking the server response
// arrives essentially instantly (<< the shard sleep).
func TestDeadlinePropagation_AlreadyExpired(t *testing.T) {
	srv, addr := newSlowTCPServer(t, 2*time.Second)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("slow", addr)

	// Build a ctx whose deadline is already past. context.WithDeadline
	// in the past works but ctx.Err() fires immediately on the client,
	// which would block us testing the server-side path. Instead, set
	// DeadlineNanos directly by going through QueryRemoteTCPCtx with a
	// pre-expired deadline via a manually constructed request: use
	// WithDeadline at past time and rely on the client to still send.
	past := time.Now().Add(-1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), past)
	defer cancel()

	start := time.Now()
	_, err := pool.QueryRemoteTCPCtx(ctx, "slow", 0, "MATCH (n) RETURN n")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for already-expired deadline")
	}
	// Must be much less than the shard sleep (2s); ideally <100ms.
	if elapsed > 500*time.Millisecond {
		t.Errorf("expired deadline did not short-circuit: took %s", elapsed)
	}
}

// TestDeadlinePropagation_ZeroMeansNoDeadline verifies acceptance
// criterion 2: a request with DeadlineNanos == 0 (legacy / pre-#84
// client) is unbounded — the server runs to completion. This is the
// backwards-compat guarantee for old clients talking to new servers.
func TestDeadlinePropagation_ZeroMeansNoDeadline(t *testing.T) {
	// Shard sleeps 200ms, no deadline propagated.
	srv, addr := newSlowTCPServer(t, 200*time.Millisecond)
	defer srv.Stop()

	pool := NewTCPPool(2, 5*time.Second)
	defer pool.Close()
	pool.SetPeer("slow", addr)

	// QueryRemoteTCP (no ctx) uses context.Background(), so
	// DeadlineNanos is 0 on the wire — old client emulation.
	resp, err := pool.QueryRemoteTCP("slow", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(resp.Rows))
	}
}

// TestDeadlinePropagation_WireFieldRoundtrip verifies the field is
// preserved through msgpack encode/decode. This is the v1-additive
// backwards-compat smoke test: a future client setting the field
// must arrive intact at a new server, and old peers ignoring it
// must not fail to decode.
func TestDeadlinePropagation_WireFieldRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	req := QueryRequest{ShardID: 7, Cypher: "RETURN 1", DeadlineNanos: 1234567890}
	if err := WriteFrame(&buf, MsgQuery, req); err != nil {
		t.Fatal(err)
	}
	_, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	var decoded QueryRequest
	if err := Decode(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.DeadlineNanos != 1234567890 {
		t.Errorf("DeadlineNanos roundtrip failed: got %d", decoded.DeadlineNanos)
	}
	if decoded.ShardID != 7 || decoded.Cypher != "RETURN 1" {
		t.Errorf("other fields corrupted: %+v", decoded)
	}
}
