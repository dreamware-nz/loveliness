package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// flakyRemote is a RemoteQuerier that fails the first n calls with a
// supplied error, then succeeds on every subsequent call. Used to
// exercise the retry loop without needing a real network failure.
//
// When failuresPerShard > 0, the shared `failures` counter is
// ignored and instead each shard ID gets its own independent
// failure budget of failuresPerShard attempts. This is what the
// end-to-end test needs to avoid a goroutine-scheduling-dependent
// distribution of the shared failure pool across shards (see
// TestRouter_RemoteRetry_EndToEnd — without per-shard tracking,
// one unlucky shard can absorb all failures and break the test).
type flakyRemote struct {
	failures         int32 // remaining failures to inject (atomic) — shared mode
	failuresPerShard int32 // when >0, per-shard mode (ignores `failures`)
	perShardCalls    sync.Map
	err              error
	calls            int32
	resp             *shard.QueryResponse
}

func (f *flakyRemote) QueryRemoteShard(_ string, shardID int, _ string) (*shard.QueryResponse, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.failuresPerShard > 0 {
		// Per-shard counter: load-or-init a *int32 for this shardID,
		// then increment. Fail while the count is <= failuresPerShard.
		v, _ := f.perShardCalls.LoadOrStore(shardID, new(int32))
		count := atomic.AddInt32(v.(*int32), 1)
		if count <= f.failuresPerShard {
			return nil, f.err
		}
	} else if atomic.AddInt32(&f.failures, -1) >= 0 {
		return nil, f.err
	}
	if f.resp == nil {
		return &shard.QueryResponse{Columns: []string{"x"}, Rows: []map[string]any{{"x": 1}}}, nil
	}
	return f.resp, nil
}

func (f *flakyRemote) Calls() int { return int(atomic.LoadInt32(&f.calls)) }

// dial-error fixture: net.OpError wrapping a "connection refused"
// message — what the real transport emits when a peer is down.
func connRefusedErr() error {
	return &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: errors.New("connection refused"),
	}
}

func TestIsRetryableRemoteError_Classification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"context wrapped canceled", fmt.Errorf("forward: %w", context.Canceled), false},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"deadline wrapped", fmt.Errorf("forward to peer: %w", context.DeadlineExceeded), true},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF wrapped", fmt.Errorf("read frame: %w", io.ErrUnexpectedEOF), true},
		{"connection refused", connRefusedErr(), true},
		{"connection refused wrapped", fmt.Errorf("forward: %w", connRefusedErr()), true},
		{"plain bug", errors.New("unmarshal failed"), false},
		{"protocol error", errors.New("remote shard error: invalid query"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableRemoteError(tc.err); got != tc.want {
				t.Errorf("isRetryableRemoteError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryRemoteCall_SucceedsWithinBudget(t *testing.T) {
	// Two transient failures, then success on the third attempt.
	// maxRetries=2 ⇒ 3 attempts total ⇒ should succeed.
	fr := &flakyRemote{failures: 2, err: io.ErrUnexpectedEOF}
	resp, err := retryRemoteCall(context.Background(), 2, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if err != nil {
		t.Fatalf("expected success after 2 retries, got %v", err)
	}
	if resp == nil {
		t.Fatal("nil response on success")
	}
	if fr.Calls() != 3 {
		t.Errorf("attempts = %d, want 3 (initial + 2 retries)", fr.Calls())
	}
}

func TestRetryRemoteCall_ExhaustsBudget(t *testing.T) {
	// Three transient failures, maxRetries=2 ⇒ 3 attempts ⇒ surfaces error.
	fr := &flakyRemote{failures: 3, err: io.ErrUnexpectedEOF}
	_, err := retryRemoteCall(context.Background(), 2, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected last-attempt error to surface, got %v", err)
	}
	if fr.Calls() != 3 {
		t.Errorf("attempts = %d, want 3", fr.Calls())
	}
}

func TestRetryRemoteCall_NonRetryableShortCircuits(t *testing.T) {
	// A plain bug should never retry. fr is set up so a retry would
	// succeed; we want to prove no retry happens.
	fr := &flakyRemote{failures: 1, err: errors.New("malformed response")}
	_, err := retryRemoteCall(context.Background(), 5, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if err == nil {
		t.Fatal("expected non-retryable error to surface, got nil")
	}
	if fr.Calls() != 1 {
		t.Errorf("non-retryable should short-circuit at 1 attempt, got %d", fr.Calls())
	}
}

func TestRetryRemoteCall_ZeroRetriesIsSingleAttempt(t *testing.T) {
	// maxRetries=0 ⇒ exactly one attempt (no retries).
	fr := &flakyRemote{failures: 1, err: io.ErrUnexpectedEOF}
	_, err := retryRemoteCall(context.Background(), 0, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if err == nil {
		t.Fatal("expected single attempt to fail, got nil")
	}
	if fr.Calls() != 1 {
		t.Errorf("attempts = %d, want 1 (no retries)", fr.Calls())
	}
}

func TestRetryRemoteCall_HonorsContextCancel(t *testing.T) {
	// Cancel the context mid-loop; the retry must abort and surface
	// the cancel error rather than the retryable error.
	fr := &flakyRemote{failures: 100, err: io.ErrUnexpectedEOF}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(2 * time.Millisecond)
		cancel()
	}()
	_, err := retryRemoteCall(ctx, 1000, 5*time.Millisecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestRouter_RemoteRetry_EndToEnd is the integration-flavored check:
// configure the router with retries and a flaky remote, prove the
// scatter result eventually succeeds despite transport flapping.
func TestRouter_RemoteRetry_EndToEnd(t *testing.T) {
	const shardCount = 4
	r := NewRouter(make([]*shard.Shard, shardCount), 2*time.Second)

	fr := &flakyRemote{failuresPerShard: 1, err: io.ErrUnexpectedEOF}
	r.SetRemoteTransport("local", fr, allRemotePlacement{})
	r.SetRemoteRetryBackoff(time.Microsecond) // race-free fast retries

	res, err := r.Execute(context.Background(), "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	// Each shard fails once and then succeeds on retry, so total calls
	// should be 2*shardCount and Partial should be false.
	if res.Partial {
		t.Errorf("expected non-partial result after successful retries, got %+v", res)
	}
	if got, want := fr.Calls(), 2*shardCount; got != want {
		t.Errorf("total remote attempts = %d, want %d (each shard once-failed, once-retried)", got, want)
	}
}

// TestRouter_RemoteRetry_Disabled confirms the SetRemoteRetries(0)
// override produces single-attempt behavior — the failure surfaces
// as a partial result on the first call, no retries.
func TestRouter_RemoteRetry_Disabled(t *testing.T) {
	const shardCount = 2
	r := NewRouter(make([]*shard.Shard, shardCount), 2*time.Second)

	fr := &flakyRemote{failures: int32(shardCount), err: io.ErrUnexpectedEOF}
	r.SetRemoteTransport("local", fr, allRemotePlacement{})
	r.SetRemoteRetries(0) // override the auto-installed default
	r.SetRemoteRetryBackoff(time.Microsecond)

	res, _ := r.Execute(context.Background(), "MATCH (n) RETURN n")
	if res == nil {
		t.Fatal("nil result")
	}
	if !res.Partial {
		t.Error("expected Partial=true with retries disabled and a failing remote")
	}
	if got, want := fr.Calls(), shardCount; got != want {
		t.Errorf("attempts with retries=0 = %d, want %d (one per shard, no retries)", got, want)
	}
}

func TestSetRemoteTransport_DefaultsRetryConfig(t *testing.T) {
	r := NewRouter(make([]*shard.Shard, 4), time.Second)
	r.SetRemoteTransport("local", &flakyRemote{}, allRemotePlacement{})
	if r.remoteRetries != defaultRemoteRetries {
		t.Errorf("remoteRetries default = %d, want %d", r.remoteRetries, defaultRemoteRetries)
	}
	if r.remoteRetryBackoff != defaultRemoteRetryBackoff {
		t.Errorf("remoteRetryBackoff default = %v, want %v", r.remoteRetryBackoff, defaultRemoteRetryBackoff)
	}
}
