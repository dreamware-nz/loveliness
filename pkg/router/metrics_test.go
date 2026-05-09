package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// classifyRemoteError contract: every transport class we recognise
// in isRetryableRemoteError must also classify here, so the retry
// counter and the error counter are coherent. A protocol/query
// error must collapse to "other" so cardinality stays bounded.
func TestClassifyRemoteError_StableLabels(t *testing.T) {
	connRefused := &net.OpError{Op: "dial", Net: "tcp",
		Err: errors.New("connection refused")}
	connReset := &net.OpError{Op: "read", Net: "tcp",
		Err: errors.New("connection reset by peer")}
	brokenPipe := &net.OpError{Op: "write", Net: "tcp",
		Err: errors.New("broken pipe")}

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"context canceled", context.Canceled, "canceled"},
		{"deadline exceeded", context.DeadlineExceeded, "timeout"},
		{"deadline wrapped", fmt.Errorf("forward: %w", context.DeadlineExceeded), "timeout"},
		{"io.EOF", io.EOF, "eof"},
		{"unexpected EOF wrapped", fmt.Errorf("read frame: %w", io.ErrUnexpectedEOF), "eof"},
		{"connection refused", connRefused, "conn_refused"},
		{"connection refused wrapped", fmt.Errorf("forward: %w", connRefused), "conn_refused"},
		{"connection reset", connReset, "conn_reset"},
		{"broken pipe", brokenPipe, "broken_pipe"},
		{"protocol", errors.New("remote shard error: invalid query"), "other"},
		{"plain bug", errors.New("unmarshal failed"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyRemoteError(tc.err)
			if got != tc.want {
				t.Errorf("classifyRemoteError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// timeout-via-net.Error: anything reporting Timeout()=true (without
// an explicit context.DeadlineExceeded) must still bucket as
// "timeout". A user-defined net.Error proves we're not relying on
// the concrete *net.OpError type for that branch.
func TestClassifyRemoteError_NetTimeoutInterface(t *testing.T) {
	if got := classifyRemoteError(timeoutNetError{}); got != "timeout" {
		t.Errorf("net.Error.Timeout()=true should classify as timeout, got %q", got)
	}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

// RTT histogram: bucket cumulation must match the contract. An
// observation at 0.001 should land in 0.001 and every higher bucket;
// nothing below.
func TestRouterMetrics_RTTBucketCumulation(t *testing.T) {
	m := NewRouterMetrics()
	m.ObserveRemoteRTT(3, 0.001)

	snap := m.Snapshot().RTT
	if len(snap) != 1 || snap[0].ShardID != 3 {
		t.Fatalf("snapshot shape = %+v, want one entry for shard 3", snap)
	}
	for _, p := range snap[0].Buckets {
		// Every bucket >= 0.001 should have count 1; the +Inf
		// bucket is the synthetic total.
		if p.UpperBound >= 0.001 {
			if p.Count != 1 {
				t.Errorf("bucket %v count = %d, want 1", p.UpperBound, p.Count)
			}
		} else if p.Count != 0 {
			t.Errorf("bucket %v count = %d, want 0", p.UpperBound, p.Count)
		}
	}
	if snap[0].Count != 1 {
		t.Errorf("count = %d, want 1", snap[0].Count)
	}
	if snap[0].Sum != 0.001 {
		t.Errorf("sum = %v, want 0.001", snap[0].Sum)
	}

	// +Inf must always equal the total count — that's the histogram
	// contract Prometheus relies on.
	last := snap[0].Buckets[len(snap[0].Buckets)-1]
	if !math.IsInf(last.UpperBound, 1) {
		t.Errorf("last bucket upper bound = %v, want +Inf", last.UpperBound)
	}
	if last.Count != snap[0].Count {
		t.Errorf("+Inf bucket count = %d, want %d (== total count)", last.Count, snap[0].Count)
	}
}

// Negative durations (clock skew) clamp to zero. Otherwise a
// backwards clock would corrupt the cumulative invariant.
func TestRouterMetrics_RTTNegativeClampsToZero(t *testing.T) {
	m := NewRouterMetrics()
	m.ObserveRemoteRTT(0, -1.5)
	snap := m.Snapshot().RTT
	if len(snap) != 1 {
		t.Fatalf("expected one shard entry, got %d", len(snap))
	}
	if snap[0].Sum != 0 {
		t.Errorf("sum after clamp = %v, want 0", snap[0].Sum)
	}
	// First bucket (smallest upper bound) must already include the
	// clamped-to-zero observation.
	if snap[0].Buckets[0].Count != 1 {
		t.Errorf("first bucket count = %d, want 1", snap[0].Buckets[0].Count)
	}
}

// Snapshot ordering: shards must come out sorted ascending, errors
// alphabetical, bloom skips by shard. Stable order is what makes
// /metrics scrape-diff-friendly.
func TestRouterMetrics_SnapshotIsDeterministic(t *testing.T) {
	m := NewRouterMetrics()
	m.ObserveRemoteRTT(7, 0.01)
	m.ObserveRemoteRTT(2, 0.02)
	m.ObserveRemoteRTT(5, 0.03)
	m.IncRemoteError("conn_refused")
	m.IncRemoteError("timeout")
	m.IncRemoteError("eof")
	m.IncBloomSkip(9)
	m.IncBloomSkip(1)

	for i := 0; i < 5; i++ { // run a few times; map iteration is randomised
		snap := m.Snapshot()
		if got := []int{snap.RTT[0].ShardID, snap.RTT[1].ShardID, snap.RTT[2].ShardID}; got[0] != 2 || got[1] != 5 || got[2] != 7 {
			t.Errorf("RTT shard order = %v, want [2 5 7]", got)
		}
		if got := []string{snap.Errors[0].Code, snap.Errors[1].Code, snap.Errors[2].Code}; got[0] != "conn_refused" || got[1] != "eof" || got[2] != "timeout" {
			t.Errorf("Errors code order = %v, want [conn_refused eof timeout]", got)
		}
		if got := []int{snap.BloomSkips[0].ShardID, snap.BloomSkips[1].ShardID}; got[0] != 1 || got[1] != 9 {
			t.Errorf("BloomSkip shard order = %v, want [1 9]", got)
		}
	}
}

// Counters: empty codes are dropped (defence in depth — every
// classifyRemoteError path returns a non-empty code, so an empty
// here means a programming error upstream and we'd rather lose the
// observation than pollute the label set).
func TestRouterMetrics_ErrorEmptyCodeDropped(t *testing.T) {
	m := NewRouterMetrics()
	m.IncRemoteError("")
	if len(m.Snapshot().Errors) != 0 {
		t.Errorf("empty code should not produce a sample")
	}
}

// Concurrency: many goroutines hammering the same primitives must
// produce a consistent total (no lost increments under -race).
func TestRouterMetrics_ConcurrentSafe(t *testing.T) {
	m := NewRouterMetrics()
	const writers = 16
	const opsPerWriter = 250

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				m.ObserveRemoteRTT(0, 0.001)
				m.IncRemoteError("timeout")
				m.IncBloomSkip(0)
			}
		}()
	}
	wg.Wait()

	snap := m.Snapshot()
	want := uint64(writers * opsPerWriter)
	if snap.RTT[0].Count != want {
		t.Errorf("RTT count = %d, want %d", snap.RTT[0].Count, want)
	}
	if snap.Errors[0].Count != want {
		t.Errorf("error count = %d, want %d", snap.Errors[0].Count, want)
	}
	if snap.BloomSkips[0].Count != want {
		t.Errorf("bloom skip count = %d, want %d", snap.BloomSkips[0].Count, want)
	}
}

// Nil-safety: every method on (*RouterMetrics)(nil) must be a no-op
// so call sites (tests, or future code paths that bypass NewRouter)
// don't have to nil-check.
func TestRouterMetrics_NilSafe(t *testing.T) {
	var m *RouterMetrics
	m.ObserveRemoteRTT(0, 1.0)
	m.IncRemoteError("timeout")
	m.IncBloomSkip(0)
	m.observeRemote(0, time.Millisecond, errors.New("x"))
	snap := m.Snapshot()
	if snap.RTT != nil || snap.Errors != nil || snap.BloomSkips != nil {
		t.Errorf("nil snapshot must be empty, got %+v", snap)
	}
}

// End-to-end: a Router configured with a flaky remote should record
// the failed attempts in error counters and the (failed + final
// successful) attempts in the RTT histogram. Proves the metrics are
// wired into the real call site, not just the primitives.
func TestRouter_RemoteCallObservedInMetrics(t *testing.T) {
	const shardCount = 2
	r := NewRouter(make([]*shard.Shard, shardCount), 2*time.Second)

	connRefused := &net.OpError{Op: "dial", Net: "tcp",
		Err: errors.New("connection refused")}
	fr := &flakyRemote{failures: int32(shardCount), err: connRefused}
	r.SetRemoteTransport("local", fr, allRemotePlacement{})
	r.SetRemoteRetryBackoff(time.Microsecond)

	if _, err := r.Execute(context.Background(), "MATCH (n) RETURN n"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	snap := r.Metrics().Snapshot()
	// Two shards × (1 fail + 1 retry success) = 4 RTT samples spread
	// across two shard_id series.
	if len(snap.RTT) != shardCount {
		t.Fatalf("RTT series = %d, want %d", len(snap.RTT), shardCount)
	}
	var totalRTT uint64
	for _, s := range snap.RTT {
		totalRTT += s.Count
	}
	if totalRTT != uint64(2*shardCount) {
		t.Errorf("RTT total observations = %d, want %d", totalRTT, 2*shardCount)
	}
	// One conn_refused error per shard → counter at shardCount.
	if len(snap.Errors) != 1 || snap.Errors[0].Code != "conn_refused" {
		t.Fatalf("error snapshot = %+v, want one conn_refused entry", snap.Errors)
	}
	if snap.Errors[0].Count != uint64(shardCount) {
		t.Errorf("conn_refused count = %d, want %d", snap.Errors[0].Count, shardCount)
	}
}
