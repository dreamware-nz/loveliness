package router

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// budgetTestRemote is a RemoteQuerier whose behaviour per attempt is
// driven by an atomic counter so we can exercise budget enforcement
// across concurrent scatter goroutines deterministically. It always
// returns a retryable error so retryRemoteCall keeps reaching for
// the budget.
type budgetTestRemote struct {
	calls atomic.Int64
}

func (b *budgetTestRemote) QueryRemoteShard(_ string, _ int, _ string) (*shard.QueryResponse, error) {
	b.calls.Add(1)
	return nil, io.ErrUnexpectedEOF
}

// TestRetryBudget_Reserve_RespectsAttemptCap exercises the
// primitive's attempt accounting without going through the router.
// 5 reservations against a cap of 3 should grant exactly 3.
func TestRetryBudget_Reserve_RespectsAttemptCap(t *testing.T) {
	b := newRetryBudget(3, 0)
	grants := 0
	for i := 0; i < 5; i++ {
		if b.reserve() {
			grants++
		}
	}
	if grants != 3 {
		t.Errorf("granted = %d, want 3", grants)
	}
	if b.attemptCount() < 3 {
		t.Errorf("attemptCount = %d, want >= 3", b.attemptCount())
	}
}

// TestRetryBudget_Reserve_RespectsWallclockCap shows the wallclock
// cap stops reservations even when the count cap hasn't fired.
func TestRetryBudget_Reserve_RespectsWallclockCap(t *testing.T) {
	b := newRetryBudget(0, 10*time.Millisecond)
	// First reserve immediately.
	if !b.reserve() {
		t.Fatal("first reserve should succeed")
	}
	// Sleep past the wallclock cap.
	time.Sleep(15 * time.Millisecond)
	if b.reserve() {
		t.Error("reserve after wallclock expired should fail")
	}
}

// TestRetryBudget_NilSafe ensures the budget is fully nil-safe so
// callers that haven't configured a budget (or use the legacy
// retryRemoteCall path) keep working.
func TestRetryBudget_NilSafe(t *testing.T) {
	var b *retryBudget
	if !b.reserve() {
		t.Error("nil budget should grant all reservations")
	}
	if b.attemptCount() != 0 {
		t.Errorf("nil budget attemptCount = %d, want 0", b.attemptCount())
	}
	// withRetryBudget(nil) should return the original ctx unchanged.
	ctx := context.Background()
	if got := withRetryBudget(ctx, nil); got != ctx {
		t.Error("withRetryBudget(nil) should return original ctx")
	}
}

// TestRetryRemoteCall_BudgetExhausted verifies that the retry loop
// stops issuing RPCs once the shared budget is gone, even when each
// per-shard retry policy would allow more attempts. The exhausted
// error must be matchable via errors.Is(errBudgetExhausted).
func TestRetryRemoteCall_BudgetExhausted(t *testing.T) {
	fr := &flakyRemote{failures: 1000, err: io.ErrUnexpectedEOF}
	budget := newRetryBudget(3, 0)
	ctx := withRetryBudget(context.Background(), budget)

	// maxRetries=10 would normally yield 11 attempts; the budget
	// caps it at 3.
	_, err := retryRemoteCall(ctx, 10, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if err == nil {
		t.Fatal("expected budget-exhausted error, got nil")
	}
	if !errors.Is(err, errBudgetExhausted) {
		t.Errorf("expected errBudgetExhausted, got %v", err)
	}
	if fr.Calls() != 3 {
		t.Errorf("attempts = %d, want 3 (budget cap)", fr.Calls())
	}
}

// TestRetryRemoteCall_BudgetSharedAcrossCalls demonstrates the budget
// is global, not per-call: two retryRemoteCall invocations share the
// same budget, and the second can be denied even if the first didn't
// fully exhaust it.
func TestRetryRemoteCall_BudgetSharedAcrossCalls(t *testing.T) {
	fr := &flakyRemote{failures: 1000, err: io.ErrUnexpectedEOF}
	budget := newRetryBudget(4, 0)
	ctx := withRetryBudget(context.Background(), budget)

	// First call: 3 attempts (initial + 2 retries) → 3 of 4 used.
	_, _ = retryRemoteCall(ctx, 2, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	// Second call: only 1 attempt of budget remains.
	_, err := retryRemoteCall(ctx, 2, time.Microsecond, func() (*shard.QueryResponse, error) {
		return fr.QueryRemoteShard("peer", 0, "q")
	})
	if !errors.Is(err, errBudgetExhausted) {
		t.Errorf("expected second call to exhaust budget, got %v", err)
	}
	if fr.Calls() != 4 {
		t.Errorf("attempts = %d, want 4 (budget total)", fr.Calls())
	}
}

// TestRouter_ScatterRetryBudget_PerQuery is the integration check.
// Configure the router with a small budget; every shard returns a
// retryable error; total RPCs must equal the budget, NOT the
// (shardCount × (1+retries)) the naive multiplication would yield.
// This is the central #85 acceptance.
func TestRouter_ScatterRetryBudget_PerQuery(t *testing.T) {
	// 8 shards, 2 retries each → naive cap would be 24 RPCs.
	// Set budget to 6.
	shardCount := 8
	shards := make([]*shard.Shard, shardCount)
	r := NewRouter(shards, 500*time.Millisecond)
	br := &budgetTestRemote{}
	r.SetRemoteTransport("n1", br, &allToOnePlacement{node: "n1"})
	r.SetRemoteRetries(2)
	r.SetRemoteRetryBackoff(time.Microsecond)
	r.SetRetryBudget(6, 0)

	// Use a scatter-shaped query (no shard key → needs scatter).
	res, err := r.Execute(context.Background(), "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("Execute returned err: %v (partial allowed)", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if !res.Partial {
		t.Error("expected partial result (all shards failed)")
	}
	if got := br.calls.Load(); got > 6 {
		t.Errorf("total RPCs = %d, want <= 6 (budget cap)", got)
	}
	// At least one error should be budget-exhausted (the per-shard
	// goroutine that hit the budget first surfaces this).
	foundBudgetErr := false
	for _, e := range res.Errors {
		if strings.Contains(e.Error, "retry budget exhausted") {
			foundBudgetErr = true
			break
		}
	}
	if !foundBudgetErr {
		t.Errorf("expected at least one budget-exhausted error in res.Errors, got: %+v", res.Errors)
	}
	// Metric should record the outcome exactly once.
	snap := r.metrics.Snapshot()
	foundMetric := false
	for _, s := range snap.Retries {
		if s.Outcome == RetryOutcomeBudgetExhausted && s.Count == 1 {
			foundMetric = true
			break
		}
	}
	if !foundMetric {
		t.Errorf("expected budget_exhausted retry-outcome metric, got: %+v", snap.Retries)
	}
}

// TestRouter_ScatterRetryBudget_DeadlineBeatsBudget covers the
// co-design point with #84: a deadline that fires before the budget
// would still cap retries, and the metric outcome is
// deadline_exceeded, not budget_exhausted.
func TestRouter_ScatterRetryBudget_DeadlineBeatsBudget(t *testing.T) {
	shardCount := 4
	shards := make([]*shard.Shard, shardCount)
	// Very tight router timeout — scatter ctx deadline will fire
	// before we get anywhere near a 100-attempt budget.
	r := NewRouter(shards, 30*time.Millisecond)
	br := &budgetTestRemote{}
	r.SetRemoteTransport("n1", br, &allToOnePlacement{node: "n1"})
	r.SetRemoteRetries(10)
	r.SetRemoteRetryBackoff(10 * time.Millisecond) // each retry adds wallclock
	r.SetRetryBudget(100, 0)

	res, _ := r.Execute(context.Background(), "MATCH (n) RETURN n")
	if res == nil {
		t.Fatal("nil result")
	}
	if !res.Partial {
		t.Error("expected partial result (deadline expired)")
	}
	// Total RPCs should be well under 100 — backoffs eat the deadline.
	if got := br.calls.Load(); got >= 100 {
		t.Errorf("total RPCs = %d, want < 100 (deadline cap)", got)
	}
}

// allToOnePlacement: every shard goes to the named node.
type allToOnePlacement struct{ node string }

func (p *allToOnePlacement) PrimaryForShard(_ int) string { return p.node }
