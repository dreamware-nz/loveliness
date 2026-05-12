package router

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Whole-query retry budget (#85).
//
// A scatter-gather over N shards with per-shard retries can issue up
// to N * (1+remoteRetries) RPCs for a single user query. On a
// flapping peer that's a self-DOS waiting for a bad day. The retry
// budget bounds the total amplification across the whole query, not
// just per shard, by capping two things at the router level:
//
//   1. maxAttempts — total RPCs attempted across the scatter
//      (successful + failed). Once exhausted, no further retries
//      fire even if individual shards still have per-shard retries
//      remaining.
//   2. maxWallclock — total wallclock spent on retries (jittered
//      backoff + per-RPC time). When zero, the inherited ctx
//      deadline is the only wallclock cap. When non-zero, whichever
//      of {ctx deadline, maxWallclock} fires first stops retries.
//
// The budget is attached to the scatter context and consulted by
// retryRemoteCall before each attempt. Co-designed with #84:
// retryRemoteCall already returns on ctx.Done(), so a retry past
// the propagated deadline cannot fire even if the budget hasn't
// reached zero — the "no retries after deadline" rule from #84's
// co-design note is enforced by composition.
//
// The closed set of outcomes is mirrored in the metric labels.

// retryBudget is the shared accounting attached to one scatter.
// Methods are safe for concurrent use; the per-attempt slot is
// allocated atomically so a 32-shard scatter doesn't need to
// serialise on a mutex to consult it.
type retryBudget struct {
	maxAttempts  int64
	maxWallclock time.Duration
	start        time.Time
	attempts     atomic.Int64
}

// newRetryBudget initialises a budget for one query.
//
// maxAttempts <= 0 disables the count cap (back-compat with code
// that doesn't configure a budget — current behaviour).
// maxWallclock <= 0 disables the wallclock cap (rely solely on the
// inherited ctx deadline).
func newRetryBudget(maxAttempts int, maxWallclock time.Duration) *retryBudget {
	return &retryBudget{
		maxAttempts:  int64(maxAttempts),
		maxWallclock: maxWallclock,
		start:        time.Now(),
	}
}

// reserve atomically reserves one slot. Returns true if the slot was
// granted, false if either cap is exhausted. Called by retryRemoteCall
// before each attempt (initial included), so the count covers
// successful and failed attempts alike — matching Envoy's retry_budget
// model where the budget is a global RPC cap, not a "retries only"
// cap.
func (b *retryBudget) reserve() bool {
	if b == nil {
		return true
	}
	if b.maxWallclock > 0 && time.Since(b.start) >= b.maxWallclock {
		return false
	}
	if b.maxAttempts > 0 {
		// Optimistic increment; if it overshoots, the next caller
		// sees attempts > maxAttempts and is denied. Slight overshoot
		// (one extra slot) is acceptable; serialising is not.
		if b.attempts.Add(1) > b.maxAttempts {
			return false
		}
		return true
	}
	// No count cap configured.
	b.attempts.Add(1)
	return true
}

// attemptCount returns the current attempt count. For tests + metrics.
func (b *retryBudget) attemptCount() int64 {
	if b == nil {
		return 0
	}
	return b.attempts.Load()
}

// retryBudgetCtxKey is the context key for the shared budget.
// Unexported to keep callers honest — only the router attaches one.
type retryBudgetCtxKey struct{}

func withRetryBudget(ctx context.Context, b *retryBudget) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, retryBudgetCtxKey{}, b)
}

func retryBudgetFromCtx(ctx context.Context) *retryBudget {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(retryBudgetCtxKey{}); v != nil {
		if b, ok := v.(*retryBudget); ok {
			return b
		}
	}
	return nil
}

// errBudgetExhausted is the sentinel returned when retryRemoteCall
// stops because the whole-query budget is gone. Callers can match it
// with errors.Is to distinguish "budget exhausted" from "deadline
// exceeded" or any per-shard error — that distinction drives the
// metrics outcome label and a future user-facing error class.
var errBudgetExhausted = fmt.Errorf("retry budget exhausted")

// budgetExhaustedError wraps the sentinel with the attempt count for
// diagnostics. Use errors.Is(err, errBudgetExhausted) to detect.
type budgetExhaustedError struct {
	attempts int64
}

func (e *budgetExhaustedError) Error() string {
	return fmt.Sprintf("retry budget exhausted after %d attempts", e.attempts)
}
func (e *budgetExhaustedError) Unwrap() error { return errBudgetExhausted }

// Closed set of retry-outcome labels for the loveliness_router_retries_total
// counter (#85). One bump per query that retried (or attempted to).
const (
	RetryOutcomeSuccessAfterRetry = "success_after_retry"
	RetryOutcomeBudgetExhausted   = "budget_exhausted"
	RetryOutcomeDeadlineExceeded  = "deadline_exceeded"
)

// retryOutcomeCounters tracks the closed set of per-query retry
// outcomes. Cardinality is bounded by the three constants above.
type retryOutcomeCounters struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newRetryOutcomeCounters() *retryOutcomeCounters {
	return &retryOutcomeCounters{counts: make(map[string]uint64)}
}

func (c *retryOutcomeCounters) Inc(outcome string) {
	if outcome == "" {
		return
	}
	c.mu.Lock()
	c.counts[outcome]++
	c.mu.Unlock()
}

// RetryOutcomeSample is one (outcome, count) tuple for the metrics writer.
type RetryOutcomeSample struct {
	Outcome string
	Count   uint64
}

// Snapshot returns the counters sorted by outcome alphabetically.
func (c *retryOutcomeCounters) Snapshot() []RetryOutcomeSample {
	c.mu.Lock()
	out := make([]RetryOutcomeSample, 0, len(c.counts))
	for k, v := range c.counts {
		out = append(out, RetryOutcomeSample{Outcome: k, Count: v})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Outcome < out[j].Outcome })
	return out
}

// classifyRetryOutcome maps the post-scatter state to a single
// outcome label. Returns "" when no retrying happened (every shard
// succeeded on first attempt) — that's the happy path and not worth
// a counter bump.
//
// Precedence: budget_exhausted > deadline_exceeded > success_after_retry.
// "Budget exhausted" is the most specific failure mode and the one
// the operator most needs to see; "deadline exceeded" is next in line
// because it shapes capacity decisions; "success after retry" only
// fires when total attempts exceed shardCount with no errors,
// meaning at least one shard retried and ultimately succeeded.
func classifyRetryOutcome(errs []ShardError, attempts, shardCount int64) string {
	for _, e := range errs {
		// errBudgetExhausted is the sentinel returned by retryRemoteCall
		// when the budget hits zero. ShardError is a string-typed
		// surface; match the canonical prefix from budgetExhaustedError.
		if strings.Contains(e.Error, "retry budget exhausted") {
			return RetryOutcomeBudgetExhausted
		}
		// Accept both the British spelling the router writes for its
		// synthetic scatter-side timeout *and* the American spelling
		// Go's stdlib ctx.Err() emits — the latter is what surfaces
		// when a per-shard retryRemoteCall returns ctx.Err() directly
		// rather than the router's outer-select message.
		if strings.Contains(e.Error, "context cancelled") ||
			strings.Contains(e.Error, "context canceled") ||
			strings.Contains(e.Error, "context deadline exceeded") ||
			strings.Contains(e.Error, "scatter-gather timed out") {
			return RetryOutcomeDeadlineExceeded
		}
	}
	if len(errs) == 0 && attempts > shardCount {
		return RetryOutcomeSuccessAfterRetry
	}
	return ""
}
