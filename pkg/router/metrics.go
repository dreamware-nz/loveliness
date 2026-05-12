package router

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// Per-router observability primitives for the cross-node scatter
// path. Hand-rolled to match pkg/api/query_metrics.go and avoid
// dragging the prometheus client_golang dependency into this package.
//
// Four series are exposed:
//
//	loveliness_router_remote_rtt_seconds{shard_id}     histogram
//	loveliness_router_remote_errors_total{code}        counter
//	loveliness_router_bloom_skip_total{shard_id}       counter
//	loveliness_router_retries_total{outcome}           counter (#85)
//
// All are populated lazily — a series only materialises after the
// first observation, matching client_golang's create-on-first-use
// semantics. The shard_id label is bounded by shardCount; the code
// label is bounded by the closed set in classifyRemoteError; the
// outcome label is bounded by the closed set in retryOutcome.

// remoteRTTBuckets are the histogram bucket upper bounds in seconds
// for per-shard RPC round-trip times. The lower end (250µs) covers
// in-process loopback; the upper end (5s) brackets the worst-case
// scatter timeout in production. The intermediate steps are sized to
// give meaningful resolution across the 1ms–1s decade where most
// real cross-node calls land.
var remoteRTTBuckets = []float64{
	0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
	0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
}

// RouterMetrics owns the three series above. Pointer-receiver methods
// are safe to call from any goroutine. A nil *RouterMetrics is a
// no-op for every method, so call sites don't need to nil-check.
type RouterMetrics struct {
	rtt        *remoteRTTHistogram
	errors     *remoteErrorCounters
	bloomSkips *bloomSkipCounters
	retries    *retryOutcomeCounters
}

// NewRouterMetrics constructs an empty metrics container. Buckets and
// keyed maps are populated on first observation.
func NewRouterMetrics() *RouterMetrics {
	return &RouterMetrics{
		rtt:        newRemoteRTTHistogram(),
		errors:     newRemoteErrorCounters(),
		bloomSkips: newBloomSkipCounters(),
		retries:    newRetryOutcomeCounters(),
	}
}

// ObserveRemoteRTT records a single remote-call duration against
// shardID. The duration is always observed, success or failure —
// failed calls also have a measurable transport latency and seeing
// them in the histogram is what makes "slow failures" diagnosable.
func (m *RouterMetrics) ObserveRemoteRTT(shardID int, seconds float64) {
	if m == nil {
		return
	}
	m.rtt.Observe(shardID, seconds)
}

// IncRemoteError records one classified remote-call failure. The
// code label is bounded by the closed set in classifyRemoteError;
// callers should not invent new codes.
func (m *RouterMetrics) IncRemoteError(code string) {
	if m == nil {
		return
	}
	m.errors.Inc(code)
}

// IncBloomSkip records a remote RPC that was skipped because the
// shard's Bloom filter ruled out the lookup key. Reserved for slice
// 4; primitives live here so the metrics writer is fully wired up
// the moment the increment site lands.
func (m *RouterMetrics) IncBloomSkip(shardID int) {
	if m == nil {
		return
	}
	m.bloomSkips.Inc(shardID)
}

// Snapshot returns a deterministic, sorted view of all three series.
// Stable ordering keeps diff-friendly tests honest and matches the
// scrape ordering the Prometheus convention expects.
func (m *RouterMetrics) Snapshot() RouterMetricsSnapshot {
	if m == nil {
		return RouterMetricsSnapshot{}
	}
	return RouterMetricsSnapshot{
		RTT:        m.rtt.Snapshot(),
		Errors:     m.errors.Snapshot(),
		BloomSkips: m.bloomSkips.Snapshot(),
		Retries:    m.retries.Snapshot(),
	}
}

// RouterMetricsSnapshot is the shape consumed by the metrics writer
// in pkg/api. The three slices are independently sorted and safe to
// emit in any order.
type RouterMetricsSnapshot struct {
	RTT        []RemoteRTTSnapshot
	Errors     []RemoteErrorSample
	BloomSkips []BloomSkipSample
	Retries    []RetryOutcomeSample
}

// IncRetryOutcome records a per-query retry outcome (#85). One bump
// per scatter that retried (or attempted to). The closed set of
// labels keeps cardinality bounded:
//
//	"success_after_retry" — caller saw success, at least one shard
//	                        retried at least once.
//	"budget_exhausted"    — retry budget hit zero before success.
//	"deadline_exceeded"   — propagated deadline fired before success.
//
// Outcomes are mutually exclusive per query; the router picks the
// first matching label.
func (m *RouterMetrics) IncRetryOutcome(outcome string) {
	if m == nil {
		return
	}
	m.retries.Inc(outcome)
}

// remote RTT histogram

// remoteRTTHistogram is a per-shard latency histogram. Same shape as
// pkg/api.queryHistogram: cumulative bucket counts plus count + sum.
type remoteRTTHistogram struct {
	mu      sync.Mutex
	buckets []float64
	series  map[int]*rttSeries
}

type rttSeries struct {
	bucketCounts []uint64
	count        uint64
	sum          float64
}

func newRemoteRTTHistogram() *remoteRTTHistogram {
	return &remoteRTTHistogram{
		buckets: append([]float64(nil), remoteRTTBuckets...),
		series:  make(map[int]*rttSeries),
	}
}

// Observe records a duration. Negative values are clamped to 0; they
// only happen if a clock skews backward mid-call and emitting a
// negative bucket would corrupt the cumulative invariant.
func (h *remoteRTTHistogram) Observe(shardID int, seconds float64) {
	if seconds < 0 {
		seconds = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.series[shardID]
	if s == nil {
		s = &rttSeries{bucketCounts: make([]uint64, len(h.buckets))}
		h.series[shardID] = s
	}
	for i, b := range h.buckets {
		if seconds <= b {
			s.bucketCounts[i]++
		}
	}
	s.count++
	s.sum += seconds
}

// RemoteRTTSnapshot is one (shard_id, histogram) tuple in deterministic
// form. A synthetic +Inf bucket is appended at the end so the writer
// emits the standard Prometheus shape without re-deriving it.
type RemoteRTTSnapshot struct {
	ShardID int
	Buckets []RTTBucketPoint
	Count   uint64
	Sum     float64
}

type RTTBucketPoint struct {
	UpperBound float64
	Count      uint64
}

// Snapshot returns the histogram series sorted by shard_id ascending.
func (h *remoteRTTHistogram) Snapshot() []RemoteRTTSnapshot {
	h.mu.Lock()
	out := make([]RemoteRTTSnapshot, 0, len(h.series))
	for sid, s := range h.series {
		points := make([]RTTBucketPoint, 0, len(h.buckets)+1)
		for i, b := range h.buckets {
			points = append(points, RTTBucketPoint{UpperBound: b, Count: s.bucketCounts[i]})
		}
		points = append(points, RTTBucketPoint{UpperBound: math.Inf(1), Count: s.count})
		out = append(out, RemoteRTTSnapshot{
			ShardID: sid,
			Buckets: points,
			Count:   s.count,
			Sum:     s.sum,
		})
	}
	h.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

// remote error counters

// remoteErrorCounters tracks failed remote RPCs by classified code.
// Cardinality is bounded by the closed set in classifyRemoteError.
type remoteErrorCounters struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newRemoteErrorCounters() *remoteErrorCounters {
	return &remoteErrorCounters{counts: make(map[string]uint64)}
}

// Inc records one failure for the given code. Empty codes are
// dropped — every classifyRemoteError path returns a non-empty
// string, so an empty code here means a programming error upstream.
func (c *remoteErrorCounters) Inc(code string) {
	if code == "" {
		return
	}
	c.mu.Lock()
	c.counts[code]++
	c.mu.Unlock()
}

type RemoteErrorSample struct {
	Code  string
	Count uint64
}

// Snapshot returns the counters sorted alphabetically by code.
func (c *remoteErrorCounters) Snapshot() []RemoteErrorSample {
	c.mu.Lock()
	out := make([]RemoteErrorSample, 0, len(c.counts))
	for k, v := range c.counts {
		out = append(out, RemoteErrorSample{Code: k, Count: v})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// bloom-skip counters

// bloomSkipCounters tracks remote RPCs avoided because the per-shard
// Bloom filter ruled out the key. Cardinality is bounded by the
// number of shards.
type bloomSkipCounters struct {
	mu     sync.Mutex
	counts map[int]uint64
}

func newBloomSkipCounters() *bloomSkipCounters {
	return &bloomSkipCounters{counts: make(map[int]uint64)}
}

// Inc records one Bloom-filter-avoided RPC for shardID.
func (c *bloomSkipCounters) Inc(shardID int) {
	c.mu.Lock()
	c.counts[shardID]++
	c.mu.Unlock()
}

type BloomSkipSample struct {
	ShardID int
	Count   uint64
}

// Snapshot returns the counters sorted by shard_id ascending.
func (c *bloomSkipCounters) Snapshot() []BloomSkipSample {
	c.mu.Lock()
	out := make([]BloomSkipSample, 0, len(c.counts))
	for k, v := range c.counts {
		out = append(out, BloomSkipSample{ShardID: k, Count: v})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

// classifyRemoteError maps an error from a RemoteQuerier call to a
// stable, low-cardinality code suitable for a Prometheus label. The
// closed set is:
//
//	"timeout"        — context.DeadlineExceeded or net.Error.Timeout()
//	"canceled"       — context.Canceled (caller-driven, not a fault)
//	"conn_refused"   — *net.OpError carrying "connection refused"
//	"conn_reset"     — *net.OpError carrying "connection reset"
//	"broken_pipe"    — *net.OpError carrying "broken pipe"
//	"eof"            — io.EOF / io.ErrUnexpectedEOF
//	"other"          — anything else (protocol errors, query errors,
//	                   peer not found, …). Caps cardinality so a flood
//	                   of unique error strings can't blow up the
//	                   /metrics scrape.
//
// Mirrors the structural checks in isRetryableRemoteError so the two
// stay coherent: every error class the retry layer recognises shows
// up here under a stable label.
func classifyRemoteError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		msg := op.Err.Error()
		switch {
		case strings.Contains(msg, "connection refused"):
			return "conn_refused"
		case strings.Contains(msg, "connection reset"):
			return "conn_reset"
		case strings.Contains(msg, "broken pipe"):
			return "broken_pipe"
		}
	}
	return "other"
}

// observeRemote records one (RTT, optional-error) pair against
// shardID. Call sites compute the elapsed wall time around their
// remote.QueryRemoteShard invocation and pass it here so the metrics
// surface even when the result type doesn't fit a single helper.
func (m *RouterMetrics) observeRemote(shardID int, elapsed time.Duration, err error) {
	if m == nil {
		return
	}
	m.ObserveRemoteRTT(shardID, elapsed.Seconds())
	if err != nil {
		m.IncRemoteError(classifyRemoteError(err))
	}
}
