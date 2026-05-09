package api

import (
	"math"
	"sort"
	"strings"
	"sync"
)

// queryCounters tracks the number of /cypher requests grouped by
// (query_type, status). Bounded label cardinality (≤ 4 types × 3 status =
// 12 series) keeps the metric cheap; that's the budget #12 mandates.
type queryCounters struct {
	mu     sync.Mutex
	counts map[queryCounterKey]uint64
}

type queryCounterKey struct {
	qtype  string
	status string
}

func newQueryCounters() *queryCounters {
	return &queryCounters{counts: make(map[queryCounterKey]uint64)}
}

// Inc records one query under (qtype, status). Both labels are normalized
// to the small fixed sets in classifyQueryType and statusBucket.
func (q *queryCounters) Inc(qtype, status string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.counts[queryCounterKey{qtype: qtype, status: status}]++
	q.mu.Unlock()
}

// Snapshot returns a deterministic, sorted snapshot for emission. Sorting
// matches the Prometheus convention of stable scrape ordering — useful
// for diff-friendly tests and for monotonicity checks downstream.
func (q *queryCounters) Snapshot() []queryCounterSample {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	out := make([]queryCounterSample, 0, len(q.counts))
	for k, v := range q.counts {
		out = append(out, queryCounterSample{QueryType: k.qtype, Status: k.status, Count: v})
	}
	q.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].QueryType != out[j].QueryType {
			return out[i].QueryType < out[j].QueryType
		}
		return out[i].Status < out[j].Status
	})
	return out
}

type queryCounterSample struct {
	QueryType string
	Status    string
	Count     uint64
}

// classifyQueryType maps a Cypher string to one of the bounded label
// values: "read" | "write" | "schema" | "unknown". Mirrors the
// router/parser classification but without paying the shard-key
// extraction cost — this runs on every request.
func classifyQueryType(cypher string) string {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if upper == "" {
		return "unknown"
	}
	// Schema DDL must be checked first because "CREATE NODE TABLE" also
	// matches the "CREATE" write prefix below.
	for _, p := range []string{
		"CREATE NODE TABLE", "CREATE REL TABLE", "CREATE RELATIONSHIP TABLE",
		"CREATE TABLE", "DROP TABLE", "DROP ", "ALTER ",
	} {
		if strings.HasPrefix(upper, p) {
			return "schema"
		}
	}
	for _, p := range []string{
		"CREATE ", "MERGE ", "SET ", "DELETE ", "REMOVE ", "DETACH ",
	} {
		if strings.HasPrefix(upper, p) || strings.Contains(upper, " "+p) {
			return "write"
		}
	}
	for _, p := range []string{
		"MATCH ", "OPTIONAL ", "RETURN ", "WITH ", "UNWIND ", "CALL ", "SHOW ",
	} {
		if strings.HasPrefix(upper, p) {
			return "read"
		}
	}
	return "unknown"
}

// queryDurationBuckets are the histogram bucket upper bounds in seconds.
// Aligned with the latency targets in #12 (0.25 ms → 2.5 s) — we expect
// most reads under 25 ms and most writes under 250 ms; buckets span both
// regimes with enough resolution to spot a tail moving.
var queryDurationBuckets = []float64{
	0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01,
	0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
}

// queryHistogram is a hand-rolled Prometheus histogram for /cypher
// latencies grouped by (query_type, status). Avoids dragging in the
// prometheus/client_golang dep just for one series; pulls its weight
// here because the only thing we need is `Observe + Snapshot`.
type queryHistogram struct {
	mu      sync.Mutex
	buckets []float64 // upper bounds (sec), strictly ascending
	series  map[queryCounterKey]*histogramSeries
}

type histogramSeries struct {
	bucketCounts []uint64 // len == len(buckets), cumulative at write time
	count        uint64
	sum          float64
}

func newQueryHistogram() *queryHistogram {
	return &queryHistogram{
		buckets: append([]float64(nil), queryDurationBuckets...),
		series:  make(map[queryCounterKey]*histogramSeries),
	}
}

// Observe records a single duration in seconds against (qtype, status).
func (h *queryHistogram) Observe(qtype, status string, seconds float64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	k := queryCounterKey{qtype: qtype, status: status}
	s := h.series[k]
	if s == nil {
		s = &histogramSeries{bucketCounts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, b := range h.buckets {
		if seconds <= b {
			s.bucketCounts[i]++
		}
	}
	s.count++
	s.sum += seconds
}

// histogramSnapshot is the deterministic, sorted dump used by the
// metrics writer.
type histogramSnapshot struct {
	QueryType string
	Status    string
	Buckets   []bucketPoint // one per bucket bound, +Inf appended at the end
	Count     uint64
	Sum       float64
}

type bucketPoint struct {
	UpperBound float64
	Count      uint64
}

// Snapshot returns a deterministic dump of the histogram. Each series
// gets the configured bucket bounds plus a synthetic +Inf bucket whose
// count equals the total observations — required by the Prometheus
// histogram contract.
func (h *queryHistogram) Snapshot() []histogramSnapshot {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	out := make([]histogramSnapshot, 0, len(h.series))
	for k, s := range h.series {
		points := make([]bucketPoint, 0, len(h.buckets)+1)
		for i, b := range h.buckets {
			points = append(points, bucketPoint{UpperBound: b, Count: s.bucketCounts[i]})
		}
		points = append(points, bucketPoint{UpperBound: math.Inf(1), Count: s.count})
		out = append(out, histogramSnapshot{
			QueryType: k.qtype, Status: k.status,
			Buckets: points, Count: s.count, Sum: s.sum,
		})
	}
	h.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].QueryType != out[j].QueryType {
			return out[i].QueryType < out[j].QueryType
		}
		return out[i].Status < out[j].Status
	})
	return out
}

// statusBucket collapses an HTTP status into the three buckets exposed
// as the `status` label: "ok" (2xx), "client_error" (4xx),
// "server_error" (5xx). Keeps cardinality bounded — the spec forbids
// labeling by raw HTTP code.
func statusBucket(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "ok"
	case code >= 400 && code < 500:
		return "client_error"
	case code >= 500:
		return "server_error"
	default:
		return "unknown"
	}
}
