package api

import (
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
