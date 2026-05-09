// arrow-bench compares the wire size and round-trip latency of the
// /cypher endpoint when negotiated as JSON vs Apache Arrow IPC
// stream. It exists to give #27 a concrete answer to the question
// "what does Arrow buy us at 25K rows?" — the cosmograph spike's
// motivating data point — without sending the user to dig through
// curl + wc -c by hand.
//
// Run against a local Loveliness server:
//
//	arrow-bench -endpoint http://localhost:8080 \
//	            -query "MATCH (p:Person) RETURN p LIMIT 25000" \
//	            -iters 5
//
// Prints a small JSON object so the result can be diffed across
// runs / branches:
//
//	{
//	  "rows": 25000,
//	  "json":  {"bytes": 4193280, "median_ms": 142.3, ...},
//	  "arrow": {"bytes":  524288, "median_ms":  78.5, ...},
//	  "ratio": {"bytes": 0.125,    "median_ms": 0.55}
//	}
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	contentTypeJSON        = "application/json"
	contentTypeArrowStream = "application/vnd.apache.arrow.stream"
)

var (
	endpoint = flag.String("endpoint", "http://localhost:8080", "Target HTTP endpoint")
	query    = flag.String("query", "MATCH (p:Person) RETURN p LIMIT 25000", "Cypher query to compare")
	iters    = flag.Int("iters", 5, "Iterations per format (median is reported)")
	timeout  = flag.Duration("timeout", 5*time.Minute, "Per-request timeout")
)

func main() {
	flag.Parse()
	out, err := run(*endpoint, *query, *iters, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(1)
	}
}

type formatStats struct {
	Bytes    int     `json:"bytes"`
	MedianMs float64 `json:"median_ms"`
	MinMs    float64 `json:"min_ms"`
	MaxMs    float64 `json:"max_ms"`
}

type comparison struct {
	Endpoint string  `json:"endpoint"`
	Query    string  `json:"query"`
	Iters    int     `json:"iters"`
	Rows     int     `json:"rows"`
	JSON     formatStats `json:"json"`
	Arrow    formatStats `json:"arrow"`
	Ratio    struct {
		Bytes    float64 `json:"bytes"`
		MedianMs float64 `json:"median_ms"`
	} `json:"ratio"`
}

func run(endpoint, query string, iters int, timeout time.Duration) (*comparison, error) {
	if iters <= 0 {
		return nil, fmt.Errorf("iters must be > 0")
	}

	client := &http.Client{Timeout: timeout}

	jsonStats, rowCount, err := measure(client, endpoint, query, contentTypeJSON, iters, parseJSONRowCount)
	if err != nil {
		return nil, fmt.Errorf("json measurement: %w", err)
	}
	arrowStats, _, err := measure(client, endpoint, query, contentTypeArrowStream, iters, nil)
	if err != nil {
		return nil, fmt.Errorf("arrow measurement: %w", err)
	}

	out := &comparison{
		Endpoint: endpoint,
		Query:    query,
		Iters:    iters,
		Rows:     rowCount,
		JSON:     jsonStats,
		Arrow:    arrowStats,
	}
	if jsonStats.Bytes > 0 {
		out.Ratio.Bytes = float64(arrowStats.Bytes) / float64(jsonStats.Bytes)
	}
	if jsonStats.MedianMs > 0 {
		out.Ratio.MedianMs = arrowStats.MedianMs / jsonStats.MedianMs
	}
	return out, nil
}

// measure runs a single Accept variant `iters` times and reports
// payload size + latency stats. The first invocation also returns a
// row count when a parser is supplied — that's a sanity signal so
// the JSON and Arrow measurements aren't accidentally comparing
// different result sets (e.g. an unbounded query that grew between
// iterations).
func measure(
	client *http.Client,
	endpoint, query, accept string,
	iters int,
	parseRows func([]byte) (int, error),
) (formatStats, int, error) {
	var lastBytes int
	var rowCount int
	durations := make([]float64, 0, iters)
	for i := 0; i < iters; i++ {
		size, dur, body, err := fetchOnce(client, endpoint, query, accept)
		if err != nil {
			return formatStats{}, 0, err
		}
		durations = append(durations, float64(dur)/float64(time.Millisecond))
		lastBytes = size
		if i == 0 && parseRows != nil {
			n, err := parseRows(body)
			if err == nil {
				rowCount = n
			}
		}
	}
	sort.Float64s(durations)
	return formatStats{
		Bytes:    lastBytes,
		MedianMs: durations[len(durations)/2],
		MinMs:    durations[0],
		MaxMs:    durations[len(durations)-1],
	}, rowCount, nil
}

func fetchOnce(client *http.Client, endpoint, query, accept string) (int, time.Duration, []byte, error) {
	req, err := http.NewRequest(http.MethodPost, endpoint+"/cypher", strings.NewReader(query))
	if err != nil {
		return 0, 0, nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Content-Type", "text/plain")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, time.Since(start), nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	dur := time.Since(start)
	if err != nil {
		return 0, dur, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, dur, body, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(body, 200))
	}
	return len(body), dur, body, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

func parseJSONRowCount(body []byte) (int, error) {
	var env struct {
		Rows []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return 0, err
	}
	return len(env.Rows), nil
}

