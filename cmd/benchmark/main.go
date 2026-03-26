package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	endpoint = flag.String("endpoint", "http://localhost:8080", "Loveliness HTTP endpoint")
	nodes    = flag.Int("nodes", 50_000, "Number of nodes to seed")
	edges    = flag.Int("edges", 50_000, "Number of edges to seed")
	iters    = flag.Int("iters", 200, "Iterations per benchmark")
	workers  = flag.Int("workers", 8, "Concurrent workers for throughput tests")
	skipSeed = flag.Bool("skip-seed", false, "Skip data seeding (already loaded)")
)

var cities = []string{
	"Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga",
	"Dunedin", "Palmerston North", "Napier", "Nelson", "Rotorua",
}

var firstNames = []string{
	"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Hank",
	"Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Pete",
	"Quinn", "Rose", "Sam", "Tara", "Uma", "Vince", "Wendy", "Xander",
}

func nodeName(i int) string {
	return fmt.Sprintf("%s-%d", firstNames[i%len(firstNames)], i)
}

// --- HTTP helpers ---

var client = &http.Client{
	Timeout: 5 * time.Minute,
	Transport: &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
	},
}

type queryResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Partial bool             `json:"partial"`
	Stats   struct {
		CompileMs float64 `json:"compile_time_ms"`
		ExecMs    float64 `json:"exec_time_ms"`
	} `json:"stats"`
}

func cypher(query string) (*queryResult, time.Duration, error) {
	start := time.Now()
	resp, err := client.Post(*endpoint+"/cypher", "text/plain", strings.NewReader(query))
	elapsed := time.Since(start)
	if err != nil {
		return nil, elapsed, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, elapsed, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	var result queryResult
	json.Unmarshal(body, &result)
	return &result, elapsed, nil
}

func mustCypher(query string) {
	_, _, err := cypher(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n  query: %s\n", err, query)
		os.Exit(1)
	}
}

// --- Benchmark infrastructure ---

type benchResult struct {
	Name       string
	Iters      int
	Errors     int
	Latencies  []time.Duration
	TotalTime  time.Duration
	RowCounts  []int
}

func (b *benchResult) P(pct float64) time.Duration {
	if len(b.Latencies) == 0 {
		return 0
	}
	sort.Slice(b.Latencies, func(i, j int) bool { return b.Latencies[i] < b.Latencies[j] })
	idx := int(math.Ceil(pct/100*float64(len(b.Latencies)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(b.Latencies) {
		idx = len(b.Latencies) - 1
	}
	return b.Latencies[idx]
}

func (b *benchResult) Mean() time.Duration {
	if len(b.Latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, l := range b.Latencies {
		total += l
	}
	return total / time.Duration(len(b.Latencies))
}

func (b *benchResult) QPS() float64 {
	if b.TotalTime == 0 {
		return 0
	}
	return float64(b.Iters) / b.TotalTime.Seconds()
}

func (b *benchResult) AvgRows() float64 {
	if len(b.RowCounts) == 0 {
		return 0
	}
	total := 0
	for _, c := range b.RowCounts {
		total += c
	}
	return float64(total) / float64(len(b.RowCounts))
}

func runBench(name string, n int, fn func() (int, error)) benchResult {
	br := benchResult{Name: name, Iters: n}
	start := time.Now()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		rows, err := fn()
		elapsed := time.Since(t0)
		br.Latencies = append(br.Latencies, elapsed)
		br.RowCounts = append(br.RowCounts, rows)
		if err != nil {
			br.Errors++
		}
	}
	br.TotalTime = time.Since(start)
	return br
}

func runBenchConcurrent(name string, n, concurrency int, fn func() (int, error)) benchResult {
	br := benchResult{Name: name, Iters: n}
	var mu sync.Mutex
	var remaining atomic.Int64
	remaining.Store(int64(n))

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				t0 := time.Now()
				rows, err := fn()
				elapsed := time.Since(t0)
				mu.Lock()
				br.Latencies = append(br.Latencies, elapsed)
				br.RowCounts = append(br.RowCounts, rows)
				if err != nil {
					br.Errors++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	br.TotalTime = time.Since(start)
	return br
}

// --- Seeding ---

func seed() {
	fmt.Println("Seeding data...")

	fmt.Print("  schema... ")
	mustCypher("CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")
	mustCypher("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)")
	fmt.Println("done")

	// Bulk load nodes.
	fmt.Printf("  %d nodes... ", *nodes)
	t0 := time.Now()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Write([]string{"name", "age", "city"})
	for i := 0; i < *nodes; i++ {
		w.Write([]string{
			nodeName(i),
			strconv.Itoa(18 + rand.Intn(62)),
			cities[rand.Intn(len(cities))],
		})
	}
	w.Flush()
	bulkPost("/bulk/nodes", "Person", "", "", &buf)
	fmt.Printf("done (%s)\n", time.Since(t0).Round(time.Millisecond))

	// Bulk load edges.
	fmt.Printf("  %d edges... ", *edges)
	t0 = time.Now()
	buf.Reset()
	w = csv.NewWriter(&buf)
	w.Write([]string{"from", "to", "since"})
	for i := 0; i < *edges; i++ {
		a := rand.Intn(*nodes)
		b := rand.Intn(*nodes)
		for b == a {
			b = rand.Intn(*nodes)
		}
		w.Write([]string{nodeName(a), nodeName(b), strconv.Itoa(2015 + rand.Intn(11))})
	}
	w.Flush()
	bulkPost("/bulk/edges", "", "KNOWS", "Person", &buf)
	fmt.Printf("done (%s)\n", time.Since(t0).Round(time.Millisecond))

	fmt.Println()
}

func bulkPost(path, tableName, relTable, nodeTable string, body *bytes.Buffer) {
	req, _ := http.NewRequest("POST", *endpoint+path, body)
	req.Header.Set("Content-Type", "text/csv")
	if tableName != "" {
		req.Header.Set("X-Table", tableName)
	}
	if relTable != "" {
		req.Header.Set("X-Rel-Table", relTable)
		req.Header.Set("X-From-Table", nodeTable)
		req.Header.Set("X-To-Table", nodeTable)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bulk load failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusMultiStatus {
		fmt.Fprintf(os.Stderr, "bulk load HTTP %d: %s\n", resp.StatusCode, string(b))
		os.Exit(1)
	}
}

// --- Benchmarks ---

func benchPointLookup() benchResult {
	return runBench("point_lookup", *iters, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MATCH (p:Person {name: '%s'}) RETURN p.name, p.age, p.city", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchPointLookupConcurrent() benchResult {
	return runBenchConcurrent("point_lookup_concurrent", *iters*4, *workers, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MATCH (p:Person {name: '%s'}) RETURN p.name, p.age, p.city", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchRangeFilter() benchResult {
	return runBench("range_filter", *iters, func() (int, error) {
		city := cities[rand.Intn(len(cities))]
		minAge := 20 + rand.Intn(30)
		q := fmt.Sprintf("MATCH (p:Person) WHERE p.city = '%s' AND p.age > %d RETURN p.name, p.age LIMIT 20", city, minAge)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchCountAll() benchResult {
	return runBench("count_all_nodes", *iters/2, func() (int, error) {
		res, _, err := cypher("MATCH (p:Person) RETURN count(p)")
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchCountFiltered() benchResult {
	return runBench("count_filtered", *iters, func() (int, error) {
		city := cities[rand.Intn(len(cities))]
		q := fmt.Sprintf("MATCH (p:Person) WHERE p.city = '%s' RETURN count(p)", city)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchAggregation() benchResult {
	return runBench("aggregate_avg_age", *iters, func() (int, error) {
		city := cities[rand.Intn(len(cities))]
		q := fmt.Sprintf("MATCH (p:Person) WHERE p.city = '%s' RETURN avg(p.age), min(p.age), max(p.age)", city)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchSingleHop() benchResult {
	return runBench("1_hop_traversal", *iters, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MATCH (a:Person {name: '%s'})-[:KNOWS]->(b:Person) RETURN b.name, b.city LIMIT 10", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchTwoHop() benchResult {
	return runBench("2_hop_traversal", *iters, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MATCH (a:Person {name: '%s'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name LIMIT 20", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchVariableLengthPath() benchResult {
	return runBench("var_length_path_1_3", *iters/2, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MATCH (a:Person {name: '%s'})-[:KNOWS*1..3]->(b:Person) RETURN DISTINCT b.name LIMIT 20", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchShortestPath() benchResult {
	return runBench("shortest_path", *iters/4, func() (int, error) {
		a := nodeName(rand.Intn(*nodes))
		b := nodeName(rand.Intn(*nodes))
		for b == a {
			b = nodeName(rand.Intn(*nodes))
		}
		q := fmt.Sprintf("MATCH (a:Person {name: '%s'})-[r:KNOWS* SHORTEST 1..6]->(b:Person {name: '%s'}) RETURN length(r)", a, b)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchAllShortestPaths() benchResult {
	return runBench("all_shortest_paths", *iters/4, func() (int, error) {
		a := nodeName(rand.Intn(*nodes))
		b := nodeName(rand.Intn(*nodes))
		for b == a {
			b = nodeName(rand.Intn(*nodes))
		}
		q := fmt.Sprintf("MATCH (a:Person {name: '%s'})-[r:KNOWS* ALL SHORTEST 1..6]->(b:Person {name: '%s'}) RETURN length(r)", a, b)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchMutualFriends() benchResult {
	return runBench("mutual_friends", *iters/2, func() (int, error) {
		a := nodeName(rand.Intn(*nodes))
		b := nodeName(rand.Intn(*nodes))
		for b == a {
			b = nodeName(rand.Intn(*nodes))
		}
		q := fmt.Sprintf(
			"MATCH (a:Person {name: '%s'})-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person {name: '%s'}) RETURN m.name LIMIT 10",
			a, b)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchSingleWrite() benchResult {
	counter := atomic.Int64{}
	return runBench("single_write", *iters, func() (int, error) {
		id := counter.Add(1)
		name := fmt.Sprintf("BenchWrite-%d", id)
		q := fmt.Sprintf("CREATE (p:Person {name: '%s', age: 30, city: 'Auckland'})", name)
		_, _, err := cypher(q)
		return 0, err
	})
}

func benchMergeUpsert() benchResult {
	return runBench("merge_upsert", *iters, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf("MERGE (p:Person {name: '%s'}) ON MATCH SET p.age = 99 RETURN p.name", name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchReadAfterWrite() benchResult {
	counter := atomic.Int64{}
	return runBench("read_after_write", *iters/2, func() (int, error) {
		id := counter.Add(1)
		name := fmt.Sprintf("RAW-%d", id)
		// Write.
		wq := fmt.Sprintf("CREATE (p:Person {name: '%s', age: 25, city: 'Wellington'})", name)
		if _, _, err := cypher(wq); err != nil {
			return 0, err
		}
		// Read back.
		rq := fmt.Sprintf("MATCH (p:Person {name: '%s'}) RETURN p.name, p.age", name)
		res, _, err := cypher(rq)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchFriendOfFriendCount() benchResult {
	return runBench("friend_of_friend_count", *iters/2, func() (int, error) {
		name := nodeName(rand.Intn(*nodes))
		q := fmt.Sprintf(
			"MATCH (a:Person {name: '%s'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.name <> c.name RETURN count(DISTINCT c)",
			name)
		res, _, err := cypher(q)
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

func benchGroupByCity() benchResult {
	return runBench("group_by_city", *iters/2, func() (int, error) {
		res, _, err := cypher("MATCH (p:Person) RETURN p.city, count(p), avg(p.age) ORDER BY count(p) DESC")
		if err != nil {
			return 0, err
		}
		return len(res.Rows), nil
	})
}

// --- Output ---

func printResults(results []benchResult) {
	fmt.Println()
	fmt.Println(strings.Repeat("═", 110))
	fmt.Printf("  %-30s %6s %6s %8s %8s %8s %8s %8s %6s\n",
		"BENCHMARK", "ITERS", "ERRS", "MEAN", "P50", "P95", "P99", "QPS", "ROWS")
	fmt.Println(strings.Repeat("─", 110))

	for _, r := range results {
		fmt.Printf("  %-30s %6d %6d %8s %8s %8s %8s %8.0f %6.1f\n",
			r.Name,
			r.Iters,
			r.Errors,
			fmtDur(r.Mean()),
			fmtDur(r.P(50)),
			fmtDur(r.P(95)),
			fmtDur(r.P(99)),
			r.QPS(),
			r.AvgRows(),
		)
	}
	fmt.Println(strings.Repeat("═", 110))
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func main() {
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║    Loveliness Benchmark Suite            ║")
	fmt.Printf("║    endpoint: %-28s║\n", *endpoint)
	fmt.Printf("║    nodes: %-7d  edges: %-7d         ║\n", *nodes, *edges)
	fmt.Printf("║    iters: %-7d  workers: %-5d         ║\n", *iters, *workers)
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()

	if !*skipSeed {
		seed()
	}

	var results []benchResult

	// --- Reads ---
	fmt.Println("Running benchmarks...")
	fmt.Println()
	fmt.Println("── Reads ──")

	fmt.Print("  point_lookup... ")
	r := benchPointLookup()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  point_lookup_concurrent... ")
	r = benchPointLookupConcurrent()
	fmt.Printf("%s (qps=%.0f)\n", fmtDur(r.Mean()), r.QPS())
	results = append(results, r)

	fmt.Print("  range_filter... ")
	r = benchRangeFilter()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  count_all_nodes... ")
	r = benchCountAll()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  count_filtered... ")
	r = benchCountFiltered()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  aggregate_avg_age... ")
	r = benchAggregation()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  group_by_city... ")
	r = benchGroupByCity()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	// --- Traversals ---
	fmt.Println()
	fmt.Println("── Traversals ──")

	fmt.Print("  1_hop_traversal... ")
	r = benchSingleHop()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  2_hop_traversal... ")
	r = benchTwoHop()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  var_length_path_1_3... ")
	r = benchVariableLengthPath()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  friend_of_friend_count... ")
	r = benchFriendOfFriendCount()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  mutual_friends... ")
	r = benchMutualFriends()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	fmt.Print("  shortest_path... ")
	r = benchShortestPath()
	fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	results = append(results, r)

	// ALL SHORTEST can crash LadybugDB on certain graph shapes — skipped.
	// fmt.Print("  all_shortest_paths... ")
	// r = benchAllShortestPaths()
	// fmt.Printf("%s (p50=%s)\n", fmtDur(r.Mean()), fmtDur(r.P(50)))
	// results = append(results, r)

	// --- Writes ---
	fmt.Println()
	fmt.Println("── Writes ──")

	fmt.Print("  single_write... ")
	r = benchSingleWrite()
	fmt.Printf("%s (qps=%.0f)\n", fmtDur(r.Mean()), r.QPS())
	results = append(results, r)

	fmt.Print("  merge_upsert... ")
	r = benchMergeUpsert()
	fmt.Printf("%s (qps=%.0f)\n", fmtDur(r.Mean()), r.QPS())
	results = append(results, r)

	fmt.Print("  read_after_write... ")
	r = benchReadAfterWrite()
	fmt.Printf("%s (qps=%.0f)\n", fmtDur(r.Mean()), r.QPS())
	results = append(results, r)

	// --- Summary ---
	printResults(results)
}
