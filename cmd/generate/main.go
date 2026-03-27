package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	endpoint  = flag.String("endpoint", "http://localhost:8080", "Loveliness HTTP endpoint")
	nodes     = flag.Int("nodes", 10_000_000, "Number of nodes to generate")
	edgeRatio = flag.Float64("edge-ratio", 1.0, "Edges per node (e.g., 1.0 = same count as nodes)")
	batchSize = flag.Int("batch", 50_000, "Rows per bulk CSV upload (nodes)")
	edgeBatch = flag.Int("edge-batch", 50_000, "Rows per bulk edge upload")
	seed      = flag.Int64("seed", 42, "Random seed for deterministic edge generation")
	skipSchema = flag.Bool("skip-schema", false, "Skip schema creation (already exists)")
)

var cities = []string{
	"Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga",
	"Dunedin", "Palmerston North", "Napier", "Nelson", "Rotorua",
	"New Plymouth", "Whangarei", "Invercargill", "Whanganui", "Gisborne",
}

var firstNames = []string{
	"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Hank",
	"Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Pete",
	"Quinn", "Rose", "Sam", "Tara", "Uma", "Vince", "Wendy", "Xander",
	"Yara", "Zach", "Aiden", "Bella", "Caleb", "Diana", "Ethan", "Fiona",
}

func main() {
	flag.Parse()

	totalEdges := int(float64(*nodes) * *edgeRatio)
	fmt.Printf("Loveliness bulk data generator\n")
	fmt.Printf("  endpoint:   %s\n", *endpoint)
	fmt.Printf("  nodes:      %d\n", *nodes)
	fmt.Printf("  edges:      %d\n", totalEdges)
	fmt.Printf("  node batch: %d\n", *batchSize)
	fmt.Printf("  edge batch: %d\n", *edgeBatch)
	fmt.Printf("  seed:       %d\n", *seed)
	fmt.Println()

	client := &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// 1. Create schema.
	if !*skipSchema {
		fmt.Print("Creating schema... ")
		mustCypher(client, "CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")
		mustCypher(client, "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)")
		fmt.Println("done")
	}

	// 2. Bulk load nodes.
	fmt.Printf("Loading %d nodes in batches of %d...\n", *nodes, *batchSize)
	nodeStart := time.Now()
	nodesLoaded := 0
	nodeErrors := 0

	for offset := 0; offset < *nodes; offset += *batchSize {
		end := offset + *batchSize
		if end > *nodes {
			end = *nodes
		}
		count := end - offset

		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		_ = w.Write([]string{"name", "age", "city"})
		for i := offset; i < end; i++ {
			name := nodeName(i)
			age := strconv.Itoa(18 + rand.Intn(62))
			city := cities[rand.Intn(len(cities))]
			_ = w.Write([]string{name, age, city})
		}
		w.Flush()

		loaded, err := bulkPost(client, *endpoint+"/bulk/nodes", "Person", "", "", &buf, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  batch %d-%d failed: %v\n", offset, end, err)
			nodeErrors += count
		} else {
			nodesLoaded += int(loaded)
		}

		elapsed := time.Since(nodeStart).Seconds()
		rate := float64(nodesLoaded) / elapsed
		pct := float64(nodesLoaded) / float64(*nodes) * 100
		fmt.Printf("  nodes: %d/%d (%.1f%%) @ %.0f/sec\n", nodesLoaded, *nodes, pct, rate)
	}

	nodeElapsed := time.Since(nodeStart)
	fmt.Printf("  nodes complete: %d in %s (%.0f/sec, %d errors)\n\n",
		nodesLoaded, nodeElapsed.Round(time.Millisecond),
		float64(nodesLoaded)/nodeElapsed.Seconds(), nodeErrors)

	// 3. Two-pass edge loading.
	// Pass 1: Create all cross-shard reference nodes (X-Refs-Only: true).
	// Pass 2: Load edges without ref creation (X-Skip-Refs: true).
	nodeCount := nodesLoaded
	if nodeCount < 2 {
		fmt.Println("  not enough nodes for edges, skipping")
	} else {
		// Pass 1: reference node pre-seeding.
		fmt.Printf("Pass 1: Pre-seeding reference nodes for %d edges...\n", totalEdges)
		refStart := time.Now()
		refsCreated := 0
		refErrors := 0
		rng1 := rand.New(rand.NewSource(*seed))

		for offset := 0; offset < totalEdges; offset += *edgeBatch {
			end := offset + *edgeBatch
			if end > totalEdges {
				end = totalEdges
			}
			count := end - offset

			var buf bytes.Buffer
			w := csv.NewWriter(&buf)
			w.Write([]string{"from", "to", "since"})
			for i := 0; i < count; i++ {
				a := rng1.Intn(nodeCount)
				b := rng1.Intn(nodeCount)
				for b == a {
					b = rng1.Intn(nodeCount)
				}
				since := strconv.Itoa(2015 + rng1.Intn(11))
				w.Write([]string{nodeName(a), nodeName(b), since})
			}
			w.Flush()

			extraHeaders := map[string]string{"X-Refs-Only": "true"}
			loaded, err := bulkPost(client, *endpoint+"/bulk/edges", "", "KNOWS", "Person", &buf, extraHeaders)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ref batch %d-%d failed: %v\n", offset, end, err)
				refErrors += count
			} else {
				refsCreated += int(loaded)
			}

			elapsed := time.Since(refStart).Seconds()
			pct := float64(offset+count) / float64(totalEdges) * 100
			rate := float64(offset+count) / elapsed
			fmt.Printf("  refs: %d/%d (%.1f%%) scanned @ %.0f edges/sec, %d refs created\n",
				offset+count, totalEdges, pct, rate, refsCreated)
		}

		refElapsed := time.Since(refStart)
		fmt.Printf("  refs complete: %d created in %s (%d errors)\n\n",
			refsCreated, refElapsed.Round(time.Millisecond), refErrors)

		// Pass 2: load edges (refs already exist).
		fmt.Printf("Pass 2: Loading %d edges in batches of %d...\n", totalEdges, *edgeBatch)
		edgeStart := time.Now()
		edgesLoaded := 0
		edgeErrors := 0
		rng2 := rand.New(rand.NewSource(*seed)) // same seed = same edges

		for offset := 0; offset < totalEdges; offset += *edgeBatch {
			end := offset + *edgeBatch
			if end > totalEdges {
				end = totalEdges
			}
			count := end - offset

			var buf bytes.Buffer
			w := csv.NewWriter(&buf)
			w.Write([]string{"from", "to", "since"})
			for i := 0; i < count; i++ {
				a := rng2.Intn(nodeCount)
				b := rng2.Intn(nodeCount)
				for b == a {
					b = rng2.Intn(nodeCount)
				}
				since := strconv.Itoa(2015 + rng2.Intn(11))
				w.Write([]string{nodeName(a), nodeName(b), since})
			}
			w.Flush()

			extraHeaders := map[string]string{"X-Skip-Refs": "true"}
			loaded, err := bulkPost(client, *endpoint+"/bulk/edges", "", "KNOWS", "Person", &buf, extraHeaders)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  batch %d-%d failed: %v\n", offset, end, err)
				edgeErrors += count
			} else {
				edgesLoaded += int(loaded)
			}

			elapsed := time.Since(edgeStart).Seconds()
			rate := float64(edgesLoaded) / elapsed
			pct := float64(edgesLoaded) / float64(totalEdges) * 100
			fmt.Printf("  edges: %d/%d (%.1f%%) @ %.0f/sec\n", edgesLoaded, totalEdges, pct, rate)
		}

		edgeElapsed := time.Since(edgeStart)
		totalElapsed := time.Since(nodeStart)
		fmt.Printf("  edges complete: %d in %s (%.0f/sec, %d errors)\n\n",
			edgesLoaded, edgeElapsed.Round(time.Millisecond),
			float64(edgesLoaded)/edgeElapsed.Seconds(), edgeErrors)

		fmt.Printf("Done! Total: %s\n", totalElapsed.Round(time.Millisecond))
		fmt.Printf("  %d nodes + %d refs + %d edges\n",
			nodesLoaded, refsCreated, edgesLoaded)
	}

	if nodeErrors > 0 {
		os.Exit(1)
	}
}

// nodeName generates a deterministic unique name for node index i.
func nodeName(i int) string {
	first := firstNames[i%len(firstNames)]
	return fmt.Sprintf("%s-%d", first, i)
}

// mustCypher sends a raw Cypher query to POST /cypher.
func mustCypher(client *http.Client, cypher string) {
	resp, err := client.Post(*endpoint+"/cypher", "text/plain", bytes.NewBufferString(cypher))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cypher failed: %v\n  query: %s\n", err, cypher)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "cypher failed (status %d): %s\n  query: %s\n", resp.StatusCode, body, cypher)
		os.Exit(1)
	}
}

// bulkPost sends a CSV body to a bulk endpoint and returns the loaded count.
func bulkPost(client *http.Client, url, tableName, relTable, nodeTable string, csvBody *bytes.Buffer, extraHeaders map[string]string) (int64, error) {
	req, err := http.NewRequest("POST", url, csvBody)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "text/csv")
	if tableName != "" {
		req.Header.Set("X-Table", tableName)
	}
	if relTable != "" {
		req.Header.Set("X-Rel-Table", relTable)
		req.Header.Set("X-From-Table", nodeTable)
		req.Header.Set("X-To-Table", nodeTable)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusMultiStatus {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Loaded int64    `json:"loaded"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}
	if len(result.Errors) > 0 {
		return result.Loaded, fmt.Errorf("%d errors: %v", len(result.Errors), result.Errors[0])
	}
	return result.Loaded, nil
}
