package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	endpoint   = flag.String("endpoint", "http://localhost:8080", "Loveliness HTTP endpoint")
	nodes      = flag.Int("nodes", 10_000_000, "Number of nodes to generate")
	edgeRatio  = flag.Float64("edge-ratio", 1.0, "Edges per node (e.g., 1.0 = same count as nodes)")
	workers    = flag.Int("workers", 64, "Concurrent HTTP workers")
	batchSize  = flag.Int("batch", 1000, "Progress report interval")
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

type queryReq struct {
	Cypher string `json:"cypher"`
}

func main() {
	flag.Parse()

	totalEdges := int(float64(*nodes) * *edgeRatio)
	fmt.Printf("Loveliness data generator\n")
	fmt.Printf("  endpoint:  %s\n", *endpoint)
	fmt.Printf("  nodes:     %d\n", *nodes)
	fmt.Printf("  edges:     %d\n", totalEdges)
	fmt.Printf("  workers:   %d\n", *workers)
	fmt.Println()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *workers + 10,
			MaxIdleConnsPerHost: *workers + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// 1. Create schema.
	if !*skipSchema {
		fmt.Print("Creating schema... ")
		mustQuery(client, "CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY(name))")
		mustQuery(client, "CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)")
		fmt.Println("done")
	}

	// 2. Generate nodes.
	fmt.Printf("Inserting %d nodes...\n", *nodes)
	var nodesCreated atomic.Int64
	var nodeErrors atomic.Int64
	nodeStart := time.Now()

	nodeCh := make(chan string, *workers*2)
	var wg sync.WaitGroup

	// Progress reporter.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := nodesCreated.Load()
				elapsed := time.Since(nodeStart).Seconds()
				rate := float64(n) / elapsed
				pct := float64(n) / float64(*nodes) * 100
				errs := nodeErrors.Load()
				fmt.Printf("  nodes: %d/%d (%.1f%%) @ %.0f/sec [errors: %d]\n", n, *nodes, pct, rate, errs)
			case <-done:
				return
			}
		}
	}()

	// Workers.
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cypher := range nodeCh {
				if err := query(client, cypher); err != nil {
					nodeErrors.Add(1)
				} else {
					nodesCreated.Add(1)
				}
			}
		}()
	}

	// Feed node creates.
	for i := 0; i < *nodes; i++ {
		name := nodeName(i)
		age := 18 + rand.Intn(62)
		city := cities[rand.Intn(len(cities))]
		cypher := fmt.Sprintf("CREATE (p:Person {name: '%s', age: %d, city: '%s'})", name, age, city)
		nodeCh <- cypher
	}
	close(nodeCh)
	wg.Wait()
	close(done)

	nodeElapsed := time.Since(nodeStart)
	fmt.Printf("  nodes complete: %d in %s (%.0f/sec, %d errors)\n\n",
		nodesCreated.Load(), nodeElapsed.Round(time.Millisecond),
		float64(nodesCreated.Load())/nodeElapsed.Seconds(),
		nodeErrors.Load())

	// 3. Generate edges.
	fmt.Printf("Inserting %d edges...\n", totalEdges)
	var edgesCreated atomic.Int64
	var edgeErrors atomic.Int64
	edgeStart := time.Now()

	edgeCh := make(chan string, *workers*2)
	done2 := make(chan struct{})

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := edgesCreated.Load()
				elapsed := time.Since(edgeStart).Seconds()
				rate := float64(n) / elapsed
				pct := float64(n) / float64(totalEdges) * 100
				errs := edgeErrors.Load()
				fmt.Printf("  edges: %d/%d (%.1f%%) @ %.0f/sec [errors: %d]\n", n, totalEdges, pct, rate, errs)
			case <-done2:
				return
			}
		}
	}()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cypher := range edgeCh {
				if err := query(client, cypher); err != nil {
					edgeErrors.Add(1)
				} else {
					edgesCreated.Add(1)
				}
			}
		}()
	}

	// Feed edge creates. Each edge connects two random nodes.
	nodeCount := int(nodesCreated.Load())
	if nodeCount < 2 {
		fmt.Println("  not enough nodes for edges, skipping")
	} else {
		for i := 0; i < totalEdges; i++ {
			a := rand.Intn(nodeCount)
			b := rand.Intn(nodeCount)
			for b == a {
				b = rand.Intn(nodeCount)
			}
			since := 2015 + rand.Intn(11)
			cypher := fmt.Sprintf(
				"MATCH (a:Person {name: '%s'}), (b:Person {name: '%s'}) CREATE (a)-[:KNOWS {since: %d}]->(b)",
				nodeName(a), nodeName(b), since)
			edgeCh <- cypher
		}
	}
	close(edgeCh)
	wg.Wait()
	close(done2)

	edgeElapsed := time.Since(edgeStart)
	totalElapsed := time.Since(nodeStart)
	fmt.Printf("  edges complete: %d in %s (%.0f/sec, %d errors)\n\n",
		edgesCreated.Load(), edgeElapsed.Round(time.Millisecond),
		float64(edgesCreated.Load())/edgeElapsed.Seconds(),
		edgeErrors.Load())

	fmt.Printf("Done! Total: %s\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("  %d nodes + %d edges = %d objects\n",
		nodesCreated.Load(), edgesCreated.Load(),
		nodesCreated.Load()+edgesCreated.Load())

	if nodeErrors.Load() > 0 || edgeErrors.Load() > 0 {
		os.Exit(1)
	}
}

// nodeName generates a deterministic unique name for node index i.
// Format: "FirstName-123456" ensuring uniqueness via the index.
func nodeName(i int) string {
	first := firstNames[i%len(firstNames)]
	return fmt.Sprintf("%s-%d", first, i)
}

func query(client *http.Client, cypher string) error {
	body, _ := json.Marshal(queryReq{Cypher: cypher})
	resp, err := client.Post(*endpoint+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)
		return fmt.Errorf("status %d: %v", resp.StatusCode, result)
	}
	return nil
}

func mustQuery(client *http.Client, cypher string) {
	if err := query(client, cypher); err != nil {
		fmt.Fprintf(os.Stderr, "schema query failed: %v\n  cypher: %s\n", err, cypher)
		os.Exit(1)
	}
}
