package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func printUsage() {
	fmt.Print(`Usage: loveliness <command> [args]

Commands:
  serve          Start the server (default if no command given)
  up <N>         Start N local nodes for development
  query <cypher> Run a Cypher query against a running server
  help           Show this help
  version        Show version

Environment variables for 'query':
  LOVELINESS_URL   Server URL (default: http://localhost:8080)

Examples:
  loveliness                                  # start server
  loveliness up 3                             # 3-node local cluster
  loveliness query "MATCH (n) RETURN count(n)"
  loveliness query "CREATE (p:Person {name: 'Alice', age: 30})"
`)
}

func runQuery(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: loveliness query <cypher>")
		os.Exit(1)
	}

	cypher := strings.Join(args, " ")
	baseURL := os.Getenv("LOVELINESS_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	resp, err := http.Post(baseURL+"/cypher", "text/plain", strings.NewReader(cypher))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading response: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	// Pretty-print JSON if valid, otherwise print raw.
	var parsed any
	if err := json.Unmarshal(body, &parsed); err == nil {
		pretty, _ := json.MarshalIndent(parsed, "", "  ")
		fmt.Println(string(pretty))
	} else {
		fmt.Print(string(body))
	}
}
