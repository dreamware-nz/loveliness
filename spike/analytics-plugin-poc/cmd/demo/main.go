// Demo: boots the spike server with both plugins registered, fires two
// real HTTP requests at it, and pretty-prints the JSON responses.
//
//	go run ./spike/analytics-plugin-poc/cmd/demo
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/analytics"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/plugins"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/server"
)

func main() {
	reg := analytics.NewRegistry()
	must(reg.Register(plugins.CountByLabel{}))
	must(reg.Register(plugins.ConnectedComponents{}))

	// Two canned datasets the stub runner will return depending on the
	// "shape" of the query string. Realistic enough to demonstrate both
	// plugins without needing a live LadybugDB.
	labelRows := []map[string]any{
		{"label": "User"}, {"label": "User"}, {"label": "User"},
		{"label": "Post"}, {"label": "Post"},
		{"label": "Tag"},
	}
	edgeRows := []map[string]any{
		{"src": "a", "dst": "b"}, {"src": "b", "dst": "c"}, {"src": "c", "dst": "a"},
		{"src": "x", "dst": "y"}, {"src": "y", "dst": "z"}, {"src": "z", "dst": "x"},
		{"src": "lone", "dst": "lone2"},
	}

	runner := func(_ context.Context, cypher string) (*router.Result, error) {
		if strings.Contains(cypher, "label") {
			return &router.Result{Columns: []string{"label"}, Rows: labelRows}, nil
		}
		return &router.Result{Columns: []string{"src", "dst"}, Rows: edgeRows}, nil
	}

	srv := httptest.NewServer(server.New(runner, reg, 5*time.Second).Handler())
	defer srv.Close()

	fmt.Println("=== plugin directory ===")
	getJSON(srv.URL + "/analytics")

	fmt.Println("\n=== request 1: count_by_label on a labelled result ===")
	postJSON(srv.URL+"/db/main/query", `{
	  "cypher": "MATCH (n) RETURN n.label AS label",
	  "analytics": [{"name": "count_by_label", "params": {"column": "label"}}]
	}`)

	fmt.Println("\n=== request 2: connected_components on an edge list ===")
	postJSON(srv.URL+"/db/main/query", `{
	  "cypher": "MATCH (s)-[:KNOWS]->(d) RETURN s.id AS src, d.id AS dst",
	  "analytics": [{"name": "connected_components"}]
	}`)

	fmt.Println("\n=== request 3: both plugins at once + an unknown one ===")
	postJSON(srv.URL+"/db/main/query", `{
	  "cypher": "MATCH (s)-[:KNOWS]->(d) RETURN s.id AS src, d.id AS dst",
	  "analytics": [
	    {"name": "connected_components"},
	    {"name": "count_by_label", "params": {"column": "src"}},
	    {"name": "nonexistent"}
	  ]
	}`)

	fmt.Println("\n=== request 4: bare query, no analytics (proves opt-in) ===")
	postJSON(srv.URL+"/db/main/query", `{"cypher":"MATCH (n) RETURN n.label AS label"}`)
}

func postJSON(url, body string) {
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	must(err)
	defer resp.Body.Close()
	dump(resp)
}

func getJSON(url string) {
	resp, err := http.Get(url)
	must(err)
	defer resp.Body.Close()
	dump(resp)
}

func dump(resp *http.Response) {
	raw, _ := io.ReadAll(resp.Body)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		fmt.Println(string(raw))
		return
	}
	fmt.Printf("%s %s\n%s\n", resp.Proto, resp.Status, pretty.String())
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}
