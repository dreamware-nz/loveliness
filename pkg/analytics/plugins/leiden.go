package plugins

import (
	"context"
	"fmt"
	"sort"

	"github.com/johnjansen/loveliness/pkg/analytics/leiden"
	"github.com/johnjansen/loveliness/pkg/router"
)

// Leiden runs the Leiden community-detection algorithm on the result
// interpreted as an edge list. Returns per-community size histogram,
// modularity, and (optionally) per-node community assignments.
//
// Params:
//
//	src    (string, default "src")     — column with source node id
//	dst    (string, default "dst")     — column with destination node id
//	weight (string, optional)          — column with edge weight (default 1.0)
//	gamma  (float64, default 1.0)      — resolution; >1 finer, <1 coarser
//	seed   (int64, default 0)          — RNG seed for determinism
//	max_iter (int, default 32)         — outer-iteration cap
//	include_assignments (bool, default false)
//	    — if true, the result includes "assignments": map[id]community.
//	      Off by default because for large graphs this dominates the
//	      response payload; clients that want it must opt in.
type Leiden struct{}

func (Leiden) Name() string { return "leiden" }

func (Leiden) Compute(_ context.Context, result *router.Result, params map[string]any) (any, error) {
	srcCol := stringOr(params, "src", "src")
	dstCol := stringOr(params, "dst", "dst")
	weightCol, _ := params["weight"].(string)
	gamma := float64Or(params, "gamma", 1.0)
	seed := int64Or(params, "seed", 0)
	maxIter := intOr(params, "max_iter", 0)
	includeAssignments, _ := params["include_assignments"].(bool)

	if !columnExists(result.Columns, srcCol) {
		return nil, fmt.Errorf("leiden: src column %q not in result", srcCol)
	}
	if !columnExists(result.Columns, dstCol) {
		return nil, fmt.Errorf("leiden: dst column %q not in result", dstCol)
	}
	if weightCol != "" && !columnExists(result.Columns, weightCol) {
		return nil, fmt.Errorf("leiden: weight column %q not in result", weightCol)
	}
	if gamma < 0 {
		return nil, fmt.Errorf("leiden: gamma must be ≥ 0, got %v", gamma)
	}

	// Build the graph. Each unique node id becomes an integer index.
	// We iterate twice over rows: first to assign indices in
	// stable first-seen order (so the output is deterministic for a
	// given input order); second to add edges.
	idIndex := map[string]int{}
	nodeIDs := []string{}
	indexOf := func(id string) int {
		if i, ok := idIndex[id]; ok {
			return i
		}
		i := len(nodeIDs)
		idIndex[id] = i
		nodeIDs = append(nodeIDs, id)
		return i
	}
	for _, row := range result.Rows {
		s := keyOf(row[srcCol])
		d := keyOf(row[dstCol])
		indexOf(s)
		indexOf(d)
	}

	g := leiden.NewGraph(len(nodeIDs))
	for _, row := range result.Rows {
		u := idIndex[keyOf(row[srcCol])]
		v := idIndex[keyOf(row[dstCol])]
		w := 1.0
		if weightCol != "" {
			w = floatOf(row[weightCol])
		}
		if w <= 0 {
			continue
		}
		g.AddEdge(u, v, w)
	}

	res := leiden.Run(g, gamma, seed, maxIter)

	// Build size histogram (descending).
	sizes := make([]int, res.NumComms)
	for _, c := range res.Communities {
		sizes[c]++
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))

	out := map[string]any{
		"num_communities": res.NumComms,
		"num_nodes":       len(nodeIDs),
		"modularity":      res.Modularity,
		"iterations":      res.Iterations,
		"gamma":           gamma,
		"size_histogram":  sizes,
	}
	if includeAssignments {
		assign := make(map[string]int, len(nodeIDs))
		for i, id := range nodeIDs {
			assign[id] = res.Communities[i]
		}
		out["assignments"] = assign
	}
	return out, nil
}

// float64Or extracts a float64 from JSON params (which arrive as
// float64 from encoding/json) with a default fallback.
func float64Or(m map[string]any, k string, def float64) float64 {
	if m == nil {
		return def
	}
	switch v := m[k].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return def
}

// int64Or extracts an int64 from params; encoding/json gives float64
// for numbers so we accept both.
func int64Or(m map[string]any, k string, def int64) int64 {
	if m == nil {
		return def
	}
	switch v := m[k].(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	}
	return def
}

func intOr(m map[string]any, k string, def int) int {
	return int(int64Or(m, k, int64(def)))
}

// floatOf coerces a row cell to float64. Returns 0 for non-numeric
// values, which AddEdge then skips.
func floatOf(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	}
	return 0
}
