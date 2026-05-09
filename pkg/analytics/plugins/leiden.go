package plugins

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"

	"github.com/johnjansen/loveliness/pkg/analytics/leiden"
	"github.com/johnjansen/loveliness/pkg/router"
)

// Leiden runs the Leiden community-detection algorithm on the result
// interpreted as an edge list. Returns per-community size histogram,
// modularity, and (optionally) per-node community assignments.
//
// Resolution modes:
//
//	gamma   (float64, default 1.0) — single resolution; >1 finer, <1 coarser.
//	gammas  ([]float64, optional)  — multi-resolution sweep. If set, the
//	    plugin runs Leiden once per γ in parallel (one goroutine each, all
//	    sharing the same input graph) and returns "partitions": [...] in
//	    the same order. Mutually exclusive with `gamma`.
//
// Other params:
//
//	src    (string, default "src")     — column with source node id
//	dst    (string, default "dst")     — column with destination node id
//	weight (string, optional)          — column with edge weight (default 1.0)
//	seed   (int64, default 0)          — RNG seed for determinism (per γ)
//	max_iter (int, default 32)         — outer-iteration cap (per γ)
//	include_assignments (bool, default false)
//	    — if true, every partition's result includes "assignments":
//	      map[id]community. Off by default because for large graphs this
//	      dominates the response payload; clients that want it must opt in.
type Leiden struct{}

func (Leiden) Name() string { return "leiden" }

func (Leiden) Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	srcCol := stringOr(params, "src", "src")
	dstCol := stringOr(params, "dst", "dst")
	weightCol, _ := params["weight"].(string)
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

	gammas, sweep, err := resolveGammas(params)
	if err != nil {
		return nil, err
	}

	g, nodeIDs := buildLeidenGraph(result, srcCol, dstCol, weightCol)

	if !sweep {
		// Single-resolution mode: flat result shape (unchanged).
		gamma := gammas[0]
		part := runOneGamma(g, gamma, seed, maxIter, nodeIDs, includeAssignments)
		out := map[string]any{
			"num_communities": part["num_communities"],
			"num_nodes":       len(nodeIDs),
			"modularity":      part["modularity"],
			"iterations":      part["iterations"],
			"gamma":           gamma,
			"size_histogram":  part["size_histogram"],
		}
		if includeAssignments {
			out["assignments"] = part["assignments"]
		}
		return out, nil
	}

	// Sweep mode: run γ values in parallel against the shared (read-only)
	// graph. leiden.Run does not mutate g; each call constructs its own
	// working state, so concurrent execution is safe.
	//
	// Concurrency is bounded to runtime.NumCPU() so that a request with a
	// pathologically large gammas list (e.g., 1000 entries) cannot saturate
	// the host. Workers respect ctx between slots: a cancelled request stops
	// admitting new γs immediately, though a γ already running completes
	// (leiden.Run is CPU-bound and does not currently take ctx).
	partitions := make([]map[string]any, len(gammas))
	sem := make(chan struct{}, sweepConcurrency(len(gammas)))
	var wg sync.WaitGroup
	for i, gamma := range gammas {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil, ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(i int, gamma float64) {
			defer wg.Done()
			defer func() { <-sem }()
			partitions[i] = runOneGamma(g, gamma, seed, maxIter, nodeIDs, includeAssignments)
		}(i, gamma)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return map[string]any{
		"num_nodes":  len(nodeIDs),
		"partitions": partitions,
	}, nil
}

// sweepConcurrency caps parallel γ workers to runtime.NumCPU() but never
// allocates more slots than there are γs to run.
func sweepConcurrency(n int) int {
	c := runtime.NumCPU()
	if c < 1 {
		c = 1
	}
	if n < c {
		return n
	}
	return c
}

// resolveGammas picks single-vs-sweep mode and validates inputs.
// Returns the gamma list (length 1 in single mode), a sweep flag, and
// any validation error.
func resolveGammas(params map[string]any) ([]float64, bool, error) {
	_, hasGamma := params["gamma"]
	rawList, hasGammas := params["gammas"]
	if hasGamma && hasGammas {
		return nil, false, fmt.Errorf("leiden: pass either gamma or gammas, not both")
	}

	if hasGammas {
		list, err := toFloat64Slice(rawList)
		if err != nil {
			return nil, false, fmt.Errorf("leiden: gammas: %w", err)
		}
		if len(list) == 0 {
			return nil, false, fmt.Errorf("leiden: gammas must be non-empty")
		}
		for _, g := range list {
			if err := validateGamma(g); err != nil {
				return nil, false, err
			}
		}
		return list, true, nil
	}

	gamma := float64Or(params, "gamma", 1.0)
	if err := validateGamma(gamma); err != nil {
		return nil, false, err
	}
	return []float64{gamma}, false, nil
}

// validateGamma rejects negative, NaN, and ±Inf γ values. NaN in particular
// would otherwise pass `g < 0` (NaN comparisons are false) and produce a
// silent identity partition because every ΔQ comparison in localMove would
// evaluate to false — a wrong-answer failure with no error signal.
func validateGamma(g float64) error {
	if math.IsNaN(g) {
		return fmt.Errorf("leiden: gamma must be a finite real number, got NaN")
	}
	if math.IsInf(g, 0) {
		return fmt.Errorf("leiden: gamma must be a finite real number, got %v", g)
	}
	if g < 0 {
		return fmt.Errorf("leiden: gamma must be ≥ 0, got %v", g)
	}
	return nil
}

// buildLeidenGraph turns edge-list rows into a *leiden.Graph plus the
// stable id→index mapping (returned as the ordered nodeIDs slice).
// Iterates twice over rows: first to assign indices in first-seen order
// (so output is deterministic for a given input), second to add edges.
func buildLeidenGraph(result *router.Result, srcCol, dstCol, weightCol string) (*leiden.Graph, []string) {
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
		indexOf(keyOf(row[srcCol]))
		indexOf(keyOf(row[dstCol]))
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
	return g, nodeIDs
}

// runOneGamma executes Leiden at a single γ and produces the partition
// dict shared between the single-result and sweep paths.
func runOneGamma(g *leiden.Graph, gamma float64, seed int64, maxIter int, nodeIDs []string, includeAssignments bool) map[string]any {
	res := leiden.Run(g, gamma, seed, maxIter)

	sizes := make([]int, res.NumComms)
	for _, c := range res.Communities {
		sizes[c]++
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))

	out := map[string]any{
		"gamma":           gamma,
		"num_communities": res.NumComms,
		"modularity":      res.Modularity,
		"iterations":      res.Iterations,
		"size_histogram":  sizes,
	}
	if includeAssignments {
		assign := make(map[string]int, len(nodeIDs))
		for i, id := range nodeIDs {
			assign[id] = res.Communities[i]
		}
		out["assignments"] = assign
	}
	return out
}

// toFloat64Slice accepts the JSON-decoded forms a `gammas` list can
// take ([]any with float/int entries, or []float64 directly) and
// returns a clean []float64.
func toFloat64Slice(raw any) ([]float64, error) {
	switch v := raw.(type) {
	case []float64:
		out := make([]float64, len(v))
		copy(out, v)
		return out, nil
	case []any:
		out := make([]float64, len(v))
		for i, x := range v {
			switch n := x.(type) {
			case float64:
				out[i] = n
			case float32:
				out[i] = float64(n)
			case int:
				out[i] = float64(n)
			case int64:
				out[i] = float64(n)
			case int32:
				out[i] = float64(n)
			default:
				return nil, fmt.Errorf("entry %d is not a number (%T)", i, x)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected array, got %T", raw)
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
