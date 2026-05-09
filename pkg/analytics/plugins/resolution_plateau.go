package plugins

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/johnjansen/loveliness/pkg/analytics/leiden"
	"github.com/johnjansen/loveliness/pkg/router"
)

// ResolutionPlateau scans γ across a range, runs Leiden at each γ, and
// reports the resulting plateaus — contiguous γ ranges where the
// partition is stable. Plateaus indicate natural community-structure
// scales: γ values inside one plateau give the same answer, so picking
// any representative is safe.
//
// Params:
//
//	src    (string, default "src") — source-id column
//	dst    (string, default "dst") — destination-id column
//	weight (string, optional)      — edge-weight column (default 1.0 each)
//
//	gammas        ([]float64, optional)
//	    — explicit γ sweep. If absent, an auto-range [gamma_min, gamma_max]
//	      is generated with gamma_steps points (linearly spaced).
//	gamma_min     (float64, default 0.1)
//	gamma_max     (float64, default 5.0)
//	gamma_steps   (int,     default 21)
//	    — used only when `gammas` is absent.
//
//	nmi_threshold (float64, default 0.95)
//	    — adjacent γ partitions with NMI ≥ threshold are part of the same
//	      plateau. 1.0 demands identity; lower values widen plateaus.
//
//	seed       (int64, default 0)  — RNG seed for determinism (per γ)
//	max_iter   (int,   default 32) — outer-iteration cap (per γ)
//
//	include_partitions  (bool, default false)
//	    — if true, the response includes every per-γ partition's summary
//	      (gamma, num_communities, modularity, …). Off by default because
//	      it duplicates the plateau payload for the common case.
//	include_assignments (bool, default false)
//	    — if true, every plateau's representative carries the full
//	      id→community map. Required for clients that want to USE the
//	      partition; off by default to keep responses small.
type ResolutionPlateau struct{}

func (ResolutionPlateau) Name() string { return "resolution_plateau" }

func (ResolutionPlateau) Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	srcCol := stringOr(params, "src", "src")
	dstCol := stringOr(params, "dst", "dst")
	weightCol, _ := params["weight"].(string)
	seed := int64Or(params, "seed", 0)
	maxIter := intOr(params, "max_iter", 0)
	includePartitions, _ := params["include_partitions"].(bool)
	includeAssignments, _ := params["include_assignments"].(bool)
	threshold := float64Or(params, "nmi_threshold", 0.95)

	if !columnExists(result.Columns, srcCol) {
		return nil, fmt.Errorf("resolution_plateau: src column %q not in result", srcCol)
	}
	if !columnExists(result.Columns, dstCol) {
		return nil, fmt.Errorf("resolution_plateau: dst column %q not in result", dstCol)
	}
	if weightCol != "" && !columnExists(result.Columns, weightCol) {
		return nil, fmt.Errorf("resolution_plateau: weight column %q not in result", weightCol)
	}
	if math.IsNaN(threshold) || threshold < 0 || threshold > 1 {
		return nil, fmt.Errorf("resolution_plateau: nmi_threshold must be in [0,1], got %v", threshold)
	}

	gammas, err := resolvePlateauGammas(params)
	if err != nil {
		return nil, err
	}

	g, nodeIDs := buildLeidenGraph(result, srcCol, dstCol, weightCol)

	partitions, perGamma, err := runSweep(ctx, g, gammas, seed, maxIter, nodeIDs, includeAssignments)
	if err != nil {
		return nil, err
	}

	plateaus := leiden.DetectPlateaus(gammas, partitions, threshold)
	plateauOut := make([]map[string]any, len(plateaus))
	for i, p := range plateaus {
		entry := map[string]any{
			"gamma_min":            p.GammaMin,
			"gamma_max":            p.GammaMax,
			"num_communities":      p.NumCommunities,
			"representative_gamma": gammas[p.Representative],
		}
		if includeAssignments {
			// Reuse the per-γ map that runSweep already built for the
			// representative index — no second pass needed.
			if a, ok := perGamma[p.Representative]["assignments"]; ok {
				entry["representative_partition"] = a
			}
		}
		plateauOut[i] = entry
	}

	out := map[string]any{
		"num_nodes":     len(nodeIDs),
		"nmi_threshold": threshold,
		"plateaus":      plateauOut,
	}
	if includePartitions {
		out["partitions"] = perGamma
	}
	return out, nil
}

// resolvePlateauGammas picks the γ range, either from an explicit list
// or by linear-spacing gamma_min..gamma_max in gamma_steps points.
func resolvePlateauGammas(params map[string]any) ([]float64, error) {
	if raw, ok := params["gammas"]; ok {
		list, err := toFloat64Slice(raw)
		if err != nil {
			return nil, fmt.Errorf("resolution_plateau: gammas: %w", err)
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("resolution_plateau: gammas must be non-empty")
		}
		for _, g := range list {
			if err := validateGamma(g); err != nil {
				return nil, err
			}
		}
		// Plateau detection assumes ascending γ — sort for safety.
		sorted := make([]float64, len(list))
		copy(sorted, list)
		sort.Float64s(sorted)
		return sorted, nil
	}

	gMin := float64Or(params, "gamma_min", 0.1)
	gMax := float64Or(params, "gamma_max", 5.0)
	steps := intOr(params, "gamma_steps", 21)
	if err := validateGamma(gMin); err != nil {
		return nil, err
	}
	if err := validateGamma(gMax); err != nil {
		return nil, err
	}
	if gMax <= gMin {
		return nil, fmt.Errorf("resolution_plateau: gamma_max (%v) must be > gamma_min (%v)", gMax, gMin)
	}
	if steps < 2 {
		return nil, fmt.Errorf("resolution_plateau: gamma_steps must be ≥ 2, got %d", steps)
	}

	out := make([]float64, steps)
	for i := 0; i < steps; i++ {
		t := float64(i) / float64(steps-1)
		out[i] = gMin + t*(gMax-gMin)
	}
	return out, nil
}

// runSweep runs Leiden once per γ in parallel against the shared
// (read-only) graph and returns both the raw partition slices (used
// by DetectPlateaus) and the per-γ presentable summaries (used by
// the response).
//
// Concurrency mirrors the leiden plugin's sweep: bounded to NumCPU,
// honors ctx cancellation between slots.
func runSweep(ctx context.Context, g *leiden.Graph, gammas []float64, seed int64, maxIter int, nodeIDs []string, includeAssignments bool) ([][]int, []map[string]any, error) {
	partitions := make([][]int, len(gammas))
	summaries := make([]map[string]any, len(gammas))

	sem := make(chan struct{}, sweepConcurrency(len(gammas)))
	var wg sync.WaitGroup
	for i, gamma := range gammas {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil, nil, ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(i int, gamma float64) {
			defer wg.Done()
			defer func() { <-sem }()
			res := leiden.Run(g, gamma, seed, maxIter)
			partitions[i] = res.Communities
			summaries[i] = formatPartition(res, gamma, nodeIDs, includeAssignments)
		}(i, gamma)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return partitions, summaries, nil
}

// formatPartition matches the per-γ shape produced by the leiden
// plugin's sweep mode, so the two plugins return interchangeable
// per-partition records.
func formatPartition(res *leiden.Result, gamma float64, nodeIDs []string, includeAssignments bool) map[string]any {
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

// sweepConcurrency is shared with the leiden plugin's sweep — defined
// in leiden.go.
