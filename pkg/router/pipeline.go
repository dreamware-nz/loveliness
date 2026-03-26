package router

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// PipelineStage represents one stage in a multi-hop query pipeline.
// Instead of waiting for all results from stage N before starting stage N+1,
// the pipeline overlaps execution: as shard results stream in from stage N,
// stage N+1 queries begin immediately for resolved keys.
type PipelineStage struct {
	Query     string   // Cypher template with %s placeholder for key
	ShardKeys []string // resolved keys from previous stage
	Label     string   // target node label for shard resolution
}

// PipelineExecutor runs multi-stage traversal queries as an overlapping pipeline.
//
// Traditional execution:
//
//	Stage 1: fan-out → wait all → collect keys
//	Stage 2: fan-out → wait all → collect keys
//	Stage 3: fan-out → wait all → return
//	Total: 3 * RTT
//
// Pipeline execution:
//
//	Stage 1: fan-out → as each result arrives → immediately start Stage 2 for that key
//	Stage 2: as each result arrives → immediately start Stage 3 for that key
//	Total: ~1 * RTT + overlap
type PipelineExecutor struct {
	shards     []*shard.Shard
	shardCount int
	router     *Router
	timeout    time.Duration
}

// NewPipelineExecutor creates a pipeline executor.
func NewPipelineExecutor(shards []*shard.Shard, r *Router, timeout time.Duration) *PipelineExecutor {
	return &PipelineExecutor{
		shards:     shards,
		shardCount: len(shards),
		router:     r,
		timeout:    timeout,
	}
}

// PipelineResult contains results from a pipeline execution.
type PipelineResult struct {
	Rows    []map[string]any
	Columns []string
	Partial bool
	Errors  []ShardError
}

// Execute runs a multi-hop traversal as a streamed pipeline.
// Each hop produces keys that feed into the next hop's queries.
//
// Parameters:
//   - initialQuery: the first-hop Cypher query
//   - keyExtractor: function to extract shard keys from result rows
//   - hops: subsequent hop query templates (use %s for the key placeholder)
//   - shardKeyProp: the shard key property name for routing
func (pe *PipelineExecutor) Execute(
	ctx context.Context,
	initialQuery string,
	keyExtractor func(row map[string]any) string,
	hops []string,
	shardKeyProp string,
) (*PipelineResult, error) {

	result := &PipelineResult{}

	// Stage 1: scatter-gather the initial query.
	stage1Rows, stage1Cols, errs := pe.scatterAll(ctx, initialQuery)
	result.Columns = stage1Cols
	if len(errs) > 0 {
		result.Errors = errs
		result.Partial = true
	}

	if len(hops) == 0 {
		result.Rows = stage1Rows
		return result, nil
	}

	// Pipeline subsequent hops.
	currentRows := stage1Rows
	for hopIdx, hopTemplate := range hops {
		nextRows, cols, hopErrs := pe.pipelineHop(ctx, currentRows, keyExtractor, hopTemplate, shardKeyProp)
		if len(hopErrs) > 0 {
			result.Errors = append(result.Errors, hopErrs...)
			result.Partial = true
		}
		if hopIdx == len(hops)-1 {
			result.Columns = cols
		}
		currentRows = nextRows
	}

	result.Rows = currentRows
	return result, nil
}

// scatterAll sends a query to all healthy shards and collects results.
func (pe *PipelineExecutor) scatterAll(ctx context.Context, cypher string) ([]map[string]any, []string, []ShardError) {
	type shardResult struct {
		shardID int
		resp    *shard.QueryResponse
		err     error
	}

	var wg sync.WaitGroup
	results := make(chan shardResult, pe.shardCount)

	for _, s := range pe.shards {
		if !s.IsHealthy() {
			continue
		}
		wg.Add(1)
		go func(s *shard.Shard) {
			defer wg.Done()
			resp, err := s.Query(cypher)
			results <- shardResult{shardID: s.ID, resp: resp, err: err}
		}(s)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allRows []map[string]any
	var columns []string
	var errors []ShardError
	columnsSet := false

	timer := time.NewTimer(pe.timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			errors = append(errors, ShardError{ShardID: -1, Error: "context cancelled"})
			return allRows, columns, errors
		case <-timer.C:
			errors = append(errors, ShardError{ShardID: -1, Error: "pipeline timed out"})
			return allRows, columns, errors
		case res, ok := <-results:
			if !ok {
				return allRows, columns, errors
			}
			if res.err != nil {
				errors = append(errors, ShardError{ShardID: res.shardID, Error: res.err.Error()})
				continue
			}
			if !columnsSet && res.resp != nil {
				columns = res.resp.Columns
				columnsSet = true
			}
			if res.resp != nil {
				allRows = append(allRows, res.resp.Rows...)
			}
		}
	}
}

// pipelineHop executes the next hop for each unique key from the current rows.
// Keys are extracted, grouped by target shard, and queries fire as soon as keys
// are available — overlapping with extraction from remaining current rows.
func (pe *PipelineExecutor) pipelineHop(
	ctx context.Context,
	currentRows []map[string]any,
	keyExtractor func(row map[string]any) string,
	hopTemplate string,
	shardKeyProp string,
) ([]map[string]any, []string, []ShardError) {

	// Extract unique keys and group by shard.
	type shardBatch struct {
		shardID int
		keys    []string
	}
	keyToShard := make(map[string]int)
	seen := make(map[string]bool)

	for _, row := range currentRows {
		key := keyExtractor(row)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		shardID := pe.router.resolveShard(key)
		keyToShard[key] = shardID
	}

	// Group keys by shard for batched execution.
	shardKeys := make(map[int][]string)
	for key, sid := range keyToShard {
		shardKeys[sid] = append(shardKeys[sid], key)
	}

	// Execute queries across shards with overlapping I/O.
	type hopResult struct {
		rows    []map[string]any
		columns []string
		err     error
		shardID int
	}

	var wg sync.WaitGroup
	results := make(chan hopResult, len(shardKeys))

	for shardID, keys := range shardKeys {
		wg.Add(1)
		go func(sid int, ks []string) {
			defer wg.Done()
			var allRows []map[string]any
			var cols []string
			for _, key := range ks {
				escaped := escapeForCypher(key)
				cypher := fmt.Sprintf(hopTemplate, escaped)
				resp, err := pe.shards[sid].Query(cypher)
				if err != nil {
					results <- hopResult{err: err, shardID: sid}
					return
				}
				if len(resp.Rows) > 0 {
					allRows = append(allRows, resp.Rows...)
					if cols == nil {
						cols = resp.Columns
					}
				}
			}
			results <- hopResult{rows: allRows, columns: cols, shardID: sid}
		}(shardID, keys)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allRows []map[string]any
	var columns []string
	var errors []ShardError
	columnsSet := false

	timer := time.NewTimer(pe.timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			errors = append(errors, ShardError{ShardID: -1, Error: "context cancelled"})
			return allRows, columns, errors
		case <-timer.C:
			errors = append(errors, ShardError{ShardID: -1, Error: "hop timed out"})
			return allRows, columns, errors
		case res, ok := <-results:
			if !ok {
				return allRows, columns, errors
			}
			if res.err != nil {
				errors = append(errors, ShardError{ShardID: res.shardID, Error: res.err.Error()})
				continue
			}
			if !columnsSet && res.columns != nil {
				columns = res.columns
				columnsSet = true
			}
			allRows = append(allRows, res.rows...)
		}
	}
}

// escapeForCypher escapes a string value for use in Cypher literals.
func escapeForCypher(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
