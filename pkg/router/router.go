package router

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// ShardError records a per-shard failure during scatter-gather.
type ShardError struct {
	ShardID int    `json:"shard_id"`
	Error   string `json:"error"`
}

// Result is the merged response from one or more shards.
type Result struct {
	Columns []string           `json:"columns"`
	Rows    []map[string]any   `json:"rows"`
	Partial bool               `json:"partial,omitempty"`
	Errors  []ShardError       `json:"errors,omitempty"`
	Stats   shard.QueryStats   `json:"stats,omitempty"`
}

// Router routes parsed Cypher queries to the appropriate shards.
//
//	                  ┌──────────────┐
//	  Query ────────► │  Parse       │
//	                  │  Cypher      │
//	                  └──────┬───────┘
//	                         │
//	                  ┌──────▼───────┐
//	                  │  Resolve     │──► single shard ──► direct query
//	                  │  Shard(s)    │──► cross-shard  ──► multi-hop resolve
//	                  └──────┬───────┘
//	                         │
//	                         └──► all shards ──► scatter-gather ──► merge
type Router struct {
	shards     []*shard.Shard
	shardCount int
	timeout    time.Duration
	schema     *schema.Registry
	resolver   *Resolver

	// Phase B: Bloom filter index for shard-targeted lookups.
	bloomIndex *ShardBloomIndex

	// Phase C: Edge-cut replicator for border node full-property replication.
	edgeCut *EdgeCutReplicator

	// Phase D: Partition analyzer for community detection.
	partitioner *PartitionAnalyzer

	// Phase E: Pipeline executor for overlapping multi-hop traversals.
	pipeline *PipelineExecutor
}

// NewRouter creates a Router over the given shards.
func NewRouter(shards []*shard.Shard, timeout time.Duration) *Router {
	return &Router{
		shards:     shards,
		shardCount: len(shards),
		timeout:    timeout,
	}
}

// NewRouterWithSchema creates a Router with schema-aware shard key routing
// and cross-shard resolution.
func NewRouterWithSchema(shards []*shard.Shard, timeout time.Duration, reg *schema.Registry) *Router {
	r := &Router{
		shards:     shards,
		shardCount: len(shards),
		timeout:    timeout,
		schema:     reg,
		bloomIndex: NewShardBloomIndex(),
	}
	r.resolver = NewResolver(shards, reg, r)
	r.edgeCut = NewEdgeCutReplicator(shards, r)
	r.partitioner = NewPartitionAnalyzer(shards, r)
	r.pipeline = NewPipelineExecutor(shards, r, timeout)
	return r
}

// BloomIndex returns the Bloom filter index for external use (e.g., bulk load populates it).
func (r *Router) BloomIndex() *ShardBloomIndex {
	return r.bloomIndex
}

// EdgeCut returns the edge-cut replicator.
func (r *Router) EdgeCut() *EdgeCutReplicator {
	return r.edgeCut
}

// Partitioner returns the partition analyzer.
func (r *Router) Partitioner() *PartitionAnalyzer {
	return r.partitioner
}

// Pipeline returns the pipeline executor.
func (r *Router) Pipeline() *PipelineExecutor {
	return r.pipeline
}

// Execute parses a Cypher query, resolves target shards, and executes.
func (r *Router) Execute(ctx context.Context, cypher string) (*Result, error) {
	parsed, err := ParseWithSchema(cypher, r.schema)
	if err != nil {
		return nil, &QueryError{Code: "CYPHER_PARSE_ERROR", Message: err.Error()}
	}

	// Schema DDL (CREATE NODE TABLE, DROP, ALTER) must run on ALL shards.
	if parsed.Type == QuerySchema {
		return r.broadcast(ctx, parsed)
	}

	// Writes without a shard key are unsafe — we can't scatter-gather mutations.
	if parsed.IsWrite() && parsed.NeedsScatterGather() {
		return nil, &QueryError{
			Code:    "MISSING_SHARD_KEY",
			Message: "write queries require a shard key (property value) for routing; use inline properties {prop: 'value'} or WHERE equality",
		}
	}

	if parsed.NeedsScatterGather() {
		return r.scatterGather(ctx, parsed)
	}

	// Cross-shard edge creation: write with 2+ shard keys on different shards.
	if parsed.IsWrite() && len(parsed.ShardKeys) >= 2 && parsed.Traversal != nil && r.resolver != nil {
		if r.isCrossShard(parsed.ShardKeys) {
			return r.resolver.HandleCrossShardEdgeCreate(ctx, parsed)
		}
	}

	// Cross-shard traversal: read with traversal pattern.
	if !parsed.IsWrite() && parsed.Traversal != nil && r.resolver != nil && len(parsed.ShardKeys) > 0 {
		return r.resolver.ResolveTraversal(ctx, parsed)
	}

	// Phase B: Use Bloom filter to refine shard routing.
	// If the Bloom index is populated, it may narrow the target to fewer shards
	// (or confirm the hash-based shard). This is especially useful after
	// graph-aware repartitioning (Phase D) when keys may have migrated.
	shardID := r.resolveShard(parsed.ShardKeys[0])
	if r.bloomIndex != nil && !parsed.IsWrite() {
		likely := r.bloomIndex.LikelyShards(parsed.ShardKeys[0])
		if len(likely) == 1 {
			shardID = likely[0]
		}
	}
	return r.queryShard(ctx, shardID, parsed.Raw)
}

// broadcast sends a query to ALL shards (used for schema DDL).
// All shards must succeed; any failure is an error.
func (r *Router) broadcast(ctx context.Context, parsed *ParsedQuery) (*Result, error) {
	type shardResult struct {
		shardID int
		resp    *shard.QueryResponse
		err     error
	}

	var wg sync.WaitGroup
	results := make(chan shardResult, r.shardCount)

	for _, s := range r.shards {
		wg.Add(1)
		go func(s *shard.Shard) {
			defer wg.Done()
			resp, err := s.Query(parsed.Raw)
			results <- shardResult{shardID: s.ID, resp: resp, err: err}
		}(s)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var errors []ShardError
	for res := range results {
		if res.err != nil {
			errors = append(errors, ShardError{ShardID: res.shardID, Error: res.err.Error()})
		}
	}

	if len(errors) > 0 {
		return &Result{Errors: errors, Partial: true}, &QueryError{
			Code:    "BROADCAST_PARTIAL",
			Message: fmt.Sprintf("schema change failed on %d/%d shards", len(errors), r.shardCount),
		}
	}

	return &Result{Columns: []string{}, Rows: []map[string]any{}}, nil
}

// resolveShard hashes a key to a shard ID.
func (r *Router) resolveShard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(r.shardCount))
}

// ResolveShardForKey is exported for use by the API layer (write forwarding).
func (r *Router) ResolveShardForKey(key string) int {
	return r.resolveShard(key)
}

// isCrossShard returns true if the shard keys resolve to more than one shard.
func (r *Router) isCrossShard(keys []string) bool {
	if len(keys) < 2 {
		return false
	}
	first := r.resolveShard(keys[0])
	for _, key := range keys[1:] {
		if r.resolveShard(key) != first {
			return true
		}
	}
	return false
}

// queryShard sends a query to a single shard.
func (r *Router) queryShard(ctx context.Context, shardID int, cypher string) (*Result, error) {
	if shardID < 0 || shardID >= len(r.shards) {
		return nil, &QueryError{Code: "INVALID_SHARD", Message: fmt.Sprintf("shard %d out of range", shardID)}
	}
	s := r.shards[shardID]
	if !s.IsHealthy() {
		return nil, &QueryError{Code: "SHARD_UNAVAILABLE", Message: fmt.Sprintf("shard %d is unhealthy", shardID), ShardID: shardID}
	}

	type queryResult struct {
		resp *shard.QueryResponse
		err  error
	}
	ch := make(chan queryResult, 1)
	go func() {
		resp, err := s.Query(cypher)
		ch <- queryResult{resp, err}
	}()

	select {
	case <-ctx.Done():
		return nil, &QueryError{Code: "QUERY_TIMEOUT", Message: "query timed out"}
	case <-time.After(r.timeout):
		return nil, &QueryError{Code: "QUERY_TIMEOUT", Message: fmt.Sprintf("shard %d timed out after %s", shardID, r.timeout)}
	case res := <-ch:
		if res.err != nil {
			return nil, &QueryError{Code: "QUERY_ERROR", Message: res.err.Error(), ShardID: shardID}
		}
		return &Result{
			Columns: res.resp.Columns,
			Rows:    res.resp.Rows,
			Stats:   res.resp.Stats,
		}, nil
	}
}

// scatterGather sends the query to ALL shards and merges results.
// It applies query pushdown (rewriting AVG→SUM+COUNT for distributed aggregation)
// and post-merge operations (ORDER BY, LIMIT, DISTINCT, aggregate merging).
func (r *Router) scatterGather(ctx context.Context, parsed *ParsedQuery) (*Result, error) {
	type shardResult struct {
		shardID int
		resp    *shard.QueryResponse
		err     error
	}

	// Phase A: Parse query modifiers for aggregate pushdown.
	mods := parseQueryModifiers(parsed.Raw)
	shardQuery := parsed.Raw
	if mods.HasAggregates() {
		shardQuery = rewriteQueryForShards(parsed.Raw, mods)
	}

	var wg sync.WaitGroup
	results := make(chan shardResult, r.shardCount)

	for _, s := range r.shards {
		if !s.IsHealthy() {
			results <- shardResult{shardID: s.ID, err: fmt.Errorf("shard %d is unhealthy", s.ID)}
			continue
		}
		wg.Add(1)
		go func(s *shard.Shard) {
			defer wg.Done()
			resp, err := s.Query(shardQuery)
			results <- shardResult{shardID: s.ID, resp: resp, err: err}
		}(s)
	}

	// Close results channel after all goroutines complete.
	go func() {
		wg.Wait()
		close(results)
	}()

	merged := &Result{}
	var errors []ShardError
	columnsSet := false

	timer := time.NewTimer(r.timeout)
	defer timer.Stop()

	collected := 0
	for collected < r.shardCount {
		select {
		case <-ctx.Done():
			merged.Partial = true
			merged.Errors = append(errors, ShardError{ShardID: -1, Error: "context cancelled"})
			return merged, nil
		case <-timer.C:
			merged.Partial = true
			merged.Errors = append(errors, ShardError{ShardID: -1, Error: "scatter-gather timed out"})
			return merged, nil
		case res, ok := <-results:
			if !ok {
				goto done
			}
			collected++
			if res.err != nil {
				errors = append(errors, ShardError{ShardID: res.shardID, Error: res.err.Error()})
				continue
			}
			if !columnsSet && res.resp != nil {
				merged.Columns = res.resp.Columns
				columnsSet = true
			}
			if res.resp != nil {
				merged.Rows = append(merged.Rows, res.resp.Rows...)
			}
		}
	}

done:
	if len(errors) > 0 {
		merged.Partial = true
		merged.Errors = errors
	}
	// Deduplicate rows: remove reference nodes (PK-only stubs) when the
	// real node with full properties also appears in the results.
	merged.Rows = deduplicateRows(merged.Rows, merged.Columns)

	// Phase A: Post-merge operations — aggregate merging, ORDER BY, LIMIT, DISTINCT.
	if mods.NeedsMerge() {
		if mods.HasAggregates() {
			merged.Rows = mergeGroupedAggregateRows(merged.Rows, merged.Columns, mods)
			// Update columns after aggregate merging (AVG rewrite changes column names).
			if len(merged.Rows) > 0 {
				merged.Columns = resultColumns(merged.Rows[0])
			}
		}
		if mods.HasDistinct {
			merged.Rows = applyDistinct(merged.Rows, merged.Columns)
		}
		if len(mods.OrderBy) > 0 || mods.Limit >= 0 {
			merged.Rows = applyOrderByAndLimit(merged.Rows, mods)
		}
	}

	return merged, nil
}

// resultColumns extracts sorted column names from a result row.
func resultColumns(row map[string]any) []string {
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// deduplicateRows removes rows that are strict subsets of other rows.
// A row A is a subset of row B if every non-null value in A equals the
// corresponding value in B, and B has strictly more non-null values.
// This handles reference nodes (PK-only with nulls) being superseded
// by real nodes (full properties).
func deduplicateRows(rows []map[string]any, columns []string) []map[string]any {
	if len(rows) <= 1 || len(columns) == 0 {
		return rows
	}

	// Build a key for grouping: use the first column as the identity key.
	// Rows with the same value for column[0] are candidates for dedup.
	type group struct {
		rows    []int // indices into the original rows slice
	}
	groups := make(map[string]*group)
	keyCol := columns[0]

	for i, row := range rows {
		keyVal := fmt.Sprintf("%v", row[keyCol])
		g, ok := groups[keyVal]
		if !ok {
			g = &group{}
			groups[keyVal] = g
		}
		g.rows = append(g.rows, i)
	}

	// For each group with multiple rows, drop rows that are strict subsets
	// (have nulls where another row has real values). If all rows in a group
	// have identical non-null counts, keep all of them — they're real
	// duplicates from different shards, not reference vs real node.
	drop := make(map[int]bool)
	for _, g := range groups {
		if len(g.rows) < 2 {
			continue
		}
		// Find the maximum non-null count in this group.
		maxNonNull := 0
		for _, idx := range g.rows {
			if nn := countNonNull(rows[idx]); nn > maxNonNull {
				maxNonNull = nn
			}
		}
		// Only drop rows that have strictly fewer non-null values
		// (these are reference/stub nodes).
		for _, idx := range g.rows {
			if countNonNull(rows[idx]) < maxNonNull {
				drop[idx] = true
			}
		}
	}

	if len(drop) == 0 {
		return rows
	}

	out := make([]map[string]any, 0, len(rows)-len(drop))
	for i, row := range rows {
		if !drop[i] {
			out = append(out, row)
		}
	}
	return out
}

func countNonNull(row map[string]any) int {
	count := 0
	for _, v := range row {
		if v != nil {
			count++
		}
	}
	return count
}

// QueryError is a structured error with a code for the API layer.
type QueryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	ShardID int    `json:"shard_id,omitempty"`
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}
