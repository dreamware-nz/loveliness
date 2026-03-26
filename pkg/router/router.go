package router

import (
	"context"
	"fmt"
	"hash/fnv"
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
//	                  │  Shard(s)    │
//	                  └──────┬───────┘
//	                         │
//	                         └──► all shards ──► scatter-gather ──► merge
type Router struct {
	shards     []*shard.Shard
	shardCount int
	timeout    time.Duration
	schema     *schema.Registry
}

// NewRouter creates a Router over the given shards.
func NewRouter(shards []*shard.Shard, timeout time.Duration) *Router {
	return &Router{
		shards:     shards,
		shardCount: len(shards),
		timeout:    timeout,
	}
}

// NewRouterWithSchema creates a Router with schema-aware shard key routing.
func NewRouterWithSchema(shards []*shard.Shard, timeout time.Duration, reg *schema.Registry) *Router {
	return &Router{
		shards:     shards,
		shardCount: len(shards),
		timeout:    timeout,
		schema:     reg,
	}
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

	// Route to the shard determined by the first shard key.
	shardID := r.resolveShard(parsed.ShardKeys[0])
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
func (r *Router) scatterGather(ctx context.Context, parsed *ParsedQuery) (*Result, error) {
	type shardResult struct {
		shardID int
		resp    *shard.QueryResponse
		err     error
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
			resp, err := s.Query(parsed.Raw)
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
	return merged, nil
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
