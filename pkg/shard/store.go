package shard

// QueryResponse represents the result of a Cypher query against a shard.
type QueryResponse struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Stats   QueryStats       `json:"stats,omitempty"`
}

// QueryStats contains timing information for a query.
type QueryStats struct {
	CompileTimeMs float64 `json:"compile_time_ms,omitempty"`
	ExecTimeMs    float64 `json:"exec_time_ms,omitempty"`
}

// Store is the interface for shard storage backends.
// The production implementation uses LadybugDB via go-ladybug CGo bindings.
// Tests use MemoryStore.
type Store interface {
	// Query executes a Cypher query and returns the results.
	Query(cypher string) (*QueryResponse, error)
	// Close releases all resources held by the store.
	Close() error
}
