package shard

import (
	"fmt"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store for testing. It doesn't parse Cypher —
// it stores nodes by a simple key and returns them on any MATCH query.
type MemoryStore struct {
	mu    sync.RWMutex
	nodes map[string]map[string]any // key → properties
	log   []string                 // query log for assertions
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		nodes: make(map[string]map[string]any),
	}
}

// PutNode inserts a node directly (test helper, not via Cypher).
func (s *MemoryStore) PutNode(key string, props map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[key] = props
}

// QueryLog returns all queries received (for test assertions).
func (s *MemoryStore) QueryLog() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.log))
	copy(out, s.log)
	return out
}

// Query records the query and returns all stored nodes.
func (s *MemoryStore) Query(cypher string) (*QueryResponse, error) {
	s.mu.Lock()
	s.log = append(s.log, cypher)
	s.mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	upper := strings.ToUpper(strings.TrimSpace(cypher))
	// Schema DDL, writes, and CREATE data — all return empty success.
	if strings.HasPrefix(upper, "CREATE") || strings.HasPrefix(upper, "DROP") ||
		strings.HasPrefix(upper, "ALTER") || strings.HasPrefix(upper, "MERGE") ||
		strings.Contains(upper, "SET ") || strings.Contains(upper, "DELETE") ||
		strings.Contains(upper, "REMOVE ") {
		return &QueryResponse{Columns: []string{}, Rows: []map[string]any{}}, nil
	}
	if strings.HasPrefix(upper, "MATCH") || strings.HasPrefix(upper, "OPTIONAL MATCH") ||
		strings.HasPrefix(upper, "UNWIND") || strings.HasPrefix(upper, "CALL") {
		rows := make([]map[string]any, 0, len(s.nodes))
		for _, props := range s.nodes {
			rows = append(rows, props)
		}
		columns := []string{"n"}
		if len(rows) > 0 {
			columns = make([]string, 0)
			for k := range rows[0] {
				columns = append(columns, k)
			}
		}
		return &QueryResponse{Columns: columns, Rows: rows}, nil
	}

	return nil, fmt.Errorf("unsupported query in MemoryStore: %s", cypher)
}

// Close is a no-op for MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}
