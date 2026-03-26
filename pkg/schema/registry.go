package schema

import (
	"fmt"
	"strings"
	"sync"
)

// TableSchema holds the shard key configuration for a node table.
type TableSchema struct {
	Name     string `json:"name"`
	ShardKey string `json:"shard_key"` // property name used for shard routing
}

// Registry tracks schema metadata for all node tables in the cluster.
// It maps table names to their shard key property, so the router knows
// which property value to hash for shard routing.
//
// Thread-safe. State is replicated via Raft FSM.
type Registry struct {
	mu     sync.RWMutex
	tables map[string]TableSchema // table name (uppercase) → schema
}

// NewRegistry creates an empty schema registry.
func NewRegistry() *Registry {
	return &Registry{
		tables: make(map[string]TableSchema),
	}
}

// Register adds or updates a table's schema.
func (r *Registry) Register(name, shardKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tables[strings.ToUpper(name)] = TableSchema{
		Name:     name,
		ShardKey: shardKey,
	}
}

// GetShardKey returns the shard key property for a table, or "" if unknown.
func (r *Registry) GetShardKey(tableName string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ts, ok := r.tables[strings.ToUpper(tableName)]; ok {
		return ts.ShardKey
	}
	return ""
}

// GetTable returns the full schema for a table.
func (r *Registry) GetTable(tableName string) (TableSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ts, ok := r.tables[strings.ToUpper(tableName)]
	return ts, ok
}

// Remove drops a table from the registry.
func (r *Registry) Remove(tableName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables, strings.ToUpper(tableName))
}

// All returns a copy of all registered table schemas.
func (r *Registry) All() map[string]TableSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]TableSchema, len(r.tables))
	for k, v := range r.tables {
		out[k] = v
	}
	return out
}

// Restore replaces all schemas (used by Raft snapshot restore).
func (r *Registry) Restore(tables map[string]TableSchema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tables = tables
}

// ParseCreateNodeTable extracts the table name and PRIMARY KEY property from
// a CREATE NODE TABLE statement.
//
// Supported forms:
//
//	CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))
//	CREATE NODE TABLE Person(name STRING PRIMARY KEY, age INT64)
func ParseCreateNodeTable(cypher string) (tableName, primaryKey string, err error) {
	trimmed := strings.TrimSpace(cypher)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "CREATE NODE TABLE") {
		return "", "", fmt.Errorf("not a CREATE NODE TABLE statement")
	}

	// Extract table name: comes after "CREATE NODE TABLE", handling variable whitespace.
	rest := strings.TrimSpace(trimmed[len("CREATE NODE TABLE"):])
	// Table name ends at '(' or whitespace
	nameEnd := strings.IndexAny(rest, "( \t")
	if nameEnd == -1 {
		return "", "", fmt.Errorf("missing table definition")
	}
	tableName = strings.TrimSpace(rest[:nameEnd])

	// Find the parenthesized column definitions
	parenStart := strings.IndexByte(rest, '(')
	if parenStart == -1 {
		return "", "", fmt.Errorf("missing column definitions")
	}
	parenEnd := strings.LastIndexByte(rest, ')')
	if parenEnd == -1 || parenEnd <= parenStart {
		return "", "", fmt.Errorf("malformed column definitions")
	}
	body := rest[parenStart+1 : parenEnd]
	upperBody := strings.ToUpper(body)

	// Try: PRIMARY KEY(prop) at the end
	if idx := strings.Index(upperBody, "PRIMARY KEY("); idx != -1 {
		keyStart := idx + len("PRIMARY KEY(")
		keyEnd := strings.IndexByte(upperBody[keyStart:], ')')
		if keyEnd == -1 {
			return "", "", fmt.Errorf("malformed PRIMARY KEY clause")
		}
		primaryKey = strings.TrimSpace(body[idx+len("PRIMARY KEY(") : idx+len("PRIMARY KEY(")+keyEnd])
		return tableName, primaryKey, nil
	}

	// Try: prop TYPE PRIMARY KEY inline
	if idx := strings.Index(upperBody, "PRIMARY KEY"); idx != -1 {
		// Walk backwards from PRIMARY KEY to find the column name
		before := strings.TrimSpace(body[:idx])
		// Find the last comma before this point, or start of body
		lastComma := strings.LastIndexByte(before, ',')
		var colDef string
		if lastComma == -1 {
			colDef = before
		} else {
			colDef = before[lastComma+1:]
		}
		colDef = strings.TrimSpace(colDef)
		// Column name is the first token
		parts := strings.Fields(colDef)
		if len(parts) == 0 {
			return "", "", fmt.Errorf("cannot find column name for PRIMARY KEY")
		}
		primaryKey = parts[0]
		return tableName, primaryKey, nil
	}

	return "", "", fmt.Errorf("no PRIMARY KEY found in CREATE NODE TABLE")
}

// ParseDropTable extracts the table name from a DROP TABLE statement.
func ParseDropTable(cypher string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if !strings.HasPrefix(upper, "DROP TABLE") {
		return "", fmt.Errorf("not a DROP TABLE statement")
	}
	rest := strings.TrimSpace(cypher[len("DROP TABLE"):])
	// Table name is the next token
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", fmt.Errorf("missing table name")
	}
	return parts[0], nil
}
