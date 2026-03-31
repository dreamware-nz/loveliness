package router

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/catalog"
	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// DatabaseRouter multiplexes queries to per-database Routers.
// It also handles admin commands (CREATE/STOP/START/DROP DATABASE, SHOW DATABASES).
type DatabaseRouter struct {
	mu      sync.RWMutex
	routers map[string]*Router
	schemas map[string]*schema.Registry
	shards  map[string][]*shard.Shard // db name -> shards
	cat     *catalog.Catalog
	timeout time.Duration
}

// NewDatabaseRouter creates a DatabaseRouter backed by the given catalog.
func NewDatabaseRouter(cat *catalog.Catalog, timeout time.Duration) *DatabaseRouter {
	return &DatabaseRouter{
		routers: make(map[string]*Router),
		schemas: make(map[string]*schema.Registry),
		shards:  make(map[string][]*shard.Shard),
		cat:     cat,
		timeout: timeout,
	}
}

// GetRouter returns the Router for a named database, or an error if not found/not online.
func (dr *DatabaseRouter) GetRouter(dbName string) (*Router, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	r, ok := dr.routers[dbName]
	if !ok {
		db := dr.cat.GetDatabase(dbName)
		if db == nil {
			return nil, fmt.Errorf("database %q not found", dbName)
		}
		return nil, fmt.Errorf("database %q is %s", dbName, db.State)
	}
	return r, nil
}

// GetSchema returns the schema registry for a named database.
func (dr *DatabaseRouter) GetSchema(dbName string) *schema.Registry {
	dr.mu.RLock()
	defer dr.mu.RUnlock()
	return dr.schemas[dbName]
}

// RegisterDatabase sets up a Router for a newly created database.
func (dr *DatabaseRouter) RegisterDatabase(dbName string, shards []*shard.Shard) {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	reg := schema.NewRegistry()
	r := NewRouterWithSchema(shards, dr.timeout, reg)
	dr.routers[dbName] = r
	dr.schemas[dbName] = reg
	dr.shards[dbName] = shards
}

// Catalog returns the underlying catalog.
func (dr *DatabaseRouter) Catalog() *catalog.Catalog {
	return dr.cat
}

// UnregisterDatabase removes a database's Router (for stop/delete).
// Returns the shards that were associated with it for cleanup.
func (dr *DatabaseRouter) UnregisterDatabase(dbName string) []*shard.Shard {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	shards := dr.shards[dbName]
	delete(dr.routers, dbName)
	delete(dr.schemas, dbName)
	delete(dr.shards, dbName)
	return shards
}

// Execute runs a query against a specific database.
func (dr *DatabaseRouter) Execute(ctx context.Context, dbName, cypher string) (*Result, error) {
	r, err := dr.GetRouter(dbName)
	if err != nil {
		return nil, &QueryError{Code: "DATABASE_ERROR", Message: err.Error()}
	}
	return r.Execute(ctx, cypher)
}

// IsAdminCommand returns true if the query is a database admin command.
func IsAdminCommand(cypher string) bool {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	return strings.HasPrefix(upper, "CREATE DATABASE") ||
		strings.HasPrefix(upper, "STOP DATABASE") ||
		strings.HasPrefix(upper, "START DATABASE") ||
		strings.HasPrefix(upper, "DROP DATABASE") ||
		strings.HasPrefix(upper, "SHOW DATABASES")
}

// AdminCommandType identifies parsed admin commands.
type AdminCommandType int

const (
	AdminCreateDatabase AdminCommandType = iota
	AdminStopDatabase
	AdminStartDatabase
	AdminDropDatabase
	AdminShowDatabases
)

// AdminCommand is a parsed database admin command.
type AdminCommand struct {
	Type       AdminCommandType
	Name       string // database name (empty for SHOW DATABASES)
	ShardCount int    // only for CREATE DATABASE
}

// ParseAdminCommand extracts the admin command from a Cypher string.
// Returns nil if not an admin command.
func ParseAdminCommand(cypher string) *AdminCommand {
	trimmed := strings.TrimSpace(cypher)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "SHOW DATABASES") {
		return &AdminCommand{Type: AdminShowDatabases}
	}

	if strings.HasPrefix(upper, "CREATE DATABASE") {
		return parseCreateDatabase(trimmed[len("CREATE DATABASE"):])
	}

	if strings.HasPrefix(upper, "STOP DATABASE") {
		name := strings.TrimSpace(trimmed[len("STOP DATABASE"):])
		return &AdminCommand{Type: AdminStopDatabase, Name: strings.ToLower(name)}
	}

	if strings.HasPrefix(upper, "START DATABASE") {
		name := strings.TrimSpace(trimmed[len("START DATABASE"):])
		return &AdminCommand{Type: AdminStartDatabase, Name: strings.ToLower(name)}
	}

	if strings.HasPrefix(upper, "DROP DATABASE") {
		name := strings.TrimSpace(trimmed[len("DROP DATABASE"):])
		return &AdminCommand{Type: AdminDropDatabase, Name: strings.ToLower(name)}
	}

	return nil
}

// parseCreateDatabase parses "name SHARDS n" from the rest of a CREATE DATABASE statement.
func parseCreateDatabase(rest string) *AdminCommand {
	rest = strings.TrimSpace(rest)
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return nil
	}

	cmd := &AdminCommand{
		Type:       AdminCreateDatabase,
		Name:       strings.ToLower(parts[0]),
		ShardCount: 3, // default
	}

	// Look for SHARDS n.
	for i := 1; i < len(parts)-1; i++ {
		if strings.ToUpper(parts[i]) == "SHARDS" {
			var n int
			if _, err := fmt.Sscanf(parts[i+1], "%d", &n); err == nil && n > 0 {
				cmd.ShardCount = n
			}
			break
		}
	}

	return cmd
}
