package bolt

import (
	"context"
	"fmt"

	"github.com/johnjansen/loveliness/pkg/router"
)

// RouterAdapter wraps a Loveliness router to satisfy the QueryExecutor
// interface for the Bolt server.
type RouterAdapter struct {
	router *router.Router
}

// NewRouterAdapter creates a Bolt-compatible query executor from a router.
func NewRouterAdapter(r *router.Router) *RouterAdapter {
	return &RouterAdapter{router: r}
}

// Query executes Cypher via the router and returns columns + rows.
// The params argument is ignored here — interpolation happens before this call.
func (a *RouterAdapter) Query(cypher string, params map[string]any) ([]string, []map[string]any, error) {
	resp, err := a.router.Execute(context.Background(), cypher)
	if err != nil {
		return nil, nil, fmt.Errorf("%v", err)
	}

	// Extract column names from the first row if not provided.
	columns := resp.Columns
	if len(columns) == 0 && len(resp.Rows) > 0 {
		for k := range resp.Rows[0] {
			columns = append(columns, k)
		}
	}

	return columns, resp.Rows, nil
}

// DatabaseQueryExecutor extends QueryExecutor with database-scoped queries
// and admin command support.
type DatabaseQueryExecutor interface {
	// QueryDatabase executes Cypher against a specific database.
	QueryDatabase(dbName, cypher string, params map[string]any) (columns []string, rows []map[string]any, err error)
	// IsAdminCommand returns true if the cypher is a database admin command.
	IsAdminCommand(cypher string) bool
	// ExecuteAdmin executes a database admin command and returns results.
	ExecuteAdmin(cypher string) (columns []string, rows []map[string]any, err error)
}

// DatabaseRouterAdapter wraps a DatabaseRouter to satisfy the DatabaseQueryExecutor interface.
type DatabaseRouterAdapter struct {
	dbRouter *router.DatabaseRouter
	cluster  AdminCluster
}

// AdminCluster provides the cluster operations needed for admin commands.
type AdminCluster interface {
	IsLeader() bool
	LeaderAddr() string
	CreateDatabase(name string, shardCount int) error
	StopDatabase(name string) error
	StartDatabase(name string) error
	DeleteDatabase(name string) error
}

// NewDatabaseRouterAdapter creates a Bolt-compatible database query executor.
func NewDatabaseRouterAdapter(dr *router.DatabaseRouter, c AdminCluster) *DatabaseRouterAdapter {
	return &DatabaseRouterAdapter{dbRouter: dr, cluster: c}
}

func (a *DatabaseRouterAdapter) QueryDatabase(dbName, cypher string, params map[string]any) ([]string, []map[string]any, error) {
	resp, err := a.dbRouter.Execute(context.Background(), dbName, cypher)
	if err != nil {
		return nil, nil, fmt.Errorf("%v", err)
	}

	columns := resp.Columns
	if len(columns) == 0 && len(resp.Rows) > 0 {
		for k := range resp.Rows[0] {
			columns = append(columns, k)
		}
	}
	return columns, resp.Rows, nil
}

func (a *DatabaseRouterAdapter) IsAdminCommand(cypher string) bool {
	return router.IsAdminCommand(cypher)
}

func (a *DatabaseRouterAdapter) ExecuteAdmin(cypher string) ([]string, []map[string]any, error) {
	cmd := router.ParseAdminCommand(cypher)
	if cmd == nil {
		return nil, nil, fmt.Errorf("not an admin command")
	}

	switch cmd.Type {
	case router.AdminShowDatabases:
		cat := a.dbRouter.Catalog()
		dbs := cat.ListDatabases()
		rows := make([]map[string]any, len(dbs))
		for i, db := range dbs {
			rows[i] = map[string]any{
				"name":        db.Name,
				"state":       db.State.String(),
				"shard_count": int64(db.ShardCount),
			}
		}
		return []string{"name", "state", "shard_count"}, rows, nil

	case router.AdminCreateDatabase:
		if err := a.cluster.CreateDatabase(cmd.Name, cmd.ShardCount); err != nil {
			return nil, nil, err
		}
		return []string{"status", "name"}, []map[string]any{{"status": "created", "name": cmd.Name}}, nil

	case router.AdminStopDatabase:
		if err := a.cluster.StopDatabase(cmd.Name); err != nil {
			return nil, nil, err
		}
		return []string{"status", "name"}, []map[string]any{{"status": "stopped", "name": cmd.Name}}, nil

	case router.AdminStartDatabase:
		if err := a.cluster.StartDatabase(cmd.Name); err != nil {
			return nil, nil, err
		}
		return []string{"status", "name"}, []map[string]any{{"status": "started", "name": cmd.Name}}, nil

	case router.AdminDropDatabase:
		if err := a.cluster.DeleteDatabase(cmd.Name); err != nil {
			return nil, nil, err
		}
		return []string{"status", "name"}, []map[string]any{{"status": "deleted", "name": cmd.Name}}, nil
	}

	return nil, nil, fmt.Errorf("unknown admin command")
}
