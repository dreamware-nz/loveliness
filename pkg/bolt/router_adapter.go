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
