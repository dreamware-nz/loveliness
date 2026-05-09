// Package plugins houses concrete analytics plugins the spike registers
// at boot. They live here only because this is a spike; production
// plugins would live next to the analytics package or in their own modules.
package plugins

import (
	"context"
	"fmt"

	"github.com/johnjansen/loveliness/pkg/router"
)

// CountByLabel groups rows by the value of one column and counts each group.
// Param: column (string, required).
type CountByLabel struct{}

func (CountByLabel) Name() string { return "count_by_label" }

func (CountByLabel) Compute(_ context.Context, result *router.Result, params map[string]any) (any, error) {
	col, _ := params["column"].(string)
	if col == "" {
		return nil, fmt.Errorf("count_by_label: missing required param 'column'")
	}
	if !columnExists(result.Columns, col) {
		return nil, fmt.Errorf("count_by_label: column %q not in result (have %v)", col, result.Columns)
	}
	counts := map[string]int{}
	for _, row := range result.Rows {
		key := fmt.Sprintf("%v", row[col])
		counts[key]++
	}
	return map[string]any{"counts": counts, "total": len(result.Rows)}, nil
}

func columnExists(cols []string, want string) bool {
	for _, c := range cols {
		if c == want {
			return true
		}
	}
	return false
}
