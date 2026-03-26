package router

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// AggregateFunc represents a recognized aggregate function.
type AggregateFunc int

const (
	AggCount AggregateFunc = iota
	AggSum
	AggAvg
	AggMin
	AggMax
)

// AggregateCol describes one aggregate column in a RETURN clause.
type AggregateCol struct {
	Func     AggregateFunc
	Arg      string // original argument, e.g. "p.age" or "p._ID"
	Alias    string // result column name as LadybugDB returns it, e.g. "COUNT(p._ID)"
	Original string // original expression text, e.g. "count(p)"
}

// QueryModifiers captures ORDER BY, LIMIT, DISTINCT from the query.
type QueryModifiers struct {
	OrderBy    []OrderCol
	Limit      int  // -1 if no LIMIT
	HasDistinct bool
	Aggregates []AggregateCol
}

// OrderCol is one ORDER BY column.
type OrderCol struct {
	Column string
	Desc   bool
}

// parseQueryModifiers extracts aggregate functions, ORDER BY, LIMIT,
// and DISTINCT from a Cypher query for scatter-gather merging.
func parseQueryModifiers(cypher string) *QueryModifiers {
	mods := &QueryModifiers{Limit: -1}
	upper := strings.ToUpper(cypher)

	// DISTINCT
	if idx := strings.Index(upper, "RETURN DISTINCT "); idx != -1 {
		mods.HasDistinct = true
	}

	// Aggregates in RETURN clause.
	mods.Aggregates = extractAggregates(cypher)

	// ORDER BY
	if idx := strings.Index(upper, "ORDER BY "); idx != -1 {
		mods.OrderBy = parseOrderBy(cypher[idx+9:])
	}

	// LIMIT
	if idx := strings.Index(upper, "LIMIT "); idx != -1 {
		rest := strings.TrimSpace(cypher[idx+6:])
		// Take the first token as the limit value.
		end := strings.IndexAny(rest, " \t\n;")
		if end == -1 {
			end = len(rest)
		}
		if n, err := strconv.Atoi(rest[:end]); err == nil {
			mods.Limit = n
		}
	}

	return mods
}

// HasAggregates returns true if the query has aggregate functions to merge.
func (m *QueryModifiers) HasAggregates() bool {
	return len(m.Aggregates) > 0
}

// NeedsMerge returns true if any post-processing is needed on scatter-gather results.
func (m *QueryModifiers) NeedsMerge() bool {
	return m.HasAggregates() || len(m.OrderBy) > 0 || m.Limit >= 0 || m.HasDistinct
}

// extractAggregates finds aggregate function calls in the RETURN clause.
func extractAggregates(cypher string) []AggregateCol {
	upper := strings.ToUpper(cypher)
	retIdx := strings.LastIndex(upper, "RETURN ")
	if retIdx == -1 {
		return nil
	}

	// Handle RETURN DISTINCT
	retClause := cypher[retIdx+7:]
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(retClause)), "DISTINCT ") {
		retClause = strings.TrimSpace(retClause)[9:]
	}

	// Truncate at ORDER BY, LIMIT, SKIP.
	for _, term := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := strings.Index(strings.ToUpper(retClause), term); idx != -1 {
			retClause = retClause[:idx]
		}
	}

	var aggs []AggregateCol
	parts := splitReturnColumns(retClause)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if agg, ok := parseAggregateExpr(part); ok {
			aggs = append(aggs, agg)
		}
	}
	return aggs
}

// splitReturnColumns splits a RETURN clause on commas, respecting parentheses.
func splitReturnColumns(clause string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range clause {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, clause[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, clause[start:])
	return parts
}

// parseAggregateExpr parses "count(p)", "avg(p.age) AS average", etc.
func parseAggregateExpr(expr string) (AggregateCol, bool) {
	expr = strings.TrimSpace(expr)

	// Strip AS alias.
	alias := ""
	if idx := strings.Index(strings.ToUpper(expr), " AS "); idx != -1 {
		alias = strings.TrimSpace(expr[idx+4:])
		expr = strings.TrimSpace(expr[:idx])
	}

	upper := strings.ToUpper(expr)
	var fn AggregateFunc
	var prefix string

	switch {
	case strings.HasPrefix(upper, "COUNT("):
		fn = AggCount
		prefix = "COUNT("
	case strings.HasPrefix(upper, "SUM("):
		fn = AggSum
		prefix = "SUM("
	case strings.HasPrefix(upper, "AVG("):
		fn = AggAvg
		prefix = "AVG("
	case strings.HasPrefix(upper, "MIN("):
		fn = AggMin
		prefix = "MIN("
	case strings.HasPrefix(upper, "MAX("):
		fn = AggMax
		prefix = "MAX("
	default:
		return AggregateCol{}, false
	}

	// Extract argument.
	closeIdx := strings.LastIndexByte(expr, ')')
	if closeIdx == -1 {
		return AggregateCol{}, false
	}
	arg := strings.TrimSpace(expr[len(prefix):closeIdx])

	col := AggregateCol{
		Func:     fn,
		Arg:      arg,
		Original: expr,
	}
	if alias != "" {
		col.Alias = alias
	}
	return col, true
}

// parseOrderBy parses "p.age DESC, p.name ASC" into OrderCol slices.
func parseOrderBy(clause string) []OrderCol {
	// Truncate at LIMIT, SKIP.
	upper := strings.ToUpper(clause)
	for _, term := range []string{"LIMIT", "SKIP"} {
		if idx := strings.Index(upper, term); idx != -1 {
			clause = clause[:idx]
		}
	}

	var cols []OrderCol
	parts := strings.Split(clause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		desc := false
		upper := strings.ToUpper(part)
		if strings.HasSuffix(upper, " DESC") {
			desc = true
			part = strings.TrimSpace(part[:len(part)-5])
		} else if strings.HasSuffix(upper, " ASC") {
			part = strings.TrimSpace(part[:len(part)-4])
		}
		cols = append(cols, OrderCol{Column: part, Desc: desc})
	}
	return cols
}

// rewriteQueryForShards rewrites a query for shard-local execution.
// For AVG: replaces avg(X) with sum(X) and count(X) so the router can
// compute the correct global average from shard-local results.
func rewriteQueryForShards(cypher string, mods *QueryModifiers) string {
	if !mods.HasAggregates() {
		return cypher
	}

	// Check if any aggregate is AVG — only then do we need to rewrite.
	hasAvg := false
	for _, agg := range mods.Aggregates {
		if agg.Func == AggAvg {
			hasAvg = true
			break
		}
	}
	if !hasAvg {
		return cypher
	}

	// Rewrite: replace avg(X) with sum(X), count(X) in the RETURN clause.
	upper := strings.ToUpper(cypher)
	retIdx := strings.LastIndex(upper, "RETURN ")
	if retIdx == -1 {
		return cypher
	}

	prefix := cypher[:retIdx+7]
	rest := cypher[retIdx+7:]

	// Find the end of the RETURN clause.
	retEnd := len(rest)
	for _, term := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := strings.Index(strings.ToUpper(rest), term); idx != -1 && idx < retEnd {
			retEnd = idx
		}
	}
	retClause := rest[:retEnd]
	suffix := rest[retEnd:]

	// Replace each avg(X) with sum(X), count(X).
	newClause := retClause
	for _, agg := range mods.Aggregates {
		if agg.Func == AggAvg {
			// Case-insensitive replacement of avg(arg) with sum(arg), count(arg)
			oldExpr := agg.Original
			newExpr := fmt.Sprintf("sum(%s), count(%s)", agg.Arg, agg.Arg)
			// Try case-insensitive replacement.
			idx := strings.Index(strings.ToUpper(newClause), strings.ToUpper(oldExpr))
			if idx != -1 {
				newClause = newClause[:idx] + newExpr + newClause[idx+len(oldExpr):]
			}
		}
	}

	return prefix + newClause + suffix
}

// mergeAggregateRows merges per-shard aggregate results into a single row.
func mergeAggregateRows(rows []map[string]any, mods *QueryModifiers) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	merged := make(map[string]any)

	// For non-AVG aggregates, merge directly from the result columns.
	// For AVG, we need to combine sum/count columns.
	for _, agg := range mods.Aggregates {
		switch agg.Func {
		case AggCount:
			merged[findResultCol(rows[0], agg)] = sumNumeric(rows, findResultCol(rows[0], agg))
		case AggSum:
			merged[findResultCol(rows[0], agg)] = sumNumeric(rows, findResultCol(rows[0], agg))
		case AggMin:
			col := findResultCol(rows[0], agg)
			merged[col] = minValue(rows, col)
		case AggMax:
			col := findResultCol(rows[0], agg)
			merged[col] = maxValue(rows, col)
		case AggAvg:
			// AVG was rewritten to sum + count. Find those columns.
			sumCol := findColContaining(rows[0], "SUM(", agg.Arg)
			countCol := findColContaining(rows[0], "COUNT(", agg.Arg)
			if sumCol != "" && countCol != "" {
				totalSum := sumNumeric(rows, sumCol)
				totalCount := sumNumeric(rows, countCol)
				var avg float64
				if totalCount != 0 {
					avg = totalSum / totalCount
				}
				// Use the alias or construct the original column name.
				outCol := agg.Alias
				if outCol == "" {
					outCol = fmt.Sprintf("AVG(%s)", agg.Arg)
				}
				merged[outCol] = avg
			}
		}
	}

	// Copy any non-aggregate columns from the first row (for GROUP BY columns).
	for col, val := range rows[0] {
		if _, exists := merged[col]; !exists {
			// Check if this column is an aggregate result column.
			isAgg := false
			upperCol := strings.ToUpper(col)
			for _, prefix := range []string{"COUNT(", "SUM(", "AVG(", "MIN(", "MAX("} {
				if strings.HasPrefix(upperCol, prefix) {
					isAgg = true
					break
				}
			}
			if !isAgg {
				merged[col] = val
			}
		}
	}

	return []map[string]any{merged}
}

// mergeGroupedAggregateRows merges per-shard results that have GROUP BY columns.
// Groups rows by non-aggregate columns, then merges aggregates within each group.
func mergeGroupedAggregateRows(rows []map[string]any, columns []string, mods *QueryModifiers) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	// Identify group-by columns (non-aggregate columns).
	aggCols := make(map[string]bool)
	for _, col := range columns {
		upperCol := strings.ToUpper(col)
		for _, prefix := range []string{"COUNT(", "SUM(", "AVG(", "MIN(", "MAX("} {
			if strings.HasPrefix(upperCol, prefix) {
				aggCols[col] = true
				break
			}
		}
	}

	// If no group-by columns, just merge everything.
	hasGroupBy := false
	for _, col := range columns {
		if !aggCols[col] {
			hasGroupBy = true
			break
		}
	}
	if !hasGroupBy {
		return mergeAggregateRows(rows, mods)
	}

	// Group rows by non-aggregate column values.
	type group struct {
		key  string
		rows []map[string]any
	}
	groups := make(map[string]*group)
	var order []string
	for _, row := range rows {
		var keyParts []string
		for _, col := range columns {
			if !aggCols[col] {
				keyParts = append(keyParts, fmt.Sprintf("%v", row[col]))
			}
		}
		key := strings.Join(keyParts, "|")
		g, ok := groups[key]
		if !ok {
			g = &group{key: key}
			groups[key] = g
			order = append(order, key)
		}
		g.rows = append(g.rows, row)
	}

	// Merge each group.
	var result []map[string]any
	for _, key := range order {
		g := groups[key]
		merged := mergeAggregateRows(g.rows, mods)
		if len(merged) > 0 {
			// Carry forward the group-by columns.
			for _, col := range columns {
				if !aggCols[col] {
					merged[0][col] = g.rows[0][col]
				}
			}
			result = append(result, merged[0])
		}
	}

	return result
}

// applyOrderByAndLimit sorts rows by ORDER BY columns and applies LIMIT.
func applyOrderByAndLimit(rows []map[string]any, mods *QueryModifiers) []map[string]any {
	if len(mods.OrderBy) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			for _, col := range mods.OrderBy {
				vi := rows[i][col.Column]
				vj := rows[j][col.Column]
				cmp := compareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if col.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}

	if mods.Limit >= 0 && mods.Limit < len(rows) {
		rows = rows[:mods.Limit]
	}

	return rows
}

// applyDistinct removes duplicate rows.
func applyDistinct(rows []map[string]any, columns []string) []map[string]any {
	if len(rows) <= 1 {
		return rows
	}
	seen := make(map[string]bool)
	var result []map[string]any
	for _, row := range rows {
		key := rowKey(row, columns)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// --- helpers ---

// findResultCol finds the actual column name in the result that matches an aggregate.
func findResultCol(row map[string]any, agg AggregateCol) string {
	if agg.Alias != "" {
		if _, ok := row[agg.Alias]; ok {
			return agg.Alias
		}
	}
	// LadybugDB often uppercases: COUNT(p._ID), SUM(p.age), etc.
	// Try exact match first.
	for col := range row {
		if strings.EqualFold(col, agg.Original) {
			return col
		}
	}
	// Try matching function + arg pattern.
	return findColContaining(row, strings.ToUpper(aggFuncName(agg.Func))+"(", agg.Arg)
}

func findColContaining(row map[string]any, prefix, arg string) string {
	upperArg := strings.ToUpper(arg)
	for col := range row {
		upperCol := strings.ToUpper(col)
		if strings.HasPrefix(upperCol, prefix) && strings.Contains(upperCol, upperArg) {
			return col
		}
	}
	return ""
}

func aggFuncName(fn AggregateFunc) string {
	switch fn {
	case AggCount:
		return "COUNT"
	case AggSum:
		return "SUM"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	}
	return ""
}

func sumNumeric(rows []map[string]any, col string) float64 {
	var total float64
	for _, row := range rows {
		total += toFloat64(row[col])
	}
	return total
}

func minValue(rows []map[string]any, col string) any {
	var result any
	min := math.MaxFloat64
	for _, row := range rows {
		v := row[col]
		if v == nil {
			continue
		}
		f := toFloat64(v)
		if f < min {
			min = f
			result = v
		}
	}
	return result
}

func maxValue(rows []map[string]any, col string) any {
	var result any
	max := -math.MaxFloat64
	for _, row := range rows {
		v := row[col]
		if v == nil {
			continue
		}
		f := toFloat64(v)
		if f > max {
			max = f
			result = v
		}
	}
	return result
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison.
	fa, okA := numericVal(a)
	fb, okB := numericVal(b)
	if okA && okB {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	// Fall back to string comparison.
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}

func numericVal(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

func rowKey(row map[string]any, columns []string) string {
	var parts []string
	if len(columns) > 0 {
		for _, col := range columns {
			parts = append(parts, fmt.Sprintf("%v", row[col]))
		}
	} else {
		for k, v := range row {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(parts)
	}
	return strings.Join(parts, "|")
}
