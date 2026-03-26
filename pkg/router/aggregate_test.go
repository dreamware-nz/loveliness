package router

import (
	"strings"
	"testing"
)

func TestParseQueryModifiers_Count(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN count(p)")
	if !mods.HasAggregates() {
		t.Fatal("expected aggregates")
	}
	if len(mods.Aggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(mods.Aggregates))
	}
	if mods.Aggregates[0].Func != AggCount {
		t.Errorf("expected COUNT, got %d", mods.Aggregates[0].Func)
	}
}

func TestParseQueryModifiers_AvgWithAlias(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN avg(p.age) AS average")
	if len(mods.Aggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(mods.Aggregates))
	}
	agg := mods.Aggregates[0]
	if agg.Func != AggAvg {
		t.Errorf("expected AVG, got %d", agg.Func)
	}
	if agg.Alias != "average" {
		t.Errorf("expected alias 'average', got '%s'", agg.Alias)
	}
}

func TestParseQueryModifiers_OrderByAndLimit(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 10")
	if len(mods.OrderBy) != 1 {
		t.Fatalf("expected 1 order by column, got %d", len(mods.OrderBy))
	}
	if mods.OrderBy[0].Column != "p.age" {
		t.Errorf("expected column 'p.age', got '%s'", mods.OrderBy[0].Column)
	}
	if !mods.OrderBy[0].Desc {
		t.Error("expected DESC")
	}
	if mods.Limit != 10 {
		t.Errorf("expected limit 10, got %d", mods.Limit)
	}
}

func TestParseQueryModifiers_Distinct(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN DISTINCT p.city")
	if !mods.HasDistinct {
		t.Error("expected DISTINCT")
	}
}

func TestParseQueryModifiers_MultipleAggregates(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN count(p), avg(p.age), min(p.age), max(p.age)")
	if len(mods.Aggregates) != 4 {
		t.Fatalf("expected 4 aggregates, got %d", len(mods.Aggregates))
	}
	expected := []AggregateFunc{AggCount, AggAvg, AggMin, AggMax}
	for i, agg := range mods.Aggregates {
		if agg.Func != expected[i] {
			t.Errorf("aggregate %d: expected %d, got %d", i, expected[i], agg.Func)
		}
	}
}

func TestRewriteQueryForShards_AvgToSumCount(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN avg(p.age)")
	rewritten := rewriteQueryForShards("MATCH (p:Person) RETURN avg(p.age)", mods)
	if rewritten == "MATCH (p:Person) RETURN avg(p.age)" {
		t.Error("expected query to be rewritten")
	}
	// Should contain sum and count instead of avg.
	if !containsStr(rewritten, "sum(p.age)") {
		t.Errorf("expected sum(p.age), got: %s", rewritten)
	}
	if !containsStr(rewritten, "count(p.age)") {
		t.Errorf("expected count(p.age), got: %s", rewritten)
	}
}

func TestRewriteQueryForShards_NoAvg_NoRewrite(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN count(p)")
	rewritten := rewriteQueryForShards("MATCH (p:Person) RETURN count(p)", mods)
	if rewritten != "MATCH (p:Person) RETURN count(p)" {
		t.Errorf("expected no rewrite, got: %s", rewritten)
	}
}

func TestMergeAggregateRows_Count(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN count(p)")
	rows := []map[string]any{
		{"COUNT(p)": float64(10)},
		{"COUNT(p)": float64(15)},
		{"COUNT(p)": float64(5)},
	}
	result := mergeAggregateRows(rows, mods)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	val := toFloat64(result[0]["COUNT(p)"])
	if val != 30 {
		t.Errorf("expected 30, got %v", val)
	}
}

func TestMergeAggregateRows_MinMax(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN min(p.age), max(p.age)")
	rows := []map[string]any{
		{"MIN(p.age)": float64(20), "MAX(p.age)": float64(50)},
		{"MIN(p.age)": float64(18), "MAX(p.age)": float64(65)},
		{"MIN(p.age)": float64(25), "MAX(p.age)": float64(45)},
	}
	result := mergeAggregateRows(rows, mods)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	minVal := toFloat64(result[0]["MIN(p.age)"])
	maxVal := toFloat64(result[0]["MAX(p.age)"])
	if minVal != 18 {
		t.Errorf("expected min 18, got %v", minVal)
	}
	if maxVal != 65 {
		t.Errorf("expected max 65, got %v", maxVal)
	}
}

func TestMergeGroupedAggregateRows(t *testing.T) {
	mods := parseQueryModifiers("MATCH (p:Person) RETURN p.city, count(p)")
	columns := []string{"p.city", "COUNT(p)"}
	rows := []map[string]any{
		{"p.city": "Auckland", "COUNT(p)": float64(10)},
		{"p.city": "Wellington", "COUNT(p)": float64(5)},
		{"p.city": "Auckland", "COUNT(p)": float64(8)},
		{"p.city": "Wellington", "COUNT(p)": float64(3)},
	}
	result := mergeGroupedAggregateRows(rows, columns, mods)
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}
	for _, row := range result {
		city := row["p.city"]
		count := toFloat64(row["COUNT(p)"])
		switch city {
		case "Auckland":
			if count != 18 {
				t.Errorf("Auckland: expected 18, got %v", count)
			}
		case "Wellington":
			if count != 8 {
				t.Errorf("Wellington: expected 8, got %v", count)
			}
		default:
			t.Errorf("unexpected city: %v", city)
		}
	}
}

func TestApplyOrderByAndLimit(t *testing.T) {
	mods := &QueryModifiers{
		OrderBy: []OrderCol{{Column: "age", Desc: true}},
		Limit:   2,
	}
	rows := []map[string]any{
		{"name": "Alice", "age": float64(30)},
		{"name": "Bob", "age": float64(50)},
		{"name": "Charlie", "age": float64(20)},
		{"name": "Dave", "age": float64(40)},
	}
	result := applyOrderByAndLimit(rows, mods)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0]["name"] != "Bob" {
		t.Errorf("expected Bob first, got %v", result[0]["name"])
	}
	if result[1]["name"] != "Dave" {
		t.Errorf("expected Dave second, got %v", result[1]["name"])
	}
}

func TestApplyDistinct(t *testing.T) {
	columns := []string{"city"}
	rows := []map[string]any{
		{"city": "Auckland"},
		{"city": "Wellington"},
		{"city": "Auckland"},
		{"city": "Wellington"},
		{"city": "Christchurch"},
	}
	result := applyDistinct(rows, columns)
	if len(result) != 3 {
		t.Errorf("expected 3 distinct cities, got %d", len(result))
	}
}

func TestSplitReturnColumns(t *testing.T) {
	parts := splitReturnColumns("count(p), avg(p.age), p.city")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d: %v", len(parts), parts)
	}
}

func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}
