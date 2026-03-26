package backup

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// ExportTable exports all rows from a node table across all shards as CSV.
// The first row is the header. Duplicate rows (from reference nodes) are
// deduplicated by primary key.
func ExportTable(w io.Writer, shards []*shard.Shard, tableName, pkColumn string) (int64, error) {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	var header []string
	seen := make(map[string]bool) // PK dedup
	var count int64
	headerWritten := false

	for _, s := range shards {
		cypher := fmt.Sprintf("MATCH (n:%s) RETURN n.*", tableName)
		resp, err := s.Query(cypher)
		if err != nil {
			return count, fmt.Errorf("shard %d: %w", s.ID, err)
		}

		if !headerWritten && len(resp.Columns) > 0 {
			header = cleanColumns(resp.Columns, tableName)
			cw.Write(header)
			headerWritten = true
		}

		pkIdx := findCol(header, pkColumn)

		for _, row := range resp.Rows {
			record := rowToRecord(row, resp.Columns, header, tableName)
			if pkIdx >= 0 && pkIdx < len(record) {
				pk := record[pkIdx]
				if seen[pk] {
					continue // skip reference node duplicate
				}
				seen[pk] = true
			}
			cw.Write(record)
			count++
		}
	}
	return count, nil
}

// ExportEdges exports all edges of a relationship type across all shards as CSV.
// Columns: from_key, to_key, plus any relationship properties.
func ExportEdges(w io.Writer, shards []*shard.Shard, relTable, fromTable, toTable, fromPK, toPK string) (int64, error) {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	headerWritten := false
	var header []string
	var count int64
	type edgeKey struct{ from, to string }
	seen := make(map[edgeKey]bool)

	for _, s := range shards {
		cypher := fmt.Sprintf(
			"MATCH (a:%s)-[r:%s]->(b:%s) RETURN a.%s, b.%s, r.*",
			fromTable, relTable, toTable, fromPK, toPK,
		)
		resp, err := s.Query(cypher)
		if err != nil {
			return count, fmt.Errorf("shard %d: %w", s.ID, err)
		}

		if !headerWritten && len(resp.Columns) > 0 {
			header = make([]string, 0, len(resp.Columns))
			header = append(header, "from", "to")
			for _, col := range resp.Columns {
				if col == fmt.Sprintf("a.%s", fromPK) || col == fmt.Sprintf("b.%s", toPK) {
					continue
				}
				header = append(header, cleanColName(col, relTable))
			}
			cw.Write(header)
			headerWritten = true
		}

		fromCol := fmt.Sprintf("a.%s", fromPK)
		toCol := fmt.Sprintf("b.%s", toPK)

		for _, row := range resp.Rows {
			fromVal := fmt.Sprintf("%v", row[fromCol])
			toVal := fmt.Sprintf("%v", row[toCol])
			ek := edgeKey{fromVal, toVal}
			if seen[ek] {
				continue
			}
			seen[ek] = true

			record := []string{fromVal, toVal}
			for _, col := range resp.Columns {
				if col == fromCol || col == toCol {
					continue
				}
				record = append(record, fmt.Sprintf("%v", row[col]))
			}
			cw.Write(record)
			count++
		}
	}
	return count, nil
}

// cleanColumns removes the "n." prefix and table prefix from column names.
func cleanColumns(cols []string, tableName string) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = cleanColName(col, tableName)
	}
	sort.Strings(result)
	return result
}

func cleanColName(col, tableName string) string {
	// "n.name" → "name", "r.since" → "since"
	for _, prefix := range []string{"n.", "r.", "a.", "b."} {
		if len(col) > len(prefix) && col[:len(prefix)] == prefix {
			return col[len(prefix):]
		}
	}
	return col
}

func findCol(header []string, name string) int {
	for i, h := range header {
		if h == name {
			return i
		}
	}
	return -1
}

func rowToRecord(row map[string]any, rawCols, sortedCols []string, tableName string) []string {
	record := make([]string, len(sortedCols))
	for i, col := range sortedCols {
		// Try "n.col" format first (what LadybugDB returns).
		if v, ok := row["n."+col]; ok && v != nil {
			record[i] = fmt.Sprintf("%v", v)
		}
	}
	return record
}
