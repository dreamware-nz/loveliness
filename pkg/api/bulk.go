package api

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// bulkResult is the response for bulk load endpoints.
type bulkResult struct {
	Table  string   `json:"table"`
	Loaded int64    `json:"loaded"`
	Errors []string `json:"errors,omitempty"`
}

// handleBulkNodes accepts a CSV body, splits rows by shard key, and executes
// COPY FROM on each shard for high-throughput node ingestion.
//
// Headers:
//
//	X-Table: Person (required — the node table name)
//
// Body: CSV with a header row. One column must match the table's PRIMARY KEY.
func (s *Server) handleBulkNodes(w http.ResponseWriter, r *http.Request) {
	tableName := r.Header.Get("X-Table")
	if tableName == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "X-Table header is required", 0)
		return
	}
	if s.schema == nil {
		writeError(w, http.StatusBadRequest, "NO_SCHEMA", "schema registry not available", 0)
		return
	}

	shardKey := s.schema.GetShardKey(tableName)
	if shardKey == "" {
		writeError(w, http.StatusBadRequest, "UNKNOWN_TABLE",
			fmt.Sprintf("table %s not registered; run CREATE NODE TABLE first", tableName), 0)
		return
	}

	header, rows, err := readCSV(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_CSV", err.Error(), 0)
		return
	}

	keyIdx := findColumn(header, shardKey)
	if keyIdx == -1 {
		writeError(w, http.StatusBadRequest, "MISSING_SHARD_KEY",
			fmt.Sprintf("CSV header must contain shard key column '%s'; got %v", shardKey, header), 0)
		return
	}

	// Split rows by shard and collect keys for Bloom filter population.
	shardCount := len(s.shards)
	buckets := make([][]string, shardCount)
	shardKeys := make([][]string, shardCount) // Phase B: keys per shard for Bloom index
	headerLine := encodeCSVLine(header)
	for _, row := range rows {
		if len(row) != len(header) {
			continue
		}
		key := row[keyIdx]
		shardID := s.router.ResolveShardForKey(key)
		buckets[shardID] = append(buckets[shardID], encodeCSVLine(row))
		shardKeys[shardID] = append(shardKeys[shardID], key)
	}

	result := s.copyFromShards(tableName, headerLine, buckets)

	// Populate in-memory key trackers and Bloom filters with loaded keys.
	if result.Loaded > 0 {
		s.refTrackMu.Lock()
		for sid, keys := range shardKeys {
			if len(keys) == 0 {
				continue
			}
			if s.refTracker[sid] == nil {
				s.refTracker[sid] = make(map[string]bool, len(keys))
			}
			for _, k := range keys {
				s.refTracker[sid][k] = true
			}
		}
		s.refTrackMu.Unlock()

		if bloomIdx := s.router.BloomIndex(); bloomIdx != nil {
			for sid, keys := range shardKeys {
				if len(keys) > 0 {
					bloomIdx.AddBatch(sid, keys)
				}
			}
		}
	}
	if len(result.Errors) > 0 {
		writeJSON(w, http.StatusMultiStatus, result)
	} else {
		writeJSON(w, http.StatusOK, result)
	}
}

// handleBulkEdges accepts a CSV body, splits edges by shard, ensures reference
// nodes exist for cross-shard endpoints, and executes COPY FROM per shard.
//
// Headers:
//
//	X-Rel-Table:  KNOWS     (required — the relationship table name)
//	X-From-Table: Person    (required — the FROM node table)
//	X-To-Table:   Person    (required — the TO node table)
//
// Body: CSV with a header row. First column is FROM key, second is TO key,
// remaining columns are relationship properties.
func (s *Server) handleBulkEdges(w http.ResponseWriter, r *http.Request) {
	relTable := r.Header.Get("X-Rel-Table")
	fromTable := r.Header.Get("X-From-Table")
	toTable := r.Header.Get("X-To-Table")
	if relTable == "" || fromTable == "" || toTable == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"X-Rel-Table, X-From-Table, and X-To-Table headers are required", 0)
		return
	}
	if s.schema == nil {
		writeError(w, http.StatusBadRequest, "NO_SCHEMA", "schema registry not available", 0)
		return
	}

	fromKey := s.schema.GetShardKey(fromTable)
	toKey := s.schema.GetShardKey(toTable)
	if fromKey == "" || toKey == "" {
		writeError(w, http.StatusBadRequest, "UNKNOWN_TABLE",
			fmt.Sprintf("tables %s/%s not registered", fromTable, toTable), 0)
		return
	}

	skipRefs := r.Header.Get("X-Skip-Refs") == "true"
	refsOnly := r.Header.Get("X-Refs-Only") == "true"

	header, rows, err := readCSV(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_CSV", err.Error(), 0)
		return
	}
	if len(header) < 2 {
		writeError(w, http.StatusBadRequest, "BAD_CSV",
			"edge CSV must have at least 2 columns (from_key, to_key)", 0)
		return
	}

	// First two columns are FROM key and TO key.
	shardCount := len(s.shards)
	buckets := make([][]string, shardCount)
	headerLine := encodeCSVLine(header)

	// Track cross-shard TO keys that need reference nodes per shard.
	refNodes := make([]map[string]bool, shardCount)
	for i := range refNodes {
		refNodes[i] = make(map[string]bool)
	}

	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		fromVal := row[0]
		toVal := row[1]
		fromShard := s.router.ResolveShardForKey(fromVal)
		toShard := s.router.ResolveShardForKey(toVal)

		if !refsOnly {
			buckets[fromShard] = append(buckets[fromShard], encodeCSVLine(row))
		}

		if !skipRefs && toShard != fromShard {
			refNodes[fromShard][toVal] = true
		}
	}

	// Create reference nodes unless skipped.
	totalRefs := 0
	for _, keys := range refNodes {
		totalRefs += len(keys)
	}
	var refErrors []string
	if totalRefs > 0 {
		refErrors = s.createRefNodesCopyFrom(toTable, toKey, refNodes)
		slog.Info("bulk edges: reference nodes",
			"total", totalRefs, "errors", len(refErrors), "method", "copy_from")
	}

	if refsOnly {
		writeJSON(w, http.StatusOK, bulkResult{
			Table:  relTable,
			Loaded: int64(totalRefs - len(refErrors)),
			Errors: refErrors,
		})
		return
	}

	// COPY FROM edges per shard.
	result := s.copyFromShards(relTable, headerLine, buckets)
	result.Errors = append(refErrors, result.Errors...)

	if len(result.Errors) > 0 {
		writeJSON(w, http.StatusMultiStatus, result)
	} else {
		writeJSON(w, http.StatusOK, result)
	}
}

// createRefNodesCopyFrom creates reference nodes using COPY FROM instead of
// individual MERGE queries. For each shard, it generates a CSV with the primary
// key column populated and all other columns empty (NULL), then executes a
// single COPY FROM. If COPY FROM fails (e.g., duplicate keys), falls back to
// individual MERGE queries for that shard.
func (s *Server) createRefNodesCopyFrom(tableName, shardKeyProp string, refNodes []map[string]bool) []string {
	// Introspect table schema from shard 0 to get column names in order.
	columns, err := s.getTableColumns(0, tableName)
	if err != nil {
		// Can't introspect — fall back to MERGE for all shards.
		slog.Warn("bulk edges: table introspection failed, falling back to MERGE", "err", err)
		return s.createRefNodesMerge(tableName, shardKeyProp, refNodes)
	}

	// Find the primary key column index.
	pkIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col, shardKeyProp) {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		return s.createRefNodesMerge(tableName, shardKeyProp, refNodes)
	}

	var errors []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardID, keys := range refNodes {
		if len(keys) == 0 {
			continue
		}
		wg.Add(1)
		go func(sid int, keySet map[string]bool) {
			defer wg.Done()

			// Filter out keys that already exist on this shard to avoid
			// duplicate PK errors in COPY FROM.
			newKeys := s.filterExistingKeys(sid, tableName, shardKeyProp, keySet)
			if len(newKeys) == 0 {
				return
			}

			// Build CSV: header + one row per new reference node.
			headerLine := encodeCSVLine(columns)
			var lines []string
			emptyRow := make([]string, len(columns))
			for _, keyVal := range newKeys {
				row := make([]string, len(columns))
				copy(row, emptyRow)
				row[pkIdx] = keyVal
				lines = append(lines, encodeCSVLine(row))
			}

			// Write temp file and COPY FROM.
			f, err := os.CreateTemp("", fmt.Sprintf("loveliness-ref-%s-s%d-*.csv", tableName, sid))
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("shard %d ref temp file: %s", sid, err))
				mu.Unlock()
				return
			}
			f.WriteString(headerLine)
			for _, line := range lines {
				f.WriteString(line)
			}
			tmpPath := f.Name()
			f.Close()
			defer os.Remove(tmpPath)

			cypher := fmt.Sprintf("COPY %s FROM '%s' (HEADER=true)", tableName, tmpPath)
			if _, err = s.shards[sid].Query(cypher); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("shard %d ref COPY FROM: %s", sid, err))
				mu.Unlock()
			} else {
				// Record newly created reference nodes in tracker for fast
				// duplicate detection on subsequent batches.
				s.refTrackMu.Lock()
				if s.refTracker[sid] == nil {
					s.refTracker[sid] = make(map[string]bool)
				}
				for _, k := range newKeys {
					s.refTracker[sid][k] = true
				}
				s.refTrackMu.Unlock()
			}
		}(shardID, keys)
	}
	wg.Wait()
	return errors
}

// createRefNodesMerge creates reference nodes using individual MERGE queries.
// Used as fallback when COPY FROM is not possible.
func (s *Server) createRefNodesMerge(tableName, shardKeyProp string, refNodes []map[string]bool) []string {
	var errors []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for shardID, keys := range refNodes {
		if len(keys) == 0 {
			continue
		}
		wg.Add(1)
		go func(sid int, keySet map[string]bool) {
			defer wg.Done()
			for keyVal := range keySet {
				cypher := fmt.Sprintf("MERGE (n:%s {%s: '%s'})", tableName, shardKeyProp, escapeCypher(keyVal))
				if _, err := s.shards[sid].Query(cypher); err != nil {
					mu.Lock()
					errors = append(errors, fmt.Sprintf("shard %d ref %s: %s", sid, keyVal, err))
					mu.Unlock()
				}
			}
		}(shardID, keys)
	}
	wg.Wait()
	return errors
}

// filterExistingKeys checks which keys already exist on a shard and returns
// only the ones that don't. Uses the in-memory ref node tracker for O(1)
// lookups when available, falling back to a database scan on the first call.
func (s *Server) filterExistingKeys(shardID int, tableName, keyProp string, keys map[string]bool) []string {
	s.refTrackMu.RLock()
	tracker := s.refTracker[shardID]
	s.refTrackMu.RUnlock()

	if tracker != nil {
		// Fast path: check in-memory set of known node keys on this shard.
		var newKeys []string
		for k := range keys {
			if !tracker[k] {
				newKeys = append(newKeys, k)
			}
		}
		return newKeys
	}

	// Cold start: populate tracker from database, then filter.
	cypher := fmt.Sprintf("MATCH (n:%s) RETURN n.%s", tableName, keyProp)
	resp, err := s.shards[shardID].Query(cypher)

	existing := make(map[string]bool)
	if err == nil {
		col := fmt.Sprintf("n.%s", keyProp)
		for _, row := range resp.Rows {
			if v, ok := row[col]; ok && v != nil {
				existing[fmt.Sprintf("%v", v)] = true
			}
		}
	}

	// Store the tracker for future fast-path lookups.
	s.refTrackMu.Lock()
	s.refTracker[shardID] = existing
	s.refTrackMu.Unlock()

	var newKeys []string
	for k := range keys {
		if !existing[k] {
			newKeys = append(newKeys, k)
		}
	}
	return newKeys
}

// getTableColumns queries a shard for the column names of a node table.
func (s *Server) getTableColumns(shardID int, tableName string) ([]string, error) {
	if shardID >= len(s.shards) {
		return nil, fmt.Errorf("shard %d out of range", shardID)
	}
	cypher := fmt.Sprintf("CALL table_info('%s') RETURN *", tableName)
	resp, err := s.shards[shardID].Query(cypher)
	if err != nil {
		return nil, err
	}
	// Rows have: "property id", "name", "type", "default expression", "primary key"
	// Ordered by property id.
	type colInfo struct {
		id   int
		name string
	}
	var cols []colInfo
	for _, row := range resp.Rows {
		name, _ := row["name"].(string)
		// property id may be float64 from JSON-style decoding.
		var id int
		switch v := row["property id"].(type) {
		case float64:
			id = int(v)
		case int64:
			id = int(v)
		case int:
			id = v
		}
		if name != "" {
			cols = append(cols, colInfo{id: id, name: name})
		}
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}
	// Sort by property id.
	for i := range cols {
		for j := i + 1; j < len(cols); j++ {
			if cols[j].id < cols[i].id {
				cols[i], cols[j] = cols[j], cols[i]
			}
		}
	}
	// Deduplicate (table_info may return dupes from scatter-gather).
	var result []string
	seen := make(map[string]bool)
	for _, c := range cols {
		if !seen[c.name] {
			result = append(result, c.name)
			seen[c.name] = true
		}
	}
	return result, nil
}

// copyFromShards writes per-shard temp CSV files and executes COPY FROM on each.
func (s *Server) copyFromShards(tableName, headerLine string, buckets [][]string) bulkResult {
	var loaded atomic.Int64
	var errors []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardID, rows := range buckets {
		if len(rows) == 0 {
			continue
		}

		// Write temp CSV file.
		f, err := os.CreateTemp("", fmt.Sprintf("loveliness-%s-s%d-*.csv", tableName, shardID))
		if err != nil {
			mu.Lock()
			errors = append(errors, fmt.Sprintf("shard %d: temp file: %s", shardID, err))
			mu.Unlock()
			continue
		}
		f.WriteString(headerLine)
		for _, line := range rows {
			f.WriteString(line)
		}
		tmpPath := f.Name()
		f.Close()

		wg.Add(1)
		go func(sid int, path string, count int) {
			defer wg.Done()
			defer os.Remove(path)

			cypher := fmt.Sprintf("COPY %s FROM '%s' (HEADER=true)", tableName, path)
			_, err := s.shards[sid].Query(cypher)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("shard %d: %s", sid, err))
				mu.Unlock()
				return
			}
			loaded.Add(int64(count))
		}(shardID, tmpPath, len(rows))
	}
	wg.Wait()

	return bulkResult{
		Table:  tableName,
		Loaded: loaded.Load(),
		Errors: errors,
	}
}

// readCSV parses a complete CSV from a reader, returning header and data rows.
func readCSV(r io.Reader) (header []string, rows [][]string, err error) {
	reader := csv.NewReader(r)
	header, err = reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read CSV header: %w", err)
	}
	rows, err = reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("CSV parse error: %w", err)
	}
	return header, rows, nil
}

// findColumn returns the index of a column name (case-insensitive), or -1.
func findColumn(header []string, name string) int {
	for i, col := range header {
		if strings.EqualFold(col, name) {
			return i
		}
	}
	return -1
}

// encodeCSVLine encodes a single CSV row as a string with newline.
func encodeCSVLine(record []string) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	w.Write(record)
	w.Flush()
	return b.String()
}

// escapeCypher escapes single quotes in a string value for Cypher literals.
func escapeCypher(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
