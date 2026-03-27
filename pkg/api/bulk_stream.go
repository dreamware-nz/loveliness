package api

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

const defaultFlushThreshold = 50_000

// handleBulkNodesStream accepts a CSV body and streams rows into shards,
// flushing per-shard batches at a configurable threshold. This bounds memory
// usage to shardCount * flushThreshold * avgRowSize, enabling ingestion of
// datasets that exceed available RAM.
//
// Headers:
//
//	X-Table: Person     (required)
//	X-Flush-Size: 50000 (optional — rows per shard before flushing)
func (s *Server) handleBulkNodesStream(w http.ResponseWriter, r *http.Request) {
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

	flushSize := parseFlushSize(r.Header.Get("X-Flush-Size"))

	reader := csv.NewReader(r.Body)
	header, err := reader.Read()
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_CSV", "cannot read CSV header: "+err.Error(), 0)
		return
	}

	keyIdx := findColumn(header, shardKey)
	if keyIdx == -1 {
		writeError(w, http.StatusBadRequest, "MISSING_SHARD_KEY",
			fmt.Sprintf("CSV header must contain shard key column '%s'; got %v", shardKey, header), 0)
		return
	}

	shardCount := len(s.shards)
	headerLine := encodeCSVLine(header)

	// Streaming state: per-shard row buffers and key buffers.
	buckets := make([][]string, shardCount)
	keyBufs := make([][]string, shardCount)

	var totalLoaded atomic.Int64
	var allErrors []string
	var errMu sync.Mutex

	flush := func(shardID int) {
		if len(buckets[shardID]) == 0 {
			return
		}
		rows := buckets[shardID]
		keys := keyBufs[shardID]
		buckets[shardID] = nil
		keyBufs[shardID] = nil

		result := s.copyFromShards(tableName, headerLine, singleBucket(shardCount, shardID, rows))
		totalLoaded.Add(result.Loaded)
		if len(result.Errors) > 0 {
			errMu.Lock()
			allErrors = append(allErrors, result.Errors...)
			errMu.Unlock()
		}

		// Update trackers.
		if result.Loaded > 0 {
			s.refTrackMu.Lock()
			if s.refTracker[shardID] == nil {
				s.refTracker[shardID] = make(map[string]bool, len(keys))
			}
			for _, k := range keys {
				s.refTracker[shardID][k] = true
			}
			s.refTrackMu.Unlock()

			if bloomIdx := s.router.BloomIndex(); bloomIdx != nil {
				bloomIdx.AddBatch(shardID, keys)
			}
		}
	}

	// Stream rows.
	rowCount := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("bulk stream: CSV parse error, skipping row", "err", err, "row", rowCount)
			continue
		}
		if len(row) != len(header) {
			continue
		}

		key := row[keyIdx]
		shardID := s.router.ResolveShardForKey(key)
		buckets[shardID] = append(buckets[shardID], encodeCSVLine(row))
		keyBufs[shardID] = append(keyBufs[shardID], key)
		rowCount++

		if len(buckets[shardID]) >= flushSize {
			flush(shardID)
		}
	}

	// Flush remaining rows.
	for i := 0; i < shardCount; i++ {
		flush(i)
	}

	result := bulkResult{
		Table:  tableName,
		Loaded: totalLoaded.Load(),
		Errors: allErrors,
	}
	if len(result.Errors) > 0 {
		writeJSON(w, http.StatusMultiStatus, result)
	} else {
		writeJSON(w, http.StatusOK, result)
	}
}

// handleBulkEdgesStream is the streaming variant of handleBulkEdges.
// It processes edges in batches to bound memory usage.
//
// Headers:
//
//	X-Rel-Table:  KNOWS     (required)
//	X-From-Table: Person    (required)
//	X-To-Table:   Person    (required)
//	X-Skip-Refs:  true      (optional — skip reference node creation)
//	X-Flush-Size: 50000     (optional — rows per shard before flushing)
func (s *Server) handleBulkEdgesStream(w http.ResponseWriter, r *http.Request) {
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
	_ = toKey // used implicitly via ResolveShardForKey

	skipRefs := r.Header.Get("X-Skip-Refs") == "true"
	flushSize := parseFlushSize(r.Header.Get("X-Flush-Size"))

	reader := csv.NewReader(r.Body)
	header, err := reader.Read()
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_CSV", "cannot read CSV header: "+err.Error(), 0)
		return
	}
	if len(header) < 2 {
		writeError(w, http.StatusBadRequest, "BAD_CSV",
			"edge CSV must have at least 2 columns (from_key, to_key)", 0)
		return
	}

	shardCount := len(s.shards)
	headerLine := encodeCSVLine(header)

	buckets := make([][]string, shardCount)
	refNodes := make([]map[string]bool, shardCount)
	for i := range refNodes {
		refNodes[i] = make(map[string]bool)
	}

	var totalLoaded atomic.Int64
	var allErrors []string
	var errMu sync.Mutex

	flushEdges := func(shardID int) {
		if len(buckets[shardID]) == 0 {
			return
		}
		rows := buckets[shardID]
		buckets[shardID] = nil

		result := s.copyFromShards(relTable, headerLine, singleBucket(shardCount, shardID, rows))
		totalLoaded.Add(result.Loaded)
		if len(result.Errors) > 0 {
			errMu.Lock()
			allErrors = append(allErrors, result.Errors...)
			errMu.Unlock()
		}
	}

	flushRefs := func() {
		totalRefs := 0
		for _, keys := range refNodes {
			totalRefs += len(keys)
		}
		if totalRefs == 0 {
			return
		}

		refErrors := s.createRefNodesCopyFrom(toTable, toKey, refNodes)
		if len(refErrors) > 0 {
			errMu.Lock()
			allErrors = append(allErrors, refErrors...)
			errMu.Unlock()
		}
		slog.Info("bulk edges stream: ref flush", "total", totalRefs, "errors", len(refErrors))

		// Reset ref tracking for next batch.
		for i := range refNodes {
			refNodes[i] = make(map[string]bool)
		}
	}

	// Stream rows.
	rowCount := 0
	pendingRefs := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("bulk edges stream: CSV parse error", "err", err, "row", rowCount)
			continue
		}
		if len(row) < 2 {
			continue
		}

		fromVal := row[0]
		toVal := row[1]
		fromShard := s.router.ResolveShardForKey(fromVal)
		toShard := s.router.ResolveShardForKey(toVal)

		buckets[fromShard] = append(buckets[fromShard], encodeCSVLine(row))
		rowCount++

		if !skipRefs && toShard != fromShard {
			if !refNodes[fromShard][toVal] {
				refNodes[fromShard][toVal] = true
				pendingRefs++
			}
		}

		// Flush ref nodes before edge flush to ensure they exist.
		if pendingRefs >= flushSize {
			flushRefs()
			pendingRefs = 0
		}

		if len(buckets[fromShard]) >= flushSize {
			// Flush any pending refs first.
			if pendingRefs > 0 {
				flushRefs()
				pendingRefs = 0
			}
			flushEdges(fromShard)
		}
	}

	// Final flush.
	if pendingRefs > 0 {
		flushRefs()
	}
	for i := 0; i < shardCount; i++ {
		flushEdges(i)
	}

	result := bulkResult{
		Table:  relTable,
		Loaded: totalLoaded.Load(),
		Errors: allErrors,
	}
	if len(result.Errors) > 0 {
		writeJSON(w, http.StatusMultiStatus, result)
	} else {
		writeJSON(w, http.StatusOK, result)
	}
}

// singleBucket creates a bucket slice with rows only in the given shard index.
func singleBucket(count, idx int, rows []string) [][]string {
	b := make([][]string, count)
	b[idx] = rows
	return b
}

func parseFlushSize(v string) int {
	if v == "" {
		return defaultFlushThreshold
	}
	var n int
	_, _ = fmt.Sscanf(v, "%d", &n)
	if n < 1000 {
		return defaultFlushThreshold
	}
	return n
}

