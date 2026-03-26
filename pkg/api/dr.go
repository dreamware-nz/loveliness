package api

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/johnjansen/loveliness/pkg/backup"
	"github.com/johnjansen/loveliness/pkg/replication"
)

// DRExtension holds disaster recovery dependencies injected into the Server.
type DRExtension struct {
	BackupMgr  *backup.Manager
	WAL        *replication.WAL
	ReplicaState *replication.ReplicaState
}

// SetDR attaches DR capabilities to the server.
func (s *Server) SetDR(dr *DRExtension) {
	s.dr = dr
}

// registerDRRoutes adds DR endpoints to the mux. Called from Handler().
func (s *Server) registerDRRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /backup", s.handleBackup)
	mux.HandleFunc("POST /restore", s.handleRestore)
	mux.HandleFunc("GET /export/{table}", s.handleExport)
	mux.HandleFunc("GET /export/{table}/edges/{relTable}", s.handleExportEdges)
	mux.HandleFunc("GET /wal/status", s.handleWALStatus)
	mux.HandleFunc("GET /wal/catchup/{shardID}", s.handleWALCatchup)
}

// handleBackup streams a compressed tar archive of all shard data.
// GET /backup → application/gzip
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if s.dr == nil || s.dr.BackupMgr == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_DR", "backup not configured", 0)
		return
	}

	var walSeq uint64
	if s.dr.WAL != nil {
		walSeq = s.dr.WAL.LastSequence()
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=loveliness-backup.tar.gz")

	manifest, err := s.dr.BackupMgr.CreateBackup(w, s.shards, walSeq)
	if err != nil {
		slog.Error("backup failed", "err", err)
		// Can't write error JSON since we already started streaming.
		return
	}
	slog.Info("backup complete",
		"shards", manifest.ShardCount,
		"wal_seq", manifest.WALSequence)
}

// handleRestore accepts a compressed tar archive and restores shard data.
// POST /restore (Content-Type: application/gzip)
// WARNING: This overwrites existing shard data. Server should be drained first.
func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	if s.dr == nil || s.dr.BackupMgr == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_DR", "backup not configured", 0)
		return
	}

	manifest, err := s.dr.BackupMgr.RestoreBackup(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "RESTORE_ERROR", err.Error(), 0)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "restored",
		"shards":      manifest.ShardCount,
		"wal_sequence": manifest.WALSequence,
		"created_at":  manifest.CreatedAt,
		"note":        "restart the server to load restored data",
	})
}

// handleExport streams all rows of a node table as CSV.
// GET /export/Person → text/csv
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if tableName == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "table name required", 0)
		return
	}

	pkColumn := "name" // default
	if s.schema != nil {
		if pk := s.schema.GetShardKey(tableName); pk != "" {
			pkColumn = pk
		}
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%s.csv", tableName))

	count, err := backup.ExportTable(w, s.shards, tableName, pkColumn)
	if err != nil {
		slog.Error("export failed", "table", tableName, "err", err)
		return
	}
	slog.Info("export complete", "table", tableName, "rows", count)
}

// handleExportEdges streams all edges of a relationship type as CSV.
// GET /export/Person/edges/KNOWS → text/csv
func (s *Server) handleExportEdges(w http.ResponseWriter, r *http.Request) {
	fromTable := r.PathValue("table")
	relTable := r.PathValue("relTable")
	toTable := r.URL.Query().Get("to")
	if toTable == "" {
		toTable = fromTable // default: same table for both endpoints
	}

	if fromTable == "" || relTable == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "table and relTable required", 0)
		return
	}

	fromPK := "name"
	toPK := "name"
	if s.schema != nil {
		if pk := s.schema.GetShardKey(fromTable); pk != "" {
			fromPK = pk
		}
		if pk := s.schema.GetShardKey(toTable); pk != "" {
			toPK = pk
		}
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%s_%s.csv", fromTable, relTable))

	count, err := backup.ExportEdges(w, s.shards, relTable, fromTable, toTable, fromPK, toPK)
	if err != nil {
		slog.Error("export edges failed", "rel", relTable, "err", err)
		return
	}
	slog.Info("export edges complete", "rel", relTable, "rows", count)
}

// handleWALStatus returns the current WAL state for all shards.
// GET /wal/status
func (s *Server) handleWALStatus(w http.ResponseWriter, r *http.Request) {
	if s.dr == nil || s.dr.WAL == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_DR", "WAL not configured", 0)
		return
	}

	status := map[string]any{
		"last_sequence": s.dr.WAL.LastSequence(),
	}

	shardSeqs := make(map[string]uint64)
	for _, sh := range s.shards {
		shardSeqs[fmt.Sprintf("shard-%d", sh.ID)] = s.dr.WAL.ShardSequence(sh.ID)
	}
	status["shards"] = shardSeqs

	if s.dr.ReplicaState != nil && s.cluster != nil {
		sm := s.cluster.GetShardMap()
		replicas := make(map[string]any)
		for shardID, assignment := range sm.Assignments {
			if assignment.Replica != "" {
				walHead := s.dr.WAL.ShardSequence(shardID)
				pos := s.dr.ReplicaState.GetPosition(shardID, assignment.Replica)
				replicas[fmt.Sprintf("shard-%d", shardID)] = map[string]any{
					"replica":  assignment.Replica,
					"position": pos,
					"lag":      walHead - pos,
				}
			}
		}
		status["replicas"] = replicas
	}

	writeJSON(w, http.StatusOK, status)
}

// handleWALCatchup returns WAL entries for a shard after a given sequence.
// GET /wal/catchup/0?after=42
func (s *Server) handleWALCatchup(w http.ResponseWriter, r *http.Request) {
	if s.dr == nil || s.dr.WAL == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_DR", "WAL not configured", 0)
		return
	}

	shardIDStr := r.PathValue("shardID")
	var shardID int
	fmt.Sscanf(shardIDStr, "%d", &shardID)

	var afterSeq uint64
	if v := r.URL.Query().Get("after"); v != "" {
		fmt.Sscanf(v, "%d", &afterSeq)
	}

	entries, err := s.dr.WAL.ReadFrom(shardID, afterSeq)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "WAL_READ_ERROR", err.Error(), shardID)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"shard_id": shardID,
		"after":    afterSeq,
		"entries":  entries,
		"count":    len(entries),
	})
}
