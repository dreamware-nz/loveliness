package api

import (
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/johnjansen/loveliness/pkg/ingest"
)

// SetIngestQueue attaches the ingest queue to the server.
func (s *Server) SetIngestQueue(q *ingest.Queue) {
	s.ingestQueue = q
}

// registerIngestRoutes adds ingest queue endpoints to the mux.
func (s *Server) registerIngestRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /ingest/nodes", s.handleIngestNodes)
	mux.HandleFunc("POST /ingest/edges", s.handleIngestEdges)
	mux.HandleFunc("GET /ingest/jobs", s.handleIngestList)
	mux.HandleFunc("GET /ingest/jobs/{id}", s.handleIngestStatus)
}

// handleIngestNodes accepts a CSV body, writes it to disk, and enqueues
// an async node ingest job. Returns immediately with a job ID.
//
// Headers:
//
//	X-Table: Person (required)
//
// Body: CSV with a header row.
func (s *Server) handleIngestNodes(w http.ResponseWriter, r *http.Request) {
	if s.ingestQueue == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_QUEUE", "ingest queue not configured", 0)
		return
	}

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
		writeError(w, http.StatusBadRequest, "UNKNOWN_TABLE", "table not registered", 0)
		return
	}

	dataPath, err := spoolBody(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "SPOOL_ERROR", err.Error(), 0)
		return
	}

	headers := map[string]string{
		"X-Table":     tableName,
		"X-Shard-Key": shardKey,
	}
	jobID, err := s.ingestQueue.Enqueue(ingest.JobTypeNodesStream, headers, dataPath)
	if err != nil {
		os.Remove(dataPath)
		writeError(w, http.StatusInternalServerError, "ENQUEUE_ERROR", err.Error(), 0)
		return
	}

	slog.Info("ingest: node job enqueued", "job_id", jobID, "table", tableName)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"job_id": jobID,
		"status": "pending",
	})
}

// handleIngestEdges accepts a CSV body and enqueues an async edge ingest job.
//
// Headers:
//
//	X-Rel-Table:  KNOWS  (required)
//	X-From-Table: Person (required)
//	X-To-Table:   Person (required)
//	X-Skip-Refs:  true   (optional)
func (s *Server) handleIngestEdges(w http.ResponseWriter, r *http.Request) {
	if s.ingestQueue == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_QUEUE", "ingest queue not configured", 0)
		return
	}

	relTable := r.Header.Get("X-Rel-Table")
	fromTable := r.Header.Get("X-From-Table")
	toTable := r.Header.Get("X-To-Table")
	if relTable == "" || fromTable == "" || toTable == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"X-Rel-Table, X-From-Table, and X-To-Table headers are required", 0)
		return
	}

	dataPath, err := spoolBody(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "SPOOL_ERROR", err.Error(), 0)
		return
	}

	headers := map[string]string{
		"X-Rel-Table":  relTable,
		"X-From-Table": fromTable,
		"X-To-Table":   toTable,
		"X-Skip-Refs":  r.Header.Get("X-Skip-Refs"),
	}
	jobID, err := s.ingestQueue.Enqueue(ingest.JobTypeEdgesStream, headers, dataPath)
	if err != nil {
		os.Remove(dataPath)
		writeError(w, http.StatusInternalServerError, "ENQUEUE_ERROR", err.Error(), 0)
		return
	}

	slog.Info("ingest: edge job enqueued", "job_id", jobID, "rel", relTable)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"job_id": jobID,
		"status": "pending",
	})
}

// handleIngestList returns all ingest jobs.
// GET /ingest/jobs
func (s *Server) handleIngestList(w http.ResponseWriter, r *http.Request) {
	if s.ingestQueue == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_QUEUE", "ingest queue not configured", 0)
		return
	}

	jobs, err := s.ingestQueue.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LIST_ERROR", err.Error(), 0)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":  jobs,
		"count": len(jobs),
	})
}

// handleIngestStatus returns the status of a single ingest job.
// GET /ingest/jobs/{id}
func (s *Server) handleIngestStatus(w http.ResponseWriter, r *http.Request) {
	if s.ingestQueue == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_QUEUE", "ingest queue not configured", 0)
		return
	}

	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "job id required", 0)
		return
	}

	job, err := s.ingestQueue.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found", 0)
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// spoolBody writes the request body to a temp file and returns the path.
func spoolBody(body io.Reader) (string, error) {
	f, err := os.CreateTemp("", "loveliness-ingest-*.csv")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, body); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	f.Close()
	return f.Name(), nil
}
