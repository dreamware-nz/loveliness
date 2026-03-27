package transport

import (
	"encoding/json"
	"log/slog"
	"net/http"

)

// Handler serves internal endpoints for node-to-node communication.
type Handler struct {
	shards ShardQuerier
}

// NewHandler creates an internal transport handler.
func NewHandler(shards ShardQuerier) *Handler {
	return &Handler{shards: shards}
}

// RegisterRoutes registers internal endpoints on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/query", h.handleInternalQuery)
}

func (h *Handler) handleInternalQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeInternalError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	s := h.shards.GetShard(req.ShardID)
	if s == nil {
		writeInternalError(w, http.StatusNotFound,
			"shard not hosted on this node")
		return
	}

	resp, err := s.Query(req.Cypher)
	if err != nil {
		slog.Error("internal query failed", "shard", req.ShardID, "err", err)
		writeInternalError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	qr := QueryResponse{
		Columns: resp.Columns,
		Rows:    resp.Rows,
	}
	qr.Stats.CompileTimeMs = resp.Stats.CompileTimeMs
	qr.Stats.ExecTimeMs = resp.Stats.ExecTimeMs
	json.NewEncoder(w).Encode(qr)
}

func writeInternalError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(QueryResponse{Error: msg})
}
