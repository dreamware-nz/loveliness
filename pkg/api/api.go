package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/schema"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// Server is the HTTP API server for a Loveliness node.
type Server struct {
	router  *router.Router
	cluster *cluster.Cluster
	shards  []*shard.Shard
	schema  *schema.Registry
	timeout time.Duration

	// refTracker tracks known node keys per shard for fast duplicate
	// detection during bulk edge loading. Avoids expensive full-table
	// scans on every batch.
	refTracker map[int]map[string]bool
	refTrackMu sync.RWMutex

	// dr holds optional disaster recovery extensions (WAL, backup, replica state).
	dr *DRExtension
}

// NewServer creates a new API server.
func NewServer(r *router.Router, c *cluster.Cluster, shards []*shard.Shard, reg *schema.Registry, timeout time.Duration) *Server {
	return &Server{
		router:     r,
		cluster:    c,
		shards:     shards,
		schema:     reg,
		timeout:    timeout,
		refTracker: make(map[int]map[string]bool),
	}
}

type contextKey string

const requestIDKey contextKey = "request_id"

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", s.handleQuery)
	mux.HandleFunc("POST /cypher", s.handleCypher)
	mux.HandleFunc("POST /bulk/nodes", s.handleBulkNodes)
	mux.HandleFunc("POST /bulk/edges", s.handleBulkEdges)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /cluster", s.handleCluster)
	mux.HandleFunc("POST /join", s.handleJoin)
	s.registerDRRoutes(mux)
	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := generateRequestID()
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		w.Header().Set("X-Request-ID", reqID)

		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r.WithContext(ctx))

		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", reqID,
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// queryRequest is the JSON body for POST /query.
type queryRequest struct {
	Cypher string `json:"cypher"`
}

// errorResponse is a structured error returned to the client.
type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		ShardID int    `json:"shard_id,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string, shardID int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := errorResponse{}
	resp.Error.Code = code
	resp.Error.Message = message
	resp.Error.ShardID = shardID
	json.NewEncoder(w).Encode(resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body", 0)
		return
	}
	if req.Cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cypher field is required", 0)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	result, err := s.router.Execute(ctx, req.Cypher)
	if err != nil {
		if qe, ok := err.(*router.QueryError); ok {
			status := http.StatusInternalServerError
			switch qe.Code {
			case "CYPHER_PARSE_ERROR", "MISSING_SHARD_KEY":
				status = http.StatusBadRequest
			case "SHARD_UNAVAILABLE":
				status = http.StatusServiceUnavailable
			case "QUERY_TIMEOUT":
				status = http.StatusGatewayTimeout
			}
			writeError(w, status, qe.Code, qe.Message, qe.ShardID)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), 0)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleCypher(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB max
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error(), 0)
		return
	}
	cypher := strings.TrimSpace(string(body))
	if cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "empty query body", 0)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	result, err := s.router.Execute(ctx, cypher)
	if err != nil {
		if qe, ok := err.(*router.QueryError); ok {
			status := http.StatusInternalServerError
			switch qe.Code {
			case "CYPHER_PARSE_ERROR", "MISSING_SHARD_KEY":
				status = http.StatusBadRequest
			case "SHARD_UNAVAILABLE":
				status = http.StatusServiceUnavailable
			case "QUERY_TIMEOUT":
				status = http.StatusGatewayTimeout
			}
			writeError(w, status, qe.Code, qe.Message, qe.ShardID)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), 0)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"status": "ok",
	}

	if s.cluster != nil {
		role := "follower"
		if s.cluster.IsLeader() {
			role = "leader"
		}
		resp["role"] = role
		resp["node_id"] = s.cluster.NodeID()
	} else {
		resp["mode"] = "standalone"
	}

	if len(s.shards) > 0 {
		shardStatus := make(map[string]string, len(s.shards))
		allHealthy := true
		for _, sh := range s.shards {
			if sh.IsHealthy() {
				shardStatus[strconv.Itoa(sh.ID)] = "healthy"
			} else {
				shardStatus[strconv.Itoa(sh.ID)] = "unhealthy"
				allHealthy = false
			}
		}
		resp["shards"] = shardStatus
		if !allHealthy {
			resp["status"] = "degraded"
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	sm := s.cluster.GetShardMap()
	writeJSON(w, http.StatusOK, map[string]any{
		"leader":    s.cluster.LeaderAddr(),
		"node_id":   s.cluster.NodeID(),
		"is_leader": s.cluster.IsLeader(),
		"shard_map": sm.Assignments,
		"nodes":     sm.Nodes,
	})
}

// joinRequest is the JSON body for POST /join.
type joinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
	HTTPAddr string `json:"http_addr"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	if !s.cluster.IsLeader() {
		writeError(w, http.StatusBadRequest, "NOT_LEADER",
			fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
		return
	}

	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body", 0)
		return
	}
	if req.NodeID == "" || req.RaftAddr == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "node_id and raft_addr are required", 0)
		return
	}

	if err := s.cluster.Join(req.NodeID, req.RaftAddr); err != nil {
		slog.Error("join failed", "node_id", req.NodeID, "err", err)
		writeError(w, http.StatusInternalServerError, "JOIN_ERROR", err.Error(), 0)
		return
	}

	if err := s.cluster.RegisterNode(cluster.NodeInfo{
		ID:       req.NodeID,
		RaftAddr: req.RaftAddr,
		GRPCAddr: req.GRPCAddr,
		HTTPAddr: req.HTTPAddr,
		Alive:    true,
	}); err != nil {
		slog.Error("register node failed", "node_id", req.NodeID, "err", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "joined", "node_id": req.NodeID})
}
