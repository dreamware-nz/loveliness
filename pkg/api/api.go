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

	"github.com/johnjansen/loveliness/pkg/auth"
	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/ingest"
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

	// ingestQueue is the optional log-backed ingest queue for async bulk loading.
	ingestQueue *ingest.Queue

	// auth is the optional token authenticator.
	auth *auth.TokenAuth

	// joinTokens manages single-use, time-limited cluster join tokens.
	joinTokens *cluster.TokenStore

	// discoveryInfo holds this node's join info for the public /discovery endpoint.
	discoveryInfo *cluster.JoinInfo
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
		joinTokens: cluster.NewTokenStore(),
	}
}

type contextKey string

const requestIDKey contextKey = "request_id"

// SetAuth sets the token authenticator for protected endpoints.
func (s *Server) SetAuth(a *auth.TokenAuth) {
	s.auth = a
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	// Protected routes — require auth when enabled.
	protected := http.NewServeMux()
	protected.HandleFunc("POST /cypher", s.handleCypher)
	protected.HandleFunc("POST /bulk/nodes", s.handleBulkNodes)
	protected.HandleFunc("POST /bulk/edges", s.handleBulkEdges)
	protected.HandleFunc("POST /bulk/nodes/stream", s.handleBulkNodesStream)
	protected.HandleFunc("POST /bulk/edges/stream", s.handleBulkEdgesStream)
	protected.HandleFunc("GET /cluster", s.handleCluster)
	protected.HandleFunc("POST /join", s.handleJoin)
	protected.HandleFunc("POST /join-token", s.handleJoinToken)
	s.registerDRRoutes(protected)
	s.registerIngestRoutes(protected)

	// Top-level mux: health and discovery are public, everything else goes through auth.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /discovery", s.handleDiscovery)
	if s.auth != nil && s.auth.Enabled() {
		mux.Handle("/", s.auth.Middleware(protected))
	} else {
		mux.Handle("/", protected)
	}
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

	// Replicate schema DDL through Raft so it survives restarts.
	if s.cluster != nil {
		s.replicateSchemaDDL(cypher)
	}

	writeJSON(w, http.StatusOK, result)
}

// replicateSchemaDDL detects CREATE NODE TABLE / DROP TABLE and replicates
// the schema change through Raft. Runs asynchronously — local registry was
// already updated by the router parser, so routing works immediately.
func (s *Server) replicateSchemaDDL(cypher string) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	switch {
	case strings.HasPrefix(upper, "CREATE NODE TABLE"):
		name, key, err := schema.ParseCreateNodeTable(cypher)
		if err == nil {
			if err := s.cluster.RegisterSchema(name, key); err != nil {
				slog.Warn("schema replication failed", "table", name, "err", err)
			} else {
				slog.Debug("schema replicated", "table", name, "shard_key", key)
			}
		}
	case strings.HasPrefix(upper, "DROP TABLE"):
		name, err := schema.ParseDropTable(cypher)
		if err == nil {
			if err := s.cluster.RemoveSchema(name); err != nil {
				slog.Warn("schema removal replication failed", "table", name, "err", err)
			}
		}
	}
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
	schemaTables := s.cluster.GetSchema()
	writeJSON(w, http.StatusOK, map[string]any{
		"leader":      s.cluster.LeaderAddr(),
		"node_id":     s.cluster.NodeID(),
		"is_leader":   s.cluster.IsLeader(),
		"shard_map":   sm.Assignments,
		"nodes":       sm.Nodes,
		"schema":      schemaTables,
	})
}

// joinRequest is the JSON body for POST /join.
type joinRequest struct {
	NodeID    string `json:"node_id"`
	RaftAddr  string `json:"raft_addr"`
	GRPCAddr  string `json:"grpc_addr"`
	HTTPAddr  string `json:"http_addr"`
	BoltAddr  string `json:"bolt_addr"`
	JoinToken string `json:"join_token"`
}

// SetDiscoveryInfo sets the node's join info for the public /discovery endpoint.
func (s *Server) SetDiscoveryInfo(info cluster.JoinInfo) {
	s.discoveryInfo = &info
}

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if s.discoveryInfo == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_DISCOVERY", "discovery info not configured", 0)
		return
	}
	writeJSON(w, http.StatusOK, s.discoveryInfo)
}

func (s *Server) handleJoinToken(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	if !s.cluster.IsLeader() {
		writeError(w, http.StatusBadRequest, "NOT_LEADER",
			fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
		return
	}

	// Default TTL: 10 minutes.
	ttl := 10 * time.Minute
	tok, err := s.joinTokens.Generate(ttl)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "TOKEN_ERROR", err.Error(), 0)
		return
	}

	slog.Info("join token generated",
		"expires_at", tok.ExpiresAt.Format(time.RFC3339),
		"source_ip", r.RemoteAddr,
	)

	writeJSON(w, http.StatusOK, tok)
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

	// Validate join token (required when auth is enabled).
	if s.auth != nil && s.auth.Enabled() {
		if req.JoinToken == "" {
			slog.Warn("join rejected: missing join token",
				"node_id", req.NodeID,
				"source_ip", r.RemoteAddr,
			)
			writeError(w, http.StatusForbidden, "FORBIDDEN", "join_token is required", 0)
			return
		}
		if !s.joinTokens.Validate(req.JoinToken) {
			slog.Warn("join rejected: invalid or expired join token",
				"node_id", req.NodeID,
				"source_ip", r.RemoteAddr,
			)
			writeError(w, http.StatusForbidden, "FORBIDDEN", "invalid or expired join token", 0)
			return
		}
	}

	if err := s.cluster.Join(req.NodeID, req.RaftAddr); err != nil {
		slog.Error("join failed",
			"node_id", req.NodeID,
			"source_ip", r.RemoteAddr,
			"err", err,
		)
		writeError(w, http.StatusInternalServerError, "JOIN_ERROR", err.Error(), 0)
		return
	}

	if err := s.cluster.RegisterNode(cluster.NodeInfo{
		ID:       req.NodeID,
		RaftAddr: req.RaftAddr,
		GRPCAddr: req.GRPCAddr,
		HTTPAddr: req.HTTPAddr,
		BoltAddr: req.BoltAddr,
		Alive:    true,
	}); err != nil {
		slog.Error("register node failed", "node_id", req.NodeID, "err", err)
	}

	slog.Info("node joined cluster",
		"node_id", req.NodeID,
		"raft_addr", req.RaftAddr,
		"source_ip", r.RemoteAddr,
	)

	writeJSON(w, http.StatusOK, map[string]string{"status": "joined", "node_id": req.NodeID})
}
