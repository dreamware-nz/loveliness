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
	"sort"
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
	router   *router.Router
	dbRouter *router.DatabaseRouter
	cluster  *cluster.Cluster
	shards   []*shard.Shard
	schema   *schema.Registry
	timeout  time.Duration

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

	// startTime is captured at NewServer time so /health can report uptime
	// without piping a clock through every call site.
	startTime time.Time

	// queryCounters tracks /cypher request counts by (query_type, status)
	// for the loveliness_query_total metric.
	queryCounters *queryCounters

	// queryHistogram tracks /cypher latencies by (query_type, status)
	// for the loveliness_query_duration_seconds metric.
	queryHistogram *queryHistogram

	// bulkLoadCounters tracks rows ingested via /bulk/* endpoints,
	// labeled by table, for the loveliness_bulk_load_rows_total metric.
	bulkLoadCounters *bulkLoadCounters
}

// NewServer creates a new API server.
func NewServer(r *router.Router, c *cluster.Cluster, shards []*shard.Shard, reg *schema.Registry, timeout time.Duration) *Server {
	return &Server{
		router:           r,
		cluster:          c,
		shards:           shards,
		schema:           reg,
		timeout:          timeout,
		refTracker:       make(map[int]map[string]bool),
		joinTokens:       cluster.NewTokenStore(),
		startTime:        time.Now(),
		queryCounters:    newQueryCounters(),
		queryHistogram:   newQueryHistogram(),
		bulkLoadCounters: newBulkLoadCounters(),
	}
}

type contextKey string

const requestIDKey contextKey = "request_id"

// SetAuth sets the token authenticator for protected endpoints.
func (s *Server) SetAuth(a *auth.TokenAuth) {
	s.auth = a
}

// SetDatabaseRouter sets the multi-database router for scoped endpoints.
func (s *Server) SetDatabaseRouter(dr *router.DatabaseRouter) {
	s.dbRouter = dr
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	// Protected routes — require auth when enabled.
	protected := http.NewServeMux()
	protected.HandleFunc("POST /cypher", s.handleCypherLegacy)
	protected.HandleFunc("POST /bulk/nodes", s.handleBulkNodes)
	protected.HandleFunc("POST /bulk/edges", s.handleBulkEdges)
	protected.HandleFunc("POST /bulk/nodes/stream", s.handleBulkNodesStream)
	protected.HandleFunc("POST /bulk/edges/stream", s.handleBulkEdgesStream)
	protected.HandleFunc("GET /cluster", s.handleCluster)
	protected.HandleFunc("POST /join", s.handleJoin)
	protected.HandleFunc("POST /join-token", s.handleJoinToken)
	s.registerDRRoutes(protected)
	s.registerIngestRoutes(protected)
	s.registerAnnotationRoutes(protected)

	// Multi-database scoped routes.
	protected.HandleFunc("POST /db/{name}/cypher", s.handleCypherScoped)
	protected.HandleFunc("POST /db/{name}/bulk/nodes", s.handleBulkNodesScoped)
	protected.HandleFunc("POST /db/{name}/bulk/edges", s.handleBulkEdgesScoped)

	// Admin endpoint (not db-scoped) for CREATE/STOP/START/DROP DATABASE, SHOW DATABASES.
	protected.HandleFunc("POST /admin/cypher", s.handleAdminCypher)

	// Top-level mux: health and discovery are public, everything else goes through auth.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /health/live", s.handleHealthLive)
	mux.HandleFunc("GET /health/ready", s.handleHealthReady)
	mux.HandleFunc("GET /discovery", s.handleDiscovery)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
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

// handleCypherLegacy routes queries to the default shard set for backward compatibility.
// Clients should prefer /db/{name}/cypher for multi-database usage.
func (s *Server) handleCypherLegacy(w http.ResponseWriter, r *http.Request) {
	s.handleCypher(w, r)
}

func (s *Server) handleCypher(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	record := func(qtype string, code int) {
		bucket := statusBucket(code)
		s.queryCounters.Inc(qtype, bucket)
		s.queryHistogram.Observe(qtype, bucket, time.Since(start).Seconds())
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB max
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error(), 0)
		record("unknown", http.StatusBadRequest)
		return
	}
	cypher := strings.TrimSpace(string(body))
	if cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "empty query body", 0)
		record("unknown", http.StatusBadRequest)
		return
	}

	qtype := classifyQueryType(cypher)

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
			record(qtype, status)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), 0)
		record(qtype, http.StatusInternalServerError)
		return
	}

	// Replicate schema DDL through Raft so it survives restarts.
	if s.cluster != nil {
		s.replicateSchemaDDL(cypher)
	}

	writeNegotiated(w, r, result)
	record(qtype, http.StatusOK)
}

// writeNegotiated picks the response format based on the request's
// Accept header. JSON stays the canonical default; clients can opt
// into Arrow IPC stream or file via the documented content types.
// When the client explicitly asks for an unsupported type we honor
// the contract and return 406 instead of silently downgrading.
func writeNegotiated(w http.ResponseWriter, r *http.Request, result *router.Result) {
	chosen, unacceptable := negotiateResponse(r.Header.Get("Accept"))
	if unacceptable {
		writeError(w, http.StatusNotAcceptable, "NOT_ACCEPTABLE",
			"server can produce application/json, application/vnd.apache.arrow.stream, or application/vnd.apache.arrow.file", 0)
		return
	}
	switch chosen {
	case contentTypeArrowFile:
		buf, err := encodeResultAsArrow(result)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "ARROW_ENCODE_ERROR", err.Error(), 0)
			return
		}
		w.Header().Set("Content-Type", contentTypeArrowFile)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
	case contentTypeArrowStream:
		buf, err := encodeResultAsArrowStream(result)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "ARROW_ENCODE_ERROR", err.Error(), 0)
			return
		}
		w.Header().Set("Content-Type", contentTypeArrowStream)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
	default:
		writeJSON(w, http.StatusOK, result)
	}
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
			if err := s.cluster.RegisterTable(name, key); err != nil {
				slog.Warn("schema replication failed", "table", name, "err", err)
			} else {
				slog.Debug("schema replicated", "table", name, "shard_key", key)
			}
		}
	case strings.HasPrefix(upper, "DROP TABLE"):
		name, err := schema.ParseDropTable(cypher)
		if err == nil {
			if err := s.cluster.RemoveTable(name); err != nil {
				slog.Warn("schema removal replication failed", "table", name, "err", err)
			}
		}
	}
}

// healthSnapshot is the structured form of /health used by both the JSON
// handler and the /health/ready gate. Splitting it out keeps the readiness
// gate honest: ready returns 503 iff this snapshot says the node is
// degraded or not-yet-ready, rather than re-deriving the same checks.
type healthSnapshot struct {
	Status         string        `json:"status"` // "ok" | "degraded"
	NodeID         string        `json:"node_id,omitempty"`
	Mode           string        `json:"mode,omitempty"` // "standalone" when no cluster
	Role           string        `json:"role,omitempty"`
	LeaderID       string        `json:"leader_id,omitempty"`
	UptimeSeconds  float64       `json:"uptime_seconds"`
	PlacementEpoch uint64        `json:"placement_epoch,omitempty"`
	Shards         []shardStatus `json:"shards,omitempty"`
}

// shardStatus is the per-shard entry on /health. Sorted by ID so the
// array is deterministic across scrapes — important for diffing health
// JSON in incident reviews.
type shardStatus struct {
	ID                  int     `json:"id"`
	Status              string  `json:"status"`           // "healthy" | "unhealthy"
	Reason              string  `json:"reason,omitempty"` // populated when status != healthy
	WALSequence         uint64  `json:"wal_sequence,omitempty"`
	ReplicationLagSecs  float64 `json:"replication_lag_seconds,omitempty"`
	ReplicationLagBytes int64   `json:"replication_lag_bytes,omitempty"`
}

// snapshotHealth gathers the current node's health into a structured form.
// Both /health and /health/ready consume it; keeping a single function
// means they can never disagree on what "ready" means.
func (s *Server) snapshotHealth() healthSnapshot {
	snap := healthSnapshot{
		Status:        "ok",
		UptimeSeconds: time.Since(s.startTime).Seconds(),
	}

	if s.cluster != nil {
		snap.Role = s.cluster.Role()
		snap.NodeID = s.cluster.NodeID()
		snap.LeaderID = s.cluster.LeaderID()
		snap.PlacementEpoch = maxShardEpoch(s.cluster.GetShardMap())
	} else {
		snap.Mode = "standalone"
	}

	if len(s.shards) > 0 {
		snap.Shards = make([]shardStatus, 0, len(s.shards))
		allHealthy := true
		for _, sh := range s.shards {
			st := shardStatus{ID: sh.ID, Status: "healthy"}
			if !sh.IsHealthy() {
				st.Status = "unhealthy"
				st.Reason = "shard_marked_unhealthy"
				allHealthy = false
			}
			s.fillShardReplicationLag(&st)
			snap.Shards = append(snap.Shards, st)
		}
		sort.Slice(snap.Shards, func(i, j int) bool {
			return snap.Shards[i].ID < snap.Shards[j].ID
		})
		if !allHealthy {
			snap.Status = "degraded"
		}
	}

	return snap
}

// fillShardReplicationLag populates the WAL- and replica-derived fields
// on a shardStatus entry. Quiet no-op when the node isn't running with
// DR/replication enabled — keeps the JSON shape stable across single-node
// and clustered deployments.
func (s *Server) fillShardReplicationLag(st *shardStatus) {
	if s.dr == nil || s.dr.WAL == nil {
		return
	}
	st.WALSequence = s.dr.WAL.ShardSequence(st.ID)

	if s.dr.ReplicaState == nil || s.cluster == nil {
		return
	}
	sm := s.cluster.GetShardMap()
	assignment, ok := sm.Assignments[st.ID]
	if !ok {
		return
	}
	headTS := s.dr.WAL.HeadTimestamp(st.ID)
	var maxLagSecs float64
	var maxLagBytes int64
	for _, replica := range assignment.Replicas {
		if replica == "" {
			continue
		}
		pos := s.dr.ReplicaState.GetPosition(st.ID, replica)
		applied := s.dr.ReplicaState.GetTimestamp(st.ID, replica)
		secs := computeLagSeconds(headTS, applied, pos)
		if secs > maxLagSecs {
			maxLagSecs = secs
		}
		if b := s.dr.WAL.LagBytes(st.ID, pos); b > maxLagBytes {
			maxLagBytes = b
		}
	}
	st.ReplicationLagSecs = maxLagSecs
	st.ReplicationLagBytes = maxLagBytes
}

// maxShardEpoch returns the highest per-shard placement epoch in the map,
// or 0 if the map is empty. Surfaced as a coarse cluster-wide "placement
// generation" indicator on /health — fine-grained per-shard epochs stay
// in /cluster.
func maxShardEpoch(sm cluster.ShardMap) uint64 {
	var max uint64
	for _, a := range sm.Assignments {
		if a.Epoch > max {
			max = a.Epoch
		}
	}
	return max
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.snapshotHealth())
}

// handleHealthLive is a Kubernetes-style liveness probe. It only confirms
// the process is up and the HTTP mux is serving — it deliberately does
// NOT check shard or cluster state, because a degraded cluster shouldn't
// trigger pod restarts on its own (that would amplify outages).
func (s *Server) handleHealthLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// handleHealthReady is a Kubernetes-style readiness probe. It returns 503
// when the node should be removed from load-balancer rotation: any local
// shard unhealthy, or (when clustered) no leader is yet known.
func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	snap := s.snapshotHealth()
	reasons := []string{}

	if snap.Status != "ok" {
		reasons = append(reasons, "shards_degraded")
	}
	if s.cluster != nil && snap.LeaderID == "" {
		reasons = append(reasons, "no_leader")
	}

	if len(reasons) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":  "not_ready",
			"reasons": reasons,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	sm := s.cluster.GetShardMap()
	schemaTables := s.cluster.GetSchema()
	writeJSON(w, http.StatusOK, map[string]any{
		"leader":    s.cluster.LeaderAddr(),
		"node_id":   s.cluster.NodeID(),
		"is_leader": s.cluster.IsLeader(),
		"shard_map": sm.Assignments,
		"nodes":     sm.Nodes,
		"schema":    schemaTables,
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

// handleCypherScoped routes a Cypher query to a specific database.
func (s *Server) handleCypherScoped(w http.ResponseWriter, r *http.Request) {
	dbName := r.PathValue("name")
	if dbName == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "database name required in path", 0)
		return
	}
	if s.dbRouter == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_MULTI_DB", "multi-database not enabled", 0)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error(), 0)
		return
	}
	cypher := strings.TrimSpace(string(body))
	if cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "empty query body", 0)
		return
	}

	// Admin commands should go to /admin/cypher, not /db/{name}/cypher.
	if router.IsAdminCommand(cypher) {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"admin commands must use POST /admin/cypher", 0)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	result, err := s.dbRouter.Execute(ctx, dbName, cypher)
	if err != nil {
		if qe, ok := err.(*router.QueryError); ok {
			status := http.StatusInternalServerError
			switch qe.Code {
			case "CYPHER_PARSE_ERROR", "MISSING_SHARD_KEY":
				status = http.StatusBadRequest
			case "DATABASE_ERROR":
				status = http.StatusNotFound
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

// handleBulkNodesScoped routes bulk node loading to a specific database.
func (s *Server) handleBulkNodesScoped(w http.ResponseWriter, r *http.Request) {
	dbName := r.PathValue("name")
	if s.dbRouter == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_MULTI_DB", "multi-database not enabled", 0)
		return
	}
	_, err := s.dbRouter.GetRouter(dbName)
	if err != nil {
		writeError(w, http.StatusNotFound, "DATABASE_ERROR", err.Error(), 0)
		return
	}
	// TODO: delegate to per-database bulk handler once shard references are scoped.
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"scoped bulk node loading not yet implemented", 0)
}

// handleBulkEdgesScoped routes bulk edge loading to a specific database.
func (s *Server) handleBulkEdgesScoped(w http.ResponseWriter, r *http.Request) {
	dbName := r.PathValue("name")
	if s.dbRouter == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_MULTI_DB", "multi-database not enabled", 0)
		return
	}
	_, err := s.dbRouter.GetRouter(dbName)
	if err != nil {
		writeError(w, http.StatusNotFound, "DATABASE_ERROR", err.Error(), 0)
		return
	}
	writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
		"scoped bulk edge loading not yet implemented", 0)
}

// handleAdminCypher handles database admin commands (CREATE/STOP/START/DROP DATABASE, SHOW DATABASES).
func (s *Server) handleAdminCypher(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error(), 0)
		return
	}
	cypher := strings.TrimSpace(string(body))
	if cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "empty query body", 0)
		return
	}

	cmd := router.ParseAdminCommand(cypher)
	if cmd == nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST",
			"only admin commands (CREATE/STOP/START/DROP DATABASE, SHOW DATABASES) are accepted on this endpoint", 0)
		return
	}

	switch cmd.Type {
	case router.AdminShowDatabases:
		cat := s.cluster.GetCatalog()
		dbs := cat.ListDatabases()
		rows := make([]map[string]any, len(dbs))
		for i, db := range dbs {
			rows[i] = map[string]any{
				"name":        db.Name,
				"state":       db.State.String(),
				"shard_count": db.ShardCount,
				"created_at":  db.CreatedAt,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"columns": []string{"name", "state", "shard_count", "created_at"},
			"rows":    rows,
		})

	case router.AdminCreateDatabase:
		if !s.cluster.IsLeader() {
			writeError(w, http.StatusBadRequest, "NOT_LEADER",
				fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
			return
		}
		if err := s.cluster.CreateDatabase(cmd.Name, cmd.ShardCount); err != nil {
			writeError(w, http.StatusBadRequest, "CREATE_DATABASE_ERROR", err.Error(), 0)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":      "created",
			"name":        cmd.Name,
			"shard_count": cmd.ShardCount,
		})

	case router.AdminStopDatabase:
		if !s.cluster.IsLeader() {
			writeError(w, http.StatusBadRequest, "NOT_LEADER",
				fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
			return
		}
		if err := s.cluster.StopDatabase(cmd.Name); err != nil {
			writeError(w, http.StatusBadRequest, "STOP_DATABASE_ERROR", err.Error(), 0)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "stopped", "name": cmd.Name})

	case router.AdminStartDatabase:
		if !s.cluster.IsLeader() {
			writeError(w, http.StatusBadRequest, "NOT_LEADER",
				fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
			return
		}
		if err := s.cluster.StartDatabase(cmd.Name); err != nil {
			writeError(w, http.StatusBadRequest, "START_DATABASE_ERROR", err.Error(), 0)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "started", "name": cmd.Name})

	case router.AdminDropDatabase:
		if !s.cluster.IsLeader() {
			writeError(w, http.StatusBadRequest, "NOT_LEADER",
				fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
			return
		}
		if err := s.cluster.DeleteDatabase(cmd.Name); err != nil {
			writeError(w, http.StatusBadRequest, "DROP_DATABASE_ERROR", err.Error(), 0)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "name": cmd.Name})
	}
}
