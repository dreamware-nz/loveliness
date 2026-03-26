package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/johnjansen/loveliness/pkg/cluster"
	"github.com/johnjansen/loveliness/pkg/router"
)

// Server is the HTTP API server for a Loveliness node.
//
//	Endpoints:
//	  POST /query     — execute a Cypher query
//	  GET  /health    — node health check
//	  GET  /cluster   — cluster status (shard map, leader, nodes)
//	  POST /join      — join a new node to the cluster (leader only)
type Server struct {
	router  *router.Router
	cluster *cluster.Cluster
	timeout time.Duration
}

// NewServer creates a new API server.
func NewServer(r *router.Router, c *cluster.Cluster, timeout time.Duration) *Server {
	return &Server{
		router:  r,
		cluster: c,
		timeout: timeout,
	}
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /query", s.handleQuery)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /cluster", s.handleCluster)
	mux.HandleFunc("POST /join", s.handleJoin)
	return mux
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
			case "CYPHER_PARSE_ERROR":
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
	if s.cluster == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "mode": "standalone"})
		return
	}
	status := "follower"
	if s.cluster.IsLeader() {
		status = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"role":    status,
		"node_id": s.cluster.NodeID(),
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	sm := s.cluster.GetShardMap()
	writeJSON(w, http.StatusOK, map[string]any{
		"leader":      s.cluster.LeaderAddr(),
		"node_id":     s.cluster.NodeID(),
		"is_leader":   s.cluster.IsLeader(),
		"shard_map":   sm.Assignments,
		"nodes":       sm.Nodes,
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
		log.Printf("join error: %v", err)
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
		log.Printf("register node error: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "joined", "node_id": req.NodeID})
}
