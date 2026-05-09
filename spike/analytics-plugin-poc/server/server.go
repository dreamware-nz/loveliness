// Package server wires the spike's HTTP surface for the analytics plugin
// system. It deliberately mirrors the production POST /cypher path so a
// future merge can swap routes without touching plugin code.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/spike/analytics-plugin-poc/analytics"
)

// CypherRunner is the slimmed-down router contract the spike depends on.
// In production this would be *router.Router.Execute. The spike accepts
// any function with the same shape so tests can stub it.
type CypherRunner func(ctx context.Context, cypher string) (*router.Result, error)

type Server struct {
	run      CypherRunner
	registry *analytics.Registry
	timeout  time.Duration
}

func New(run CypherRunner, registry *analytics.Registry, timeout time.Duration) *Server {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Server{run: run, registry: registry, timeout: timeout}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /db/{name}/query", s.handleQuery)
	mux.HandleFunc("GET /analytics", s.handlePluginList)
	return mux
}

type queryRequest struct {
	Cypher    string              `json:"cypher"`
	Analytics []analytics.Request `json:"analytics,omitempty"`
}

type queryResponse struct {
	Columns         []string          `json:"columns"`
	Rows            []map[string]any  `json:"rows"`
	Analytics       map[string]any    `json:"analytics,omitempty"`
	AnalyticsErrors map[string]string `json:"analytics_errors,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error())
		return
	}
	var req queryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_JSON", err.Error())
		return
	}
	if req.Cypher == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "missing cypher")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()

	result, err := s.run(ctx, req.Cypher)
	if err != nil {
		var qe *router.QueryError
		if errors.As(err, &qe) {
			writeError(w, http.StatusBadRequest, qe.Code, qe.Message)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	resp := queryResponse{Columns: result.Columns, Rows: result.Rows}
	if len(req.Analytics) > 0 {
		out, errs := s.registry.Run(ctx, result, req.Analytics)
		if len(out) > 0 {
			resp.Analytics = out
		}
		if len(errs) > 0 {
			resp.AnalyticsErrors = errs
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePluginList(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"plugins": s.registry.Names()})
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}
