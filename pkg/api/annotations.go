package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/johnjansen/loveliness/pkg/annotations"
)

// registerAnnotationRoutes attaches the /annotations endpoints to the
// protected mux. Reads work on any node; writes must hit the leader so
// they replicate through Raft.
func (s *Server) registerAnnotationRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /annotations", s.handleAnnotationsList)
	mux.HandleFunc("GET /annotations/{target...}", s.handleAnnotationGet)
	mux.HandleFunc("POST /annotations", s.handleAnnotationSet)
	mux.HandleFunc("DELETE /annotations/{target...}", s.handleAnnotationDelete)
}

func (s *Server) handleAnnotationsList(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	out := s.cluster.GetAnnotations().List(prefix)
	writeJSON(w, http.StatusOK, map[string]any{"annotations": out})
}

func (s *Server) handleAnnotationGet(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	target := r.PathValue("target")
	if target == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "missing target", 0)
		return
	}
	a, ok := s.cluster.GetAnnotations().Get(target)
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "no annotation for target "+target, 0)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) handleAnnotationSet(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	if !s.cluster.IsLeader() {
		writeError(w, http.StatusBadRequest, "NOT_LEADER",
			fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "cannot read body: "+err.Error(), 0)
		return
	}
	var a annotations.Annotation
	if err := json.Unmarshal(body, &a); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body: "+err.Error(), 0)
		return
	}
	if strings.TrimSpace(a.Target) == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "target is required", 0)
		return
	}
	if err := s.cluster.SetAnnotation(a); err != nil {
		writeError(w, http.StatusBadRequest, "ANNOTATION_ERROR", err.Error(), 0)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "target": a.Target})
}

func (s *Server) handleAnnotationDelete(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, http.StatusServiceUnavailable, "NO_CLUSTER", "node is not part of a cluster", 0)
		return
	}
	if !s.cluster.IsLeader() {
		writeError(w, http.StatusBadRequest, "NOT_LEADER",
			fmt.Sprintf("not the leader; leader is at %s", s.cluster.LeaderAddr()), 0)
		return
	}
	target := r.PathValue("target")
	if target == "" {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "missing target", 0)
		return
	}
	if err := s.cluster.DeleteAnnotation(target); err != nil {
		writeError(w, http.StatusInternalServerError, "ANNOTATION_ERROR", err.Error(), 0)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "target": target})
}
