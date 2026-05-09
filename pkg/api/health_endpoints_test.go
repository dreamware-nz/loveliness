package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestHealthLive_AlwaysOK(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/health/live", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["status"] != "alive" {
		t.Errorf("expected status=alive, got %q", resp["status"])
	}
}

func TestHealthLive_StaysOKWhenShardUnhealthy(t *testing.T) {
	// Liveness must not flip when shards degrade — that would cause
	// restart loops on a problem the pod itself can't fix.
	shards := []*shard.Shard{
		shard.NewShard(0, &panicStore{}, 4),
	}
	shards[0].Query("MATCH (n) RETURN n")
	r := router.NewRouter(shards, 5*time.Second)
	srv := NewServer(r, nil, shards, nil, 5*time.Second)

	req := httptest.NewRequest("GET", "/health/live", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("liveness must stay OK when shards are degraded, got %d", w.Code)
	}
}

func TestHealthReady_OKWhenAllHealthy(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/health/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ready" {
		t.Errorf("expected status=ready, got %q", resp["status"])
	}
}

func TestHealthReady_503WhenShardUnhealthy(t *testing.T) {
	shards := []*shard.Shard{
		shard.NewShard(0, shard.NewMemoryStore(), 4),
		shard.NewShard(1, &panicStore{}, 4),
	}
	shards[1].Query("MATCH (n) RETURN n") // trip the panicStore
	r := router.NewRouter(shards, 5*time.Second)
	srv := NewServer(r, nil, shards, nil, 5*time.Second)

	req := httptest.NewRequest("GET", "/health/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "not_ready" {
		t.Errorf("expected status=not_ready, got %v", resp["status"])
	}
	reasons, ok := resp["reasons"].([]any)
	if !ok || len(reasons) == 0 {
		t.Errorf("expected non-empty reasons array, got %v", resp["reasons"])
	}
}

func TestHealth_StructuredFieldsPresent(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := resp["uptime_seconds"]; !ok {
		t.Errorf("expected uptime_seconds in /health response: %v", resp)
	}
	if resp["mode"] != "standalone" {
		t.Errorf("expected mode=standalone (no cluster), got %v", resp["mode"])
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", resp["status"])
	}
}

func TestHealth_UptimeIsNonNegative(t *testing.T) {
	srv := setupTestServer()
	// Sleep a tick so uptime is meaningfully > 0; we mostly want to
	// confirm it's not negative or absurd.
	time.Sleep(2 * time.Millisecond)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	uptime, ok := resp["uptime_seconds"].(float64)
	if !ok {
		t.Fatalf("uptime_seconds missing or wrong type: %v", resp["uptime_seconds"])
	}
	if uptime < 0 {
		t.Errorf("uptime_seconds must be non-negative, got %v", uptime)
	}
}
