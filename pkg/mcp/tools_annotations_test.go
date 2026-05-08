package mcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestClientAnnotationsRoundTrip drives the four annotation client
// methods against a fakeBackend that mimics the /annotations endpoints.
func TestClientAnnotationsRoundTrip(t *testing.T) {
	var mu sync.Mutex
	store := make(map[string]Annotation)

	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/annotations": func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				prefix := r.URL.Query().Get("prefix")
				mu.Lock()
				out := make([]Annotation, 0, len(store))
				for k, v := range store {
					if prefix == "" || strings.HasPrefix(k, prefix) {
						out = append(out, v)
					}
				}
				mu.Unlock()
				writeJSON(w, map[string]any{"annotations": out})
			case http.MethodPost:
				var a Annotation
				body, _ := io.ReadAll(r.Body)
				if err := json.Unmarshal(body, &a); err != nil {
					http.Error(w, "bad json", 400)
					return
				}
				mu.Lock()
				a.UpdatedAt = "2026-05-09T00:00:00Z"
				store[a.Target] = a
				mu.Unlock()
				writeJSON(w, map[string]any{"status": "ok", "target": a.Target})
			default:
				http.Error(w, "bad method", 405)
			}
		},
		"/annotations/": func(w http.ResponseWriter, r *http.Request) {
			target := strings.TrimPrefix(r.URL.Path, "/annotations/")
			switch r.Method {
			case http.MethodGet:
				mu.Lock()
				a, ok := store[target]
				mu.Unlock()
				if !ok {
					http.Error(w, `{"error":{"code":"NOT_FOUND","message":"no annotation"}}`, 404)
					return
				}
				writeJSON(w, a)
			case http.MethodDelete:
				mu.Lock()
				delete(store, target)
				mu.Unlock()
				writeJSON(w, map[string]string{"status": "deleted", "target": target})
			default:
				http.Error(w, "bad method", 405)
			}
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	ctx := context.Background()

	// Empty get — should return (nil, nil).
	got, err := c.GetAnnotation(ctx, "db:default/table:Person")
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing target, got %+v", got)
	}

	// Set.
	want := Annotation{
		Target:      "db:default/table:Person",
		Description: "people",
		Examples:    []AnnotationExample{{Title: "by name", Query: "MATCH (p:Person {name: $n}) RETURN p"}},
		Tags:        []string{"core"},
	}
	if err := c.SetAnnotation(ctx, want); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Get.
	got, err = c.GetAnnotation(ctx, want.Target)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil || got.Description != "people" || len(got.Examples) != 1 {
		t.Fatalf("get returned %+v", got)
	}

	// List with prefix.
	out, err := c.ListAnnotations(ctx, "db:default/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 1 || out[0].Target != want.Target {
		t.Fatalf("list returned %+v", out)
	}

	// Delete.
	if err := c.DeleteAnnotation(ctx, want.Target); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err = c.GetAnnotation(ctx, want.Target)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}

func TestQueryEscape(t *testing.T) {
	cases := map[string]string{
		"db:default/":         "db:default/",
		"db:foo bar":          "db:foo%20bar",
		"db:weird?x=y":        "db:weird%3Fx%3Dy",
		"cluster":             "cluster",
		"db:my-db_2/table:_a": "db:my-db_2/table:_a",
	}
	for in, want := range cases {
		if got := queryEscape(in); got != want {
			t.Errorf("queryEscape(%q) = %q, want %q", in, got, want)
		}
	}
}
