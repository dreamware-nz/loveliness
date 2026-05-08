// Package annotations stores natural-language annotations attached to
// schema elements: descriptions, query examples, and freeform tags.
//
// Annotations are a first-class part of the cluster's state — they're
// replicated through the Raft FSM alongside the shard map and database
// catalog, snapshotted, and restored on restart. They exist so an LLM
// (or human) can ask "what does this database hold?" or "how do I
// fetch X?" and get useful answers without the schema alone.
//
// Storage model: latest-wins, no version history. A SET on an existing
// target replaces the entire annotation body.
package annotations

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryExample is a parameterized query template attached to a target.
// Query holds the template (typically Cypher with $param placeholders),
// Params documents the expected parameters, Explanation describes what
// the query does in human terms.
type QueryExample struct {
	Title       string         `json:"title,omitempty"`
	Query       string         `json:"query"`
	Params      map[string]any `json:"params,omitempty"`
	Explanation string         `json:"explanation,omitempty"`
}

// Annotation is the body of metadata attached to a single target.
// Tags are freeform strings (e.g. "pii", "wide", "deprecated"). Extra
// holds arbitrary string-keyed values for callers that need to stash
// structured side data without cluttering top-level fields.
type Annotation struct {
	Target      string            `json:"target"`
	Description string            `json:"description,omitempty"`
	Examples    []QueryExample    `json:"examples,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Extra       map[string]string `json:"extra,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// targetSegment matches a single path segment after a "kind:" prefix.
// We constrain to identifier-friendly characters so target strings can
// be passed through URL paths and JSON without escaping concerns.
var targetSegment = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// ValidateTarget accepts these forms:
//
//	cluster
//	db:<name>
//	db:<name>/table:<name>
//	db:<name>/table:<name>/property:<name>
//	db:<name>/edge:<name>
//	db:<name>/edge:<name>/property:<name>
//	db:<name>/saved_query:<id>
//
// Returns the canonical target string (currently identical to the
// input) on success, or an error describing the malformed segment.
func ValidateTarget(target string) (string, error) {
	if target == "" {
		return "", fmt.Errorf("target is empty")
	}
	if target == "cluster" {
		return target, nil
	}
	parts := strings.Split(target, "/")
	for i, p := range parts {
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			return "", fmt.Errorf("segment %d %q: expected kind:name", i, p)
		}
		kind, name := kv[0], kv[1]
		switch kind {
		case "db", "table", "edge", "property", "saved_query":
		default:
			return "", fmt.Errorf("segment %d: unknown kind %q (allowed: db, table, edge, property, saved_query)", i, kind)
		}
		if !targetSegment.MatchString(name) {
			return "", fmt.Errorf("segment %d: invalid name %q", i, name)
		}
	}
	// Light structural rule: first segment must be db:* if not "cluster",
	// property/edge/table/saved_query must come after a db.
	if parts[0] == "cluster" {
		// not reachable — already returned above
	}
	first := strings.SplitN(parts[0], ":", 2)[0]
	if first != "db" {
		return "", fmt.Errorf("first segment must be cluster or db:<name>, got %q", first)
	}
	return target, nil
}

// Registry is the in-memory store of annotations, replicated through
// the Raft FSM. The Set/Delete methods do not themselves replicate;
// callers (the FSM Apply path) call them once per Raft log entry.
type Registry struct {
	mu    sync.RWMutex
	store map[string]Annotation
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{store: make(map[string]Annotation)}
}

// Set replaces any existing annotation at target. UpdatedAt is set to
// now if the caller left it zero; this lets the FSM Apply path stamp
// a deterministic time when needed.
func (r *Registry) Set(a Annotation) error {
	target, err := ValidateTarget(a.Target)
	if err != nil {
		return err
	}
	a.Target = target
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = time.Now().UTC()
	}
	// Validate examples have non-empty query strings — examples with
	// no query are useless to an LLM trying to learn from them.
	for i, ex := range a.Examples {
		if strings.TrimSpace(ex.Query) == "" {
			return fmt.Errorf("examples[%d]: query is empty", i)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[target] = a
	return nil
}

// Get returns the annotation for target, or (zero, false) if absent.
func (r *Registry) Get(target string) (Annotation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.store[target]
	return a, ok
}

// Delete drops the annotation at target. Returns true if anything was
// removed.
func (r *Registry) Delete(target string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.store[target]; !ok {
		return false
	}
	delete(r.store, target)
	return true
}

// List returns all annotations whose target starts with prefix, sorted
// by target. An empty prefix returns everything.
func (r *Registry) List(prefix string) []Annotation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Annotation, 0, len(r.store))
	for k, v := range r.store {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out
}

// Snapshot returns a deep copy suitable for Raft snapshotting.
func (r *Registry) Snapshot() map[string]Annotation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Annotation, len(r.store))
	for k, v := range r.store {
		out[k] = cloneAnnotation(v)
	}
	return out
}

// Restore replaces the registry contents from a snapshot.
func (r *Registry) Restore(snap map[string]Annotation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if snap == nil {
		r.store = make(map[string]Annotation)
		return
	}
	r.store = make(map[string]Annotation, len(snap))
	for k, v := range snap {
		r.store[k] = cloneAnnotation(v)
	}
}

func cloneAnnotation(a Annotation) Annotation {
	cp := a
	if len(a.Examples) > 0 {
		cp.Examples = make([]QueryExample, len(a.Examples))
		for i, ex := range a.Examples {
			cp.Examples[i] = ex
			if len(ex.Params) > 0 {
				cp.Examples[i].Params = make(map[string]any, len(ex.Params))
				for k, v := range ex.Params {
					cp.Examples[i].Params[k] = v
				}
			}
		}
	}
	if len(a.Tags) > 0 {
		cp.Tags = make([]string, len(a.Tags))
		copy(cp.Tags, a.Tags)
	}
	if len(a.Extra) > 0 {
		cp.Extra = make(map[string]string, len(a.Extra))
		for k, v := range a.Extra {
			cp.Extra[k] = v
		}
	}
	return cp
}
