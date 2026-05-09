// Package analytics defines the plugin contract for opt-in,
// post-execution computations on a Cypher Result. Plugins are registered
// once at server boot and selected per-request via the JSON query envelope.
package analytics

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/johnjansen/loveliness/pkg/router"
)

// Plugin is a named, side-effect-free computation that runs against a
// completed *router.Result and produces an opaque value the client gets
// back under response.analytics[Name].
//
// Implementations MUST treat the result as read-only. Mutating Rows or
// Columns will corrupt the response that's about to be serialised.
type Plugin interface {
	Name() string
	Compute(ctx context.Context, result *router.Result, params map[string]any) (any, error)
}

// Request describes a single plugin invocation pulled from the query envelope.
type Request struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
}

// Registry holds the set of plugins known to a server. Concurrent-safe
// for reads; registration is boot-only — once Freeze() is called (the
// server does this in Handler()), further Register calls fail. This
// keeps the runtime contract simple: every request sees the same set
// of plugins, and there is no window where a request can race with
// registration. Out-of-tree dynamic-load is explicitly out of scope —
// see issue #71.
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]Plugin
	frozen  bool
}

func NewRegistry() *Registry { return &Registry{plugins: map[string]Plugin{}} }

// ErrRegistryFrozen is returned by Register after Freeze() has run.
var ErrRegistryFrozen = fmt.Errorf("analytics: registry is frozen; register plugins before serving")

func (r *Registry) Register(p Plugin) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRegistryFrozen
	}
	name := p.Name()
	if name == "" {
		return fmt.Errorf("analytics: plugin has empty Name()")
	}
	if _, dup := r.plugins[name]; dup {
		return fmt.Errorf("analytics: plugin %q already registered", name)
	}
	r.plugins[name] = p
	return nil
}

// Freeze closes the registry to further Register calls. Idempotent —
// safe to call multiple times. The server invokes this from Handler()
// so the boot-time plugin set is the runtime plugin set.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

// Frozen reports whether the registry has been frozen.
func (r *Registry) Frozen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frozen
}

func (r *Registry) Lookup(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.plugins[name]
	return p, ok
}

// Names returns the registered plugin names in lexical order. Sorted so
// /analytics responses are deterministic across calls — clients can diff
// or cache without surprise from map iteration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Run executes a slice of plugin requests against a result. Errors are
// collected per-plugin so a single misbehaving plugin doesn't kill the
// whole response. Duplicate plugin names in a single request are
// rejected on the second occurrence — otherwise the second silently
// overwrites the first in the result map and the client can't tell why.
// It also caps amplification: a request can't make the same expensive
// plugin run N times under one body limit.
func (r *Registry) Run(ctx context.Context, result *router.Result, reqs []Request) (map[string]any, map[string]string) {
	out := map[string]any{}
	errs := map[string]string{}
	seen := map[string]bool{}
	for _, req := range reqs {
		if seen[req.Name] {
			errs[req.Name] = "duplicate plugin in request"
			continue
		}
		seen[req.Name] = true
		p, ok := r.Lookup(req.Name)
		if !ok {
			errs[req.Name] = "unknown plugin"
			continue
		}
		v, err := p.Compute(ctx, result, req.Params)
		if err != nil {
			errs[req.Name] = err.Error()
			continue
		}
		out[req.Name] = v
	}
	return out, errs
}
