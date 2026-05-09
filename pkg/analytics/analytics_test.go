package analytics

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/johnjansen/loveliness/pkg/router"
)

type stubPlugin struct {
	name string
	out  any
	err  error
}

func (s stubPlugin) Name() string { return s.name }
func (s stubPlugin) Compute(_ context.Context, _ *router.Result, _ map[string]any) (any, error) {
	return s.out, s.err
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stubPlugin{name: "x"}); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := r.Register(stubPlugin{name: "x"}); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
}

func TestRegistry_RegisterEmptyName(t *testing.T) {
	if err := NewRegistry().Register(stubPlugin{name: ""}); err == nil {
		t.Fatal("expected empty Name() to be rejected")
	}
}

func TestRegistry_RunUnknownPlugin(t *testing.T) {
	r := NewRegistry()
	out, errs := r.Run(context.Background(), &router.Result{}, []Request{{Name: "missing"}})
	if len(out) != 0 {
		t.Errorf("expected empty out, got %v", out)
	}
	if errs["missing"] != "unknown plugin" {
		t.Errorf("expected 'unknown plugin', got %q", errs["missing"])
	}
}

func TestRegistry_RunErrorIsolation(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(stubPlugin{name: "good", out: 42})
	_ = r.Register(stubPlugin{name: "bad", err: errors.New("kaboom")})

	out, errs := r.Run(context.Background(), &router.Result{}, []Request{
		{Name: "good"}, {Name: "bad"},
	})
	if out["good"] != 42 {
		t.Errorf("good: %v", out["good"])
	}
	if _, ok := out["bad"]; ok {
		t.Errorf("bad should not appear in out")
	}
	if errs["bad"] != "kaboom" {
		t.Errorf("bad err: %q", errs["bad"])
	}
}

func TestRegistry_Names(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(stubPlugin{name: "b"})
	_ = r.Register(stubPlugin{name: "a"})
	got := r.Names()
	if len(got) != 2 {
		t.Fatalf("names: %v", got)
	}
	// Sorted lexically for deterministic /analytics responses.
	if got[0] != "a" || got[1] != "b" {
		t.Errorf("expected sorted names, got %v", got)
	}
}

func TestRegistry_RunDuplicateRejected(t *testing.T) {
	r := NewRegistry()
	calls := 0
	_ = r.Register(stubPluginFunc{name: "p", fn: func() (any, error) {
		calls++
		return calls, nil
	}})
	out, errs := r.Run(context.Background(), &router.Result{}, []Request{
		{Name: "p"}, {Name: "p"},
	})
	if calls != 1 {
		t.Errorf("expected exactly 1 invocation, got %d (amplification!)", calls)
	}
	if out["p"] == nil {
		t.Errorf("first occurrence should still produce a value")
	}
	if errs["p"] != "duplicate plugin in request" {
		t.Errorf("expected duplicate error, got %q", errs["p"])
	}
}

type stubPluginFunc struct {
	name string
	fn   func() (any, error)
}

func (s stubPluginFunc) Name() string { return s.name }
func (s stubPluginFunc) Compute(_ context.Context, _ *router.Result, _ map[string]any) (any, error) {
	return s.fn()
}

func TestRegistry_FreezeBlocksRegister(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(stubPlugin{name: "before"}); err != nil {
		t.Fatalf("pre-freeze register: %v", err)
	}
	r.Freeze()
	if !r.Frozen() {
		t.Fatal("Frozen() should report true after Freeze()")
	}
	err := r.Register(stubPlugin{name: "after"})
	if !errors.Is(err, ErrRegistryFrozen) {
		t.Errorf("expected ErrRegistryFrozen, got %v", err)
	}
	// Pre-freeze plugin still resolvable.
	if _, ok := r.Lookup("before"); !ok {
		t.Error("pre-freeze plugin should still be looked up")
	}
	if _, ok := r.Lookup("after"); ok {
		t.Error("post-freeze plugin should not be registered")
	}
}

func TestRegistry_FreezeIdempotent(t *testing.T) {
	r := NewRegistry()
	r.Freeze()
	r.Freeze()
	r.Freeze()
	if !r.Frozen() {
		t.Fatal("Frozen() should report true")
	}
	if err := r.Register(stubPlugin{name: "x"}); !errors.Is(err, ErrRegistryFrozen) {
		t.Errorf("expected ErrRegistryFrozen, got %v", err)
	}
}

// TestRegistry_ConcurrentRegisterAndFreeze stresses the mutex contract:
// many goroutines race Register() against Freeze(). After the dust
// settles, every plugin name in the registry must satisfy Lookup, and
// no Register that "succeeded" can have happened after Freeze. Run
// under -race to catch unsynchronised access.
func TestRegistry_ConcurrentRegisterAndFreeze(t *testing.T) {
	const N = 64
	r := NewRegistry()

	var wg sync.WaitGroup
	wg.Add(N + 1)

	registered := make([]bool, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			err := r.Register(stubPlugin{name: "p" + strconv.Itoa(i)})
			if err == nil {
				registered[i] = true
			} else if !errors.Is(err, ErrRegistryFrozen) {
				t.Errorf("unexpected register err: %v", err)
			}
		}(i)
	}

	go func() {
		defer wg.Done()
		r.Freeze()
	}()

	wg.Wait()

	if !r.Frozen() {
		t.Fatal("registry should be frozen after Freeze() returns")
	}
	for i, ok := range registered {
		if !ok {
			continue
		}
		name := "p" + strconv.Itoa(i)
		if _, found := r.Lookup(name); !found {
			t.Errorf("Register %q reported success but Lookup says missing", name)
		}
	}
}
