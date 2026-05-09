package analytics

import (
	"context"
	"errors"
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
	_ = r.Register(stubPlugin{name: "a"})
	_ = r.Register(stubPlugin{name: "b"})
	got := r.Names()
	if len(got) != 2 {
		t.Errorf("names: %v", got)
	}
}
