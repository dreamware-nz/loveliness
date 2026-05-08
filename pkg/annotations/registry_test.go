package annotations

import (
	"testing"
	"time"
)

func TestValidateTarget(t *testing.T) {
	ok := []string{
		"cluster",
		"db:default",
		"db:default/table:Person",
		"db:default/table:Person/property:age",
		"db:default/edge:KNOWS",
		"db:default/edge:KNOWS/property:since",
		"db:my-db_2/saved_query:topFollowers",
	}
	for _, target := range ok {
		if _, err := ValidateTarget(target); err != nil {
			t.Errorf("ValidateTarget(%q) err: %v", target, err)
		}
	}

	bad := []string{
		"",
		"table:Person",                // first segment must be db: or cluster
		"db:default/Person",           // missing kind:
		"db:default/widget:Foo",       // unknown kind
		"db:default/table:Person; --", // injection-shaped name
		"db:/table:Person",            // empty db name
		"db:default/table:",           // empty table name
	}
	for _, target := range bad {
		if _, err := ValidateTarget(target); err == nil {
			t.Errorf("ValidateTarget(%q) accepted, expected error", target)
		}
	}
}

func TestRegistrySetGetDelete(t *testing.T) {
	r := New()
	a := Annotation{
		Target:      "db:default/table:Person",
		Description: "people in the social graph",
		Examples: []QueryExample{
			{Title: "by name", Query: "MATCH (p:Person {name: $n}) RETURN p", Params: map[string]any{"n": "Alice"}},
		},
		Tags: []string{"core"},
	}
	if err := r.Set(a); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok := r.Get("db:default/table:Person")
	if !ok {
		t.Fatal("get: not found")
	}
	if got.Description != a.Description || len(got.Examples) != 1 || got.UpdatedAt.IsZero() {
		t.Fatalf("unexpected annotation: %+v", got)
	}

	// Latest-wins: a second Set replaces the body entirely.
	if err := r.Set(Annotation{Target: a.Target, Description: "updated"}); err != nil {
		t.Fatalf("re-set: %v", err)
	}
	got, _ = r.Get(a.Target)
	if got.Description != "updated" || len(got.Examples) != 0 {
		t.Fatalf("latest-wins broken: %+v", got)
	}

	if !r.Delete(a.Target) {
		t.Fatal("delete returned false on present key")
	}
	if _, ok := r.Get(a.Target); ok {
		t.Fatal("get after delete returned ok")
	}
}

func TestRegistryListPrefix(t *testing.T) {
	r := New()
	for _, tgt := range []string{
		"db:default/table:Person",
		"db:default/table:Movie",
		"db:default/edge:KNOWS",
		"db:other/table:Person",
		"cluster",
	} {
		if err := r.Set(Annotation{Target: tgt, Description: tgt}); err != nil {
			t.Fatalf("set %q: %v", tgt, err)
		}
	}

	all := r.List("")
	if len(all) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(all))
	}
	if all[0].Target != "cluster" {
		t.Errorf("expected cluster first (sorted): %v", all[0].Target)
	}

	defaultDB := r.List("db:default/")
	if len(defaultDB) != 3 {
		t.Fatalf("expected 3 entries under db:default/, got %d", len(defaultDB))
	}
}

func TestRegistryRejectsExampleWithoutQuery(t *testing.T) {
	r := New()
	err := r.Set(Annotation{
		Target:   "db:default/table:Person",
		Examples: []QueryExample{{Title: "broken"}},
	})
	if err == nil {
		t.Fatal("expected error for example with no query")
	}
}

func TestRegistrySnapshotRestore(t *testing.T) {
	r := New()
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a := Annotation{
		Target:    "db:default/table:Person",
		Tags:      []string{"core", "pii"},
		Extra:     map[string]string{"owner": "data-team"},
		Examples:  []QueryExample{{Query: "MATCH (p:Person) RETURN count(p)"}},
		UpdatedAt: stamp,
	}
	if err := r.Set(a); err != nil {
		t.Fatal(err)
	}

	snap := r.Snapshot()
	r2 := New()
	r2.Restore(snap)
	got, ok := r2.Get(a.Target)
	if !ok {
		t.Fatal("not found in restored registry")
	}
	if got.UpdatedAt != stamp {
		t.Errorf("timestamp lost: %v", got.UpdatedAt)
	}
	if got.Tags[1] != "pii" || got.Extra["owner"] != "data-team" {
		t.Errorf("collections lost: %+v", got)
	}

	// Mutating the snapshot must not affect the source registry (deep copy).
	snap[a.Target].Tags[0] = "TAMPERED"
	if r.List("")[0].Tags[0] == "TAMPERED" {
		t.Fatal("Snapshot did not deep-copy tags")
	}
}
