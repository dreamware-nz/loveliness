package schema

import (
	"testing"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	r.Register("Person", "name")

	key := r.GetShardKey("Person")
	if key != "name" {
		t.Errorf("expected shard key 'name', got %q", key)
	}
	// Case insensitive lookup.
	key = r.GetShardKey("person")
	if key != "name" {
		t.Errorf("expected case-insensitive lookup to work, got %q", key)
	}
}

func TestRegistry_UnknownTable(t *testing.T) {
	r := NewRegistry()
	key := r.GetShardKey("Unknown")
	if key != "" {
		t.Errorf("expected empty shard key for unknown table, got %q", key)
	}
}

func TestRegistry_Remove(t *testing.T) {
	r := NewRegistry()
	r.Register("Person", "name")
	r.Remove("Person")

	key := r.GetShardKey("Person")
	if key != "" {
		t.Errorf("expected empty after remove, got %q", key)
	}
}

func TestRegistry_Restore(t *testing.T) {
	r := NewRegistry()
	r.Register("Person", "name")

	tables := map[string]TableSchema{
		"COMPANY": {Name: "Company", ShardKey: "id"},
	}
	r.Restore(tables)

	if r.GetShardKey("Person") != "" {
		t.Error("Person should be gone after restore")
	}
	if r.GetShardKey("Company") != "id" {
		t.Error("Company should exist after restore")
	}
}

func TestRegistry_All(t *testing.T) {
	r := NewRegistry()
	r.Register("Person", "name")
	r.Register("Company", "id")

	all := r.All()
	if len(all) != 2 {
		t.Errorf("expected 2 tables, got %d", len(all))
	}
	// Mutating the returned map shouldn't affect registry.
	delete(all, "PERSON")
	if r.GetShardKey("Person") != "name" {
		t.Error("All() should return a copy")
	}
}

func TestParseCreateNodeTable_PrimaryKeyClause(t *testing.T) {
	tests := []struct {
		cypher    string
		wantTable string
		wantKey   string
	}{
		{
			"CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))",
			"Person", "name",
		},
		{
			"CREATE NODE TABLE Company(id STRING, revenue DOUBLE, PRIMARY KEY(id))",
			"Company", "id",
		},
		{
			"  CREATE NODE TABLE  User ( email STRING, PRIMARY KEY(email) )",
			"User", "email",
		},
	}
	for _, tt := range tests {
		name, key, err := ParseCreateNodeTable(tt.cypher)
		if err != nil {
			t.Errorf("ParseCreateNodeTable(%q): %v", tt.cypher, err)
			continue
		}
		if name != tt.wantTable {
			t.Errorf("table: got %q, want %q", name, tt.wantTable)
		}
		if key != tt.wantKey {
			t.Errorf("key: got %q, want %q", key, tt.wantKey)
		}
	}
}

func TestParseCreateNodeTable_InlinePrimaryKey(t *testing.T) {
	name, key, err := ParseCreateNodeTable("CREATE NODE TABLE Person(name STRING PRIMARY KEY, age INT64)")
	if err != nil {
		t.Fatal(err)
	}
	if name != "Person" || key != "name" {
		t.Errorf("got (%q, %q), want (Person, name)", name, key)
	}
}

func TestParseCreateNodeTable_NoPrimaryKey(t *testing.T) {
	_, _, err := ParseCreateNodeTable("CREATE NODE TABLE Person(name STRING, age INT64)")
	if err == nil {
		t.Fatal("expected error for missing PRIMARY KEY")
	}
}

func TestParseCreateNodeTable_NotCreateNodeTable(t *testing.T) {
	_, _, err := ParseCreateNodeTable("CREATE REL TABLE KNOWS(FROM Person TO Person)")
	if err == nil {
		t.Fatal("expected error for non-node-table")
	}
}

func TestParseDropTable(t *testing.T) {
	name, err := ParseDropTable("DROP TABLE Person")
	if err != nil {
		t.Fatal(err)
	}
	if name != "Person" {
		t.Errorf("got %q, want Person", name)
	}
}
