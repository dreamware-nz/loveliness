package router

import (
	"context"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/catalog"
	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestDatabaseRouter_RegisterAndQuery(t *testing.T) {
	cat := catalog.NewCatalog()
	db, _ := cat.CreateDatabase("social", 2)

	dr := NewDatabaseRouter(cat, 5*time.Second)

	// Create shards using MemoryStore.
	shards := make([]*shard.Shard, len(db.ShardIDs))
	for i, id := range db.ShardIDs {
		shards[i] = shard.NewShard(id, shard.NewMemoryStore(), 4)
	}
	dr.RegisterDatabase("social", shards)

	// GetRouter should work.
	r, err := dr.GetRouter("social")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil router")
	}

	// Unknown database should fail.
	_, err = dr.GetRouter("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent database")
	}
}

func TestDatabaseRouter_UnregisterDatabase(t *testing.T) {
	cat := catalog.NewCatalog()
	db, _ := cat.CreateDatabase("social", 2)

	dr := NewDatabaseRouter(cat, 5*time.Second)
	shards := make([]*shard.Shard, len(db.ShardIDs))
	for i, id := range db.ShardIDs {
		shards[i] = shard.NewShard(id, shard.NewMemoryStore(), 4)
	}
	dr.RegisterDatabase("social", shards)

	returned := dr.UnregisterDatabase("social")
	if len(returned) != 2 {
		t.Errorf("expected 2 shards returned, got %d", len(returned))
	}

	_, err := dr.GetRouter("social")
	if err == nil {
		t.Fatal("expected error after unregister")
	}
}

func TestDatabaseRouter_Execute(t *testing.T) {
	cat := catalog.NewCatalog()
	db, _ := cat.CreateDatabase("social", 2)

	dr := NewDatabaseRouter(cat, 5*time.Second)
	shards := make([]*shard.Shard, len(db.ShardIDs))
	for i, id := range db.ShardIDs {
		shards[i] = shard.NewShard(id, shard.NewMemoryStore(), 4)
	}
	dr.RegisterDatabase("social", shards)

	// Execute against unregistered db should fail.
	_, err := dr.Execute(context.Background(), "nonexistent", "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected error for nonexistent database")
	}

	// Execute against registered db should work (scatter-gather over memory stores).
	result, err := dr.Execute(context.Background(), "social", "MATCH (n) RETURN n")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestDatabaseRouter_Isolation(t *testing.T) {
	cat := catalog.NewCatalog()
	db1, _ := cat.CreateDatabase("alpha", 2)
	db2, _ := cat.CreateDatabase("beta", 2)

	dr := NewDatabaseRouter(cat, 5*time.Second)

	for _, db := range []*catalog.Database{db1, db2} {
		shards := make([]*shard.Shard, len(db.ShardIDs))
		for i, id := range db.ShardIDs {
			shards[i] = shard.NewShard(id, shard.NewMemoryStore(), 4)
		}
		dr.RegisterDatabase(db.Name, shards)
	}

	// Each database should have its own schema registry.
	s1 := dr.GetSchema("alpha")
	s2 := dr.GetSchema("beta")
	if s1 == s2 {
		t.Fatal("schema registries should be different objects")
	}

	// Register a table in alpha only.
	s1.Register("Person", "name")
	if s2.GetShardKey("Person") != "" {
		t.Error("Person should not exist in beta's schema")
	}
}

func TestIsAdminCommand(t *testing.T) {
	cases := []struct {
		input string
		admin bool
	}{
		{"CREATE DATABASE social SHARDS 4", true},
		{"create database social shards 4", true},
		{"STOP DATABASE social", true},
		{"START DATABASE social", true},
		{"DROP DATABASE social", true},
		{"SHOW DATABASES", true},
		{"MATCH (n) RETURN n", false},
		{"CREATE NODE TABLE Person(name STRING PRIMARY KEY)", false},
		{"CREATE (n:Person {name: 'Alice'})", false},
	}
	for _, tc := range cases {
		if got := IsAdminCommand(tc.input); got != tc.admin {
			t.Errorf("IsAdminCommand(%q) = %v, want %v", tc.input, got, tc.admin)
		}
	}
}

func TestParseAdminCommand(t *testing.T) {
	cases := []struct {
		input      string
		typ        AdminCommandType
		name       string
		shardCount int
	}{
		{"CREATE DATABASE social SHARDS 4", AdminCreateDatabase, "social", 4},
		{"CREATE DATABASE test", AdminCreateDatabase, "test", 3}, // default shards
		{"STOP DATABASE social", AdminStopDatabase, "social", 0},
		{"START DATABASE social", AdminStartDatabase, "social", 0},
		{"DROP DATABASE social", AdminDropDatabase, "social", 0},
		{"SHOW DATABASES", AdminShowDatabases, "", 0},
	}
	for _, tc := range cases {
		cmd := ParseAdminCommand(tc.input)
		if cmd == nil {
			t.Fatalf("ParseAdminCommand(%q) returned nil", tc.input)
		}
		if cmd.Type != tc.typ {
			t.Errorf("%q: type=%d, want %d", tc.input, cmd.Type, tc.typ)
		}
		if cmd.Name != tc.name {
			t.Errorf("%q: name=%q, want %q", tc.input, cmd.Name, tc.name)
		}
		if tc.shardCount > 0 && cmd.ShardCount != tc.shardCount {
			t.Errorf("%q: shardCount=%d, want %d", tc.input, cmd.ShardCount, tc.shardCount)
		}
	}
}
