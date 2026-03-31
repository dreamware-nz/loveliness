package catalog

import (
	"testing"
)

func TestCreateDatabase(t *testing.T) {
	c := NewCatalog()
	db, err := c.CreateDatabase("social", 4)
	if err != nil {
		t.Fatal(err)
	}
	if db.Name != "social" {
		t.Errorf("expected name=social, got %s", db.Name)
	}
	if db.ShardCount != 4 {
		t.Errorf("expected shard_count=4, got %d", db.ShardCount)
	}
	if db.State != StateOnline {
		t.Errorf("expected state=ONLINE, got %s", db.State)
	}
	if len(db.ShardIDs) != 4 {
		t.Fatalf("expected 4 shard IDs, got %d", len(db.ShardIDs))
	}
	// Shard IDs should be 0,1,2,3.
	for i, id := range db.ShardIDs {
		if id != i {
			t.Errorf("shard ID[%d]=%d, expected %d", i, id, i)
		}
	}
}

func TestCreateDatabaseGlobalShardIDs(t *testing.T) {
	c := NewCatalog()
	db1, _ := c.CreateDatabase("alpha", 3)
	db2, _ := c.CreateDatabase("beta", 2)

	// beta's shard IDs should continue from alpha's.
	if db2.ShardIDs[0] != db1.ShardIDs[2]+1 {
		t.Errorf("expected beta shard IDs to start at %d, got %d", db1.ShardIDs[2]+1, db2.ShardIDs[0])
	}
}

func TestCreateDatabaseDuplicate(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_, err := c.CreateDatabase("social", 3)
	if err == nil {
		t.Fatal("expected error for duplicate name")
	}
}

func TestCreateDatabaseInvalidName(t *testing.T) {
	c := NewCatalog()

	cases := []string{"", "UPPER", "has space", "-leading-dash", "a@b", "x" + string(make([]byte, 64))}
	for _, name := range cases {
		if _, err := c.CreateDatabase(name, 1); err == nil {
			t.Errorf("expected error for name %q", name)
		}
	}
}

func TestCreateDatabaseValidNames(t *testing.T) {
	c := NewCatalog()
	for _, name := range []string{"a", "my-db", "test_123", "0db"} {
		if _, err := c.CreateDatabase(name, 1); err != nil {
			t.Errorf("unexpected error for name %q: %v", name, err)
		}
	}
}

func TestStopDatabase(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)

	if err := c.StopDatabase("social"); err != nil {
		t.Fatal(err)
	}
	db := c.GetDatabase("social")
	if db.State != StateStopped {
		t.Errorf("expected STOPPED, got %s", db.State)
	}
}

func TestStopDatabaseInvalidState(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_ = c.StopDatabase("social")

	// Stopping an already stopped database should fail.
	if err := c.StopDatabase("social"); err == nil {
		t.Fatal("expected error stopping a STOPPED database")
	}
}

func TestStartDatabase(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_ = c.StopDatabase("social")

	if err := c.StartDatabase("social"); err != nil {
		t.Fatal(err)
	}
	db := c.GetDatabase("social")
	if db.State != StateOnline {
		t.Errorf("expected ONLINE, got %s", db.State)
	}
}

func TestStartDatabaseInvalidState(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)

	// Starting an ONLINE database should fail.
	if err := c.StartDatabase("social"); err == nil {
		t.Fatal("expected error starting an ONLINE database")
	}
}

func TestDeleteDatabase(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 4)

	if err := c.DeleteDatabase("social"); err != nil {
		t.Fatal(err)
	}
	db := c.GetDatabase("social")
	if db == nil {
		t.Fatal("deleted database should still be readable as tombstone")
	}
	if db.State != StateDeleted {
		t.Errorf("expected DELETED, got %s", db.State)
	}
}

func TestDeleteDatabaseFromStopped(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_ = c.StopDatabase("social")

	if err := c.DeleteDatabase("social"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteDatabaseAlreadyDeleted(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_ = c.DeleteDatabase("social")

	if err := c.DeleteDatabase("social"); err == nil {
		t.Fatal("expected error deleting an already deleted database")
	}
}

func TestDeleteDatabaseFreesShardBudget(t *testing.T) {
	c := NewCatalog()
	c.SetLimits(64, 5) // Max 5 shards.

	_, _ = c.CreateDatabase("a", 3)
	_, _ = c.CreateDatabase("b", 2)

	// Should be at limit.
	_, err := c.CreateDatabase("c", 1)
	if err == nil {
		t.Fatal("expected shard limit error")
	}

	// Delete a, freeing 3 shards.
	_ = c.DeleteDatabase("a")

	// Now creating c should work.
	_, err = c.CreateDatabase("c", 2)
	if err != nil {
		t.Fatalf("expected create to succeed after delete: %v", err)
	}
}

func TestDeleteDatabaseNameReuse(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)
	_ = c.DeleteDatabase("social")

	// Name is still taken (tombstone).
	_, err := c.CreateDatabase("social", 3)
	if err == nil {
		t.Fatal("expected error: tombstone still present")
	}

	// Remove tombstone.
	c.RemoveTombstone("social")

	// Now it should work.
	_, err = c.CreateDatabase("social", 3)
	if err != nil {
		t.Fatalf("expected create to succeed after tombstone removal: %v", err)
	}
}

func TestListDatabases(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("a", 1)
	_, _ = c.CreateDatabase("b", 2)
	_, _ = c.CreateDatabase("c", 3)
	_ = c.DeleteDatabase("b")

	list := c.ListDatabases()
	if len(list) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(list))
	}
}

func TestShardOwner(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("alpha", 3) // shards 0,1,2
	_, _ = c.CreateDatabase("beta", 2)  // shards 3,4

	if owner := c.ShardOwner(0); owner != "alpha" {
		t.Errorf("expected alpha, got %s", owner)
	}
	if owner := c.ShardOwner(3); owner != "beta" {
		t.Errorf("expected beta, got %s", owner)
	}
	if owner := c.ShardOwner(99); owner != "" {
		t.Errorf("expected empty, got %s", owner)
	}
}

func TestSnapshotRestore(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 4)
	_, _ = c.CreateDatabase("catalog", 2)
	_ = c.StopDatabase("catalog")

	snap := c.Snapshot()

	c2 := NewCatalog()
	c2.Restore(snap)

	db := c2.GetDatabase("social")
	if db == nil || db.State != StateOnline || db.ShardCount != 4 {
		t.Errorf("social not restored correctly: %+v", db)
	}
	db2 := c2.GetDatabase("catalog")
	if db2 == nil || db2.State != StateStopped {
		t.Errorf("catalog not restored correctly: %+v", db2)
	}

	// nextShardID should be restored — creating a new db should get shard ID 6.
	db3, err := c2.CreateDatabase("new", 1)
	if err != nil {
		t.Fatal(err)
	}
	if db3.ShardIDs[0] != 6 {
		t.Errorf("expected shard ID 6, got %d", db3.ShardIDs[0])
	}
}

func TestMaxDatabasesLimit(t *testing.T) {
	c := NewCatalog()
	c.SetLimits(2, 256)

	_, _ = c.CreateDatabase("a", 1)
	_, _ = c.CreateDatabase("b", 1)

	_, err := c.CreateDatabase("c", 1)
	if err == nil {
		t.Fatal("expected max databases error")
	}
}

func TestMaxShardsLimit(t *testing.T) {
	c := NewCatalog()
	c.SetLimits(64, 5)

	_, _ = c.CreateDatabase("a", 3)
	_, err := c.CreateDatabase("b", 3)
	if err == nil {
		t.Fatal("expected shard limit error")
	}

	// Should still be able to create within limit.
	_, err = c.CreateDatabase("b", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetDatabaseReturnsCopy(t *testing.T) {
	c := NewCatalog()
	_, _ = c.CreateDatabase("social", 2)

	db := c.GetDatabase("social")
	db.Name = "mutated"
	db.ShardIDs[0] = 999

	original := c.GetDatabase("social")
	if original.Name != "social" {
		t.Error("GetDatabase should return a copy")
	}
	if original.ShardIDs[0] != 0 {
		t.Error("GetDatabase should deep-copy ShardIDs")
	}
}
