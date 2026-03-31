package cluster

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCommand(fsm *FSM, cmd Command) interface{} {
	data, _ := json.Marshal(cmd)
	return fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_AssignShard(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "node-1", Replica: "node-2"})
	result := applyCommand(fsm, Command{Type: CmdAssignShard, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	sm := fsm.GetShardMap()
	if a, ok := sm.Assignments[0]; !ok {
		t.Fatal("shard 0 not assigned")
	} else if a.Primary != "node-1" || a.Replica != "node-2" {
		t.Errorf("unexpected assignment: %+v", a)
	}
}

func TestFSM_JoinNode(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(JoinNodePayload{NodeInfo: NodeInfo{
		ID: "node-1", RaftAddr: ":9000", GRPCAddr: ":9001", HTTPAddr: ":8080", Alive: true,
	}})
	result := applyCommand(fsm, Command{Type: CmdJoinNode, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	sm := fsm.GetShardMap()
	if node, ok := sm.Nodes["node-1"]; !ok {
		t.Fatal("node-1 not found")
	} else if !node.Alive {
		t.Error("node-1 should be alive")
	}
}

func TestFSM_RemoveNode(t *testing.T) {
	fsm := NewFSM()

	// Add node first.
	payload, _ := json.Marshal(JoinNodePayload{NodeInfo: NodeInfo{
		ID: "node-1", Alive: true,
	}})
	applyCommand(fsm, Command{Type: CmdJoinNode, Payload: payload})

	// Remove it.
	payload, _ = json.Marshal(RemoveNodePayload{NodeID: "node-1"})
	applyCommand(fsm, Command{Type: CmdRemoveNode, Payload: payload})

	sm := fsm.GetShardMap()
	if node, ok := sm.Nodes["node-1"]; !ok {
		t.Fatal("node-1 should still exist but be marked dead")
	} else if node.Alive {
		t.Error("node-1 should not be alive")
	}
}

func TestFSM_PromoteReplica(t *testing.T) {
	fsm := NewFSM()

	// Assign shard with primary and replica.
	payload, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "node-1", Replica: "node-2"})
	applyCommand(fsm, Command{Type: CmdAssignShard, Payload: payload})

	// Promote replica.
	payload, _ = json.Marshal(PromoteReplicaPayload{ShardID: 0, NewPrimary: "node-2"})
	applyCommand(fsm, Command{Type: CmdPromoteReplica, Payload: payload})

	sm := fsm.GetShardMap()
	a := sm.Assignments[0]
	if a.Primary != "node-2" {
		t.Errorf("expected primary=node-2, got %s", a.Primary)
	}
	if a.Replica != "" {
		t.Errorf("expected empty replica, got %s", a.Replica)
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	fsm := NewFSM()

	// Add some state.
	payload, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "node-1"})
	applyCommand(fsm, Command{Type: CmdAssignShard, Payload: payload})
	payload, _ = json.Marshal(JoinNodePayload{NodeInfo: NodeInfo{ID: "node-1", Alive: true}})
	applyCommand(fsm, Command{Type: CmdJoinNode, Payload: payload})

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Persist to a buffer.
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore into a new FSM.
	fsm2 := NewFSM()
	if err := fsm2.Restore(sink.Reader()); err != nil {
		t.Fatal(err)
	}

	sm := fsm2.GetShardMap()
	if _, ok := sm.Assignments[0]; !ok {
		t.Error("shard 0 not restored")
	}
	if _, ok := sm.Nodes["node-1"]; !ok {
		t.Error("node-1 not restored")
	}
}

func TestFSM_CreateDatabase(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(CreateDatabasePayload{Name: "social", ShardCount: 4})
	result := applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})
	// Successful create returns the *catalog.Database.
	if _, ok := result.(error); ok {
		t.Fatalf("unexpected error: %v", result)
	}

	db := fsm.GetCatalog().GetDatabase("social")
	if db == nil {
		t.Fatal("database not created")
	}
	if db.ShardCount != 4 || len(db.ShardIDs) != 4 {
		t.Errorf("unexpected shard count: %d, IDs: %v", db.ShardCount, db.ShardIDs)
	}
}

func TestFSM_StopDatabase(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(CreateDatabasePayload{Name: "social", ShardCount: 2})
	applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})

	payload, _ = json.Marshal(DatabaseNamePayload{Name: "social"})
	result := applyCommand(fsm, Command{Type: CmdStopDatabase, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	db := fsm.GetCatalog().GetDatabase("social")
	if db.State.String() != "STOPPED" {
		t.Errorf("expected STOPPED, got %s", db.State)
	}
}

func TestFSM_StartDatabase(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(CreateDatabasePayload{Name: "social", ShardCount: 2})
	applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})

	payload, _ = json.Marshal(DatabaseNamePayload{Name: "social"})
	applyCommand(fsm, Command{Type: CmdStopDatabase, Payload: payload})

	payload, _ = json.Marshal(DatabaseNamePayload{Name: "social"})
	result := applyCommand(fsm, Command{Type: CmdStartDatabase, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	db := fsm.GetCatalog().GetDatabase("social")
	if db.State.String() != "ONLINE" {
		t.Errorf("expected ONLINE, got %s", db.State)
	}
}

func TestFSM_DeleteDatabase(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(CreateDatabasePayload{Name: "social", ShardCount: 2})
	applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})

	payload, _ = json.Marshal(DatabaseNamePayload{Name: "social"})
	result := applyCommand(fsm, Command{Type: CmdDeleteDatabase, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	db := fsm.GetCatalog().GetDatabase("social")
	if db.State.String() != "DELETED" {
		t.Errorf("expected DELETED, got %s", db.State)
	}
}

func TestFSM_DatabaseSnapshotRestore(t *testing.T) {
	fsm := NewFSM()

	// Create databases.
	payload, _ := json.Marshal(CreateDatabasePayload{Name: "alpha", ShardCount: 3})
	applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})
	payload, _ = json.Marshal(CreateDatabasePayload{Name: "beta", ShardCount: 2})
	applyCommand(fsm, Command{Type: CmdCreateDatabase, Payload: payload})
	payload, _ = json.Marshal(DatabaseNamePayload{Name: "beta"})
	applyCommand(fsm, Command{Type: CmdStopDatabase, Payload: payload})

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore into new FSM.
	fsm2 := NewFSM()
	if err := fsm2.Restore(sink.Reader()); err != nil {
		t.Fatal(err)
	}

	alpha := fsm2.GetCatalog().GetDatabase("alpha")
	if alpha == nil || alpha.State.String() != "ONLINE" || alpha.ShardCount != 3 {
		t.Errorf("alpha not restored: %+v", alpha)
	}
	beta := fsm2.GetCatalog().GetDatabase("beta")
	if beta == nil || beta.State.String() != "STOPPED" {
		t.Errorf("beta not restored: %+v", beta)
	}

	// Verify shard ID counter was restored — new db should get ID 5.
	payload, _ = json.Marshal(CreateDatabasePayload{Name: "gamma", ShardCount: 1})
	result := applyCommand(fsm2, Command{Type: CmdCreateDatabase, Payload: payload})
	if _, ok := result.(error); ok {
		t.Fatalf("unexpected error: %v", result)
	}
	gamma := fsm2.GetCatalog().GetDatabase("gamma")
	if gamma.ShardIDs[0] != 5 {
		t.Errorf("expected shard ID 5, got %d", gamma.ShardIDs[0])
	}
}

func TestFSM_GetShardMap_IsCopy(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "node-1"})
	applyCommand(fsm, Command{Type: CmdAssignShard, Payload: payload})

	sm := fsm.GetShardMap()
	sm.Assignments[0] = ShardAssignment{Primary: "mutated"}

	// Original should be unchanged.
	original := fsm.GetShardMap()
	if original.Assignments[0].Primary != "node-1" {
		t.Error("GetShardMap should return a copy, not a reference")
	}
}
