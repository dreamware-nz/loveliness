package cluster

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/johnjansen/loveliness/pkg/schema"
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

func TestFSM_RegisterSchema(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(RegisterSchemaPayload{TableName: "Person", ShardKey: "name"})
	result := applyCommand(fsm, Command{Type: CmdRegisterSchema, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	tables := fsm.GetSchema()
	if ts, ok := tables["PERSON"]; !ok {
		t.Fatal("PERSON not registered")
	} else if ts.Name != "Person" || ts.ShardKey != "name" {
		t.Errorf("unexpected schema: %+v", ts)
	}
}

func TestFSM_RemoveSchema(t *testing.T) {
	fsm := NewFSM()

	// Register first.
	payload, _ := json.Marshal(RegisterSchemaPayload{TableName: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterSchema, Payload: payload})

	// Remove it.
	payload, _ = json.Marshal(RemoveSchemaPayload{TableName: "Person"})
	result := applyCommand(fsm, Command{Type: CmdRemoveSchema, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	tables := fsm.GetSchema()
	if _, ok := tables["PERSON"]; ok {
		t.Error("PERSON should be removed")
	}
}

func TestFSM_SnapshotRestore_WithSchema(t *testing.T) {
	fsm := NewFSM()

	// Add shard map and schema state.
	payload, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "node-1"})
	applyCommand(fsm, Command{Type: CmdAssignShard, Payload: payload})
	payload, _ = json.Marshal(RegisterSchemaPayload{TableName: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterSchema, Payload: payload})

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

	tables := fsm2.GetSchema()
	if ts, ok := tables["PERSON"]; !ok {
		t.Error("PERSON schema not restored")
	} else if ts.ShardKey != "name" {
		t.Errorf("expected shard_key=name, got %s", ts.ShardKey)
	}
}

func TestFSM_GetSchema_IsCopy(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(RegisterSchemaPayload{TableName: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterSchema, Payload: payload})

	tables := fsm.GetSchema()
	tables["PERSON"] = schema.TableSchema{Name: "Mutated", ShardKey: "mutated"}

	// Original should be unchanged.
	original := fsm.GetSchema()
	if original["PERSON"].ShardKey != "name" {
		t.Error("GetSchema should return a copy, not a reference")
	}
}
