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

func TestFSM_RegisterTable(t *testing.T) {
	fsm := NewFSM()

	payload, _ := json.Marshal(RegisterTablePayload{Name: "Person", ShardKey: "name"})
	result := applyCommand(fsm, Command{Type: CmdRegisterTable, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	sm := fsm.GetShardMap()
	if sk, ok := sm.SchemaKeys["Person"]; !ok {
		t.Fatal("Person table not registered")
	} else if sk != "name" {
		t.Errorf("expected shard key 'name', got %q", sk)
	}
}

func TestFSM_RemoveTable(t *testing.T) {
	fsm := NewFSM()

	// Register first.
	payload, _ := json.Marshal(RegisterTablePayload{Name: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterTable, Payload: payload})

	// Remove.
	payload, _ = json.Marshal(RemoveTablePayload{Name: "Person"})
	result := applyCommand(fsm, Command{Type: CmdRemoveTable, Payload: payload})
	if result != nil {
		t.Fatalf("unexpected error: %v", result)
	}

	sm := fsm.GetShardMap()
	if _, ok := sm.SchemaKeys["Person"]; ok {
		t.Error("Person table should be removed")
	}
}

func TestFSM_SchemaCallback(t *testing.T) {
	fsm := NewFSM()

	var callbackTables map[string]string
	fsm.SetSchemaCallback(func(tables map[string]string) {
		callbackTables = tables
	})

	payload, _ := json.Marshal(RegisterTablePayload{Name: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterTable, Payload: payload})

	if callbackTables == nil {
		t.Fatal("callback not fired")
	}
	if callbackTables["Person"] != "name" {
		t.Errorf("callback got wrong schema: %v", callbackTables)
	}
}

func TestFSM_SnapshotRestore_IncludesSchema(t *testing.T) {
	fsm := NewFSM()

	// Add schema.
	payload, _ := json.Marshal(RegisterTablePayload{Name: "Person", ShardKey: "name"})
	applyCommand(fsm, Command{Type: CmdRegisterTable, Payload: payload})

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore into a new FSM with callback.
	fsm2 := NewFSM()
	var restoredTables map[string]string
	fsm2.SetSchemaCallback(func(tables map[string]string) {
		restoredTables = tables
	})
	if err := fsm2.Restore(sink.Reader()); err != nil {
		t.Fatal(err)
	}

	sm := fsm2.GetShardMap()
	if sk, ok := sm.SchemaKeys["Person"]; !ok {
		t.Error("schema not restored")
	} else if sk != "name" {
		t.Errorf("expected shard key 'name', got %q", sk)
	}

	// Callback should have fired during restore.
	if restoredTables == nil {
		t.Fatal("callback not fired on restore")
	}
	if restoredTables["Person"] != "name" {
		t.Errorf("callback got wrong schema on restore: %v", restoredTables)
	}
}

func TestShardMap_ShardsForNode(t *testing.T) {
	sm := ShardMap{
		Assignments: map[int]ShardAssignment{
			0: {Primary: "node-1", Replica: "node-2"},
			1: {Primary: "node-2", Replica: "node-1"},
			2: {Primary: "node-1", Replica: "node-3"},
			3: {Primary: "node-3", Replica: "node-2"},
		},
	}

	ids := sm.ShardsForNode("node-1")
	if len(ids) != 3 {
		t.Errorf("expected 3 shards for node-1, got %d: %v", len(ids), ids)
	}

	ids = sm.ShardsForNode("node-3")
	if len(ids) != 2 {
		t.Errorf("expected 2 shards for node-3, got %d: %v", len(ids), ids)
	}

	ids = sm.ShardsForNode("node-99")
	if len(ids) != 0 {
		t.Errorf("expected 0 shards for unknown node, got %d", len(ids))
	}
}

func TestShardMap_NodesForShard(t *testing.T) {
	sm := ShardMap{
		Assignments: map[int]ShardAssignment{
			0: {Primary: "node-1", Replica: "node-2"},
			1: {Primary: "node-2"},
		},
	}

	nodes := sm.NodesForShard(0)
	if len(nodes) != 2 || nodes[0] != "node-1" || nodes[1] != "node-2" {
		t.Errorf("unexpected nodes for shard 0: %v", nodes)
	}

	nodes = sm.NodesForShard(1)
	if len(nodes) != 1 || nodes[0] != "node-2" {
		t.Errorf("unexpected nodes for shard 1: %v", nodes)
	}

	nodes = sm.NodesForShard(99)
	if nodes != nil {
		t.Errorf("expected nil for unknown shard, got %v", nodes)
	}
}

func TestShardMap_PrimaryForShard(t *testing.T) {
	sm := ShardMap{
		Assignments: map[int]ShardAssignment{
			0: {Primary: "node-1", Replica: "node-2"},
		},
	}
	if p := sm.PrimaryForShard(0); p != "node-1" {
		t.Errorf("expected node-1, got %s", p)
	}
	if p := sm.PrimaryForShard(99); p != "" {
		t.Errorf("expected empty for unknown shard, got %s", p)
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
