package cluster

import (
	"encoding/json"
	"io"
	"reflect"
	"sort"
	"testing"
)

func TestUnderReplicatedShards(t *testing.T) {
	sm := ShardMap{
		Assignments: map[int]ShardAssignment{
			0: {Primary: "a", Replicas: []string{"b"}},      // 2 holders
			1: {Primary: "a"},                                // 1 holder
			2: {Primary: "a", Replicas: []string{"b", "c"}}, // 3 holders
			3: {Primary: "", Replicas: []string{"a", "b"}}, // primary missing → 2 holders
			4: {Primary: "a", Replicas: []string{"a"}},     // dup is not a 2nd holder
		},
	}

	cases := []struct {
		rf   int
		want []int
	}{
		{rf: 1, want: []int{}},        // every shard has at least 1
		{rf: 2, want: []int{1, 4}},    // shard 1 has only primary; shard 4's "replica" is the primary
		{rf: 3, want: []int{0, 1, 3, 4}}, // shard 2 is the only one with 3 holders
	}
	for _, tc := range cases {
		got := sm.UnderReplicatedShards(tc.rf)
		if got == nil {
			got = []int{}
		}
		sort.Ints(got)
		sort.Ints(tc.want)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("rf=%d: got %v, want %v", tc.rf, got, tc.want)
		}
	}
}

func TestShardAssignment_LegacyJSONShape(t *testing.T) {
	// Old snapshots used `replica` (singular). New code must read them
	// transparently so an in-place upgrade doesn't lose placement state.
	const legacy = `{"primary":"node-1","replica":"node-2"}`
	var a ShardAssignment
	if err := json.Unmarshal([]byte(legacy), &a); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if a.Primary != "node-1" || len(a.Replicas) != 1 || a.Replicas[0] != "node-2" {
		t.Fatalf("legacy fold-in failed: %+v", a)
	}

	// New shape wins when both fields present.
	const mixed = `{"primary":"x","replica":"old","replicas":["new"]}`
	var b ShardAssignment
	if err := json.Unmarshal([]byte(mixed), &b); err != nil {
		t.Fatalf("unmarshal mixed: %v", err)
	}
	if len(b.Replicas) != 1 || b.Replicas[0] != "new" {
		t.Fatalf("new-shape should win: %+v", b)
	}

	// Empty replica means no replicas, not a one-element [""] slice.
	const primaryOnly = `{"primary":"only"}`
	var c ShardAssignment
	if err := json.Unmarshal([]byte(primaryOnly), &c); err != nil {
		t.Fatal(err)
	}
	if len(c.Replicas) != 0 {
		t.Fatalf("primary-only should have no replicas: %+v", c)
	}
}

func TestAssignShardPayload_LegacyJSONShape(t *testing.T) {
	const legacy = `{"shard_id":7,"primary":"a","replica":"b"}`
	var p AssignShardPayload
	if err := json.Unmarshal([]byte(legacy), &p); err != nil {
		t.Fatal(err)
	}
	if p.ShardID != 7 || p.Primary != "a" || len(p.Replicas) != 1 || p.Replicas[0] != "b" {
		t.Fatalf("legacy payload fold-in failed: %+v", p)
	}
}

func TestFSM_AssignShardEpochIdempotency(t *testing.T) {
	fsm := NewFSM()

	// Apply two payloads at the same epoch — only the first should win.
	first, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "a", Epoch: 5})
	if r := applyCommand(fsm, Command{Type: CmdAssignShard, Payload: first}); r != nil {
		t.Fatalf("first apply: %v", r)
	}
	stale, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "stale", Epoch: 5})
	if r := applyCommand(fsm, Command{Type: CmdAssignShard, Payload: stale}); r != nil {
		t.Fatalf("stale apply errored: %v", r)
	}

	got := fsm.GetShardMap().Assignments[0]
	if got.Primary != "a" {
		t.Fatalf("equal-epoch update should not overwrite, got primary=%s", got.Primary)
	}

	// A strictly higher epoch wins.
	newer, _ := json.Marshal(AssignShardPayload{ShardID: 0, Primary: "newer", Epoch: 6})
	if r := applyCommand(fsm, Command{Type: CmdAssignShard, Payload: newer}); r != nil {
		t.Fatalf("newer apply: %v", r)
	}
	got = fsm.GetShardMap().Assignments[0]
	if got.Primary != "newer" || got.Epoch != 6 {
		t.Fatalf("higher epoch should win, got %+v", got)
	}
}

func TestFSM_LegacySnapshotRestore(t *testing.T) {
	// A pre-#5 snapshot has the old per-assignment shape (replica string,
	// no replicas/epoch). Restore must accept it and emerge with a
	// well-formed Replicas slice.
	const oldSnapshot = `{
		"shard_map": {
			"assignments": {
				"0": {"primary":"node-1","replica":"node-2"},
				"1": {"primary":"node-2","replica":"node-1"}
			},
			"nodes": {},
			"schema_keys": {}
		},
		"catalog": {"databases": {}}
	}`

	fsm := NewFSM()
	rc := nopCloser{r: []byte(oldSnapshot)}
	if err := fsm.Restore(&rc); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sm := fsm.GetShardMap()
	a := sm.Assignments[0]
	if a.Primary != "node-1" || len(a.Replicas) != 1 || a.Replicas[0] != "node-2" {
		t.Fatalf("legacy snapshot did not fold into Replicas: %+v", a)
	}
}

// nopCloser is a tiny io.ReadCloser over a byte slice for snapshot tests.
type nopCloser struct {
	r   []byte
	pos int
}

func (n *nopCloser) Read(p []byte) (int, error) {
	if n.pos >= len(n.r) {
		return 0, io.EOF
	}
	c := copy(p, n.r[n.pos:])
	n.pos += c
	return c, nil
}
func (n *nopCloser) Close() error { return nil }
