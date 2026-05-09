package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeFDCluster is a minimal failureDetectorCluster used to drive Tick in
// isolation from a real Raft FSM.
type fakeFDCluster struct {
	mu         sync.Mutex
	leader     bool
	nodeID     string
	shardMap   ShardMap
	markedDown []string
}

func (f *fakeFDCluster) IsLeader() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.leader }
func (f *fakeFDCluster) NodeID() string { return f.nodeID }
func (f *fakeFDCluster) GetShardMap() ShardMap {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shardMap
}
func (f *fakeFDCluster) MarkNodeDown(nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markedDown = append(f.markedDown, nodeID)
	if info, ok := f.shardMap.Nodes[nodeID]; ok {
		info.Alive = false
		f.shardMap.Nodes[nodeID] = info
	}
	return nil
}

// fakePinger returns canned ping results keyed by nodeID.
type fakePinger struct {
	mu      sync.Mutex
	results map[string]bool
	calls   []string
}

func (f *fakePinger) Ping(_ context.Context, nodeID, _ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, nodeID)
	if r, ok := f.results[nodeID]; ok {
		return r
	}
	return true
}

// fakeRebalancer records Plan + Execute calls for assertions.
type fakeRebalancer struct {
	mu       sync.Mutex
	moves    []Move
	executed [][]Move
}

func (f *fakeRebalancer) Plan() []Move {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Move(nil), f.moves...)
}
func (f *fakeRebalancer) Execute(moves []Move) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.executed = append(f.executed, append([]Move(nil), moves...))
	return nil
}

func TestFailureDetector_NonLeaderNoops(t *testing.T) {
	c := &fakeFDCluster{
		leader: false,
		shardMap: ShardMap{Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Alive: true, HTTPAddr: "x"},
		}},
	}
	pinger := &fakePinger{}
	rb := &fakeRebalancer{moves: []Move{{ShardID: 0, Role: "primary"}}}
	fd := newFailureDetector(c, rb, pinger, time.Second, 3)

	fd.Tick(context.Background())

	if len(pinger.calls) != 0 {
		t.Errorf("non-leader must not ping; got %d calls", len(pinger.calls))
	}
	if len(rb.executed) != 0 {
		t.Errorf("non-leader must not rebalance; got %d executes", len(rb.executed))
	}
}

func TestFailureDetector_PingsOnlyOtherAliveNodes(t *testing.T) {
	c := &fakeFDCluster{
		leader: true,
		nodeID: "node-1",
		shardMap: ShardMap{Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Alive: true, HTTPAddr: "self"},
			"node-2": {ID: "node-2", Alive: true, HTTPAddr: "addr-2"},
			"node-3": {ID: "node-3", Alive: false, HTTPAddr: "addr-3"}, // already dead
		}},
	}
	pinger := &fakePinger{}
	fd := newFailureDetector(c, &fakeRebalancer{}, pinger, time.Second, 3)

	fd.Tick(context.Background())

	if len(pinger.calls) != 1 || pinger.calls[0] != "node-2" {
		t.Errorf("expected single ping to node-2, got %v", pinger.calls)
	}
}

func TestFailureDetector_MarksDownAfterThreshold(t *testing.T) {
	c := &fakeFDCluster{
		leader: true,
		nodeID: "node-1",
		shardMap: ShardMap{Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Alive: true},
			"node-2": {ID: "node-2", Alive: true, HTTPAddr: "addr-2"},
		}},
	}
	pinger := &fakePinger{results: map[string]bool{"node-2": false}}
	fd := newFailureDetector(c, &fakeRebalancer{}, pinger, time.Second, 3)

	// First two failures should not flip the flag.
	fd.Tick(context.Background())
	fd.Tick(context.Background())
	if len(c.markedDown) != 0 {
		t.Errorf("must not mark down before threshold; got %v", c.markedDown)
	}
	// Third failure crosses the threshold.
	fd.Tick(context.Background())
	if len(c.markedDown) != 1 || c.markedDown[0] != "node-2" {
		t.Errorf("expected node-2 marked down once, got %v", c.markedDown)
	}
}

func TestFailureDetector_FailureCounterResetsOnSuccess(t *testing.T) {
	c := &fakeFDCluster{
		leader: true,
		nodeID: "node-1",
		shardMap: ShardMap{Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Alive: true},
			"node-2": {ID: "node-2", Alive: true, HTTPAddr: "addr-2"},
		}},
	}
	pinger := &fakePinger{results: map[string]bool{"node-2": false}}
	fd := newFailureDetector(c, &fakeRebalancer{}, pinger, time.Second, 3)

	// Two failures.
	fd.Tick(context.Background())
	fd.Tick(context.Background())

	// Recover.
	pinger.mu.Lock()
	pinger.results["node-2"] = true
	pinger.mu.Unlock()
	fd.Tick(context.Background())

	// Two more failures should not yet trip the threshold (counter reset).
	pinger.mu.Lock()
	pinger.results["node-2"] = false
	pinger.mu.Unlock()
	fd.Tick(context.Background())
	fd.Tick(context.Background())
	if len(c.markedDown) != 0 {
		t.Errorf("counter must reset on success; got premature mark-downs: %v", c.markedDown)
	}
}

func TestFailureDetector_ExecutesRebalancePlan(t *testing.T) {
	c := &fakeFDCluster{
		leader: true,
		nodeID: "node-1",
		shardMap: ShardMap{Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Alive: true},
			"node-2": {ID: "node-2", Alive: true, HTTPAddr: "addr-2"},
		}},
	}
	planned := []Move{{ShardID: 0, Role: "primary", FromNode: "dead", ToNode: "node-1"}}
	rb := &fakeRebalancer{moves: planned}
	fd := newFailureDetector(c, rb, &fakePinger{}, time.Second, 3)

	fd.Tick(context.Background())

	if len(rb.executed) != 1 || len(rb.executed[0]) != 1 || rb.executed[0][0].ShardID != 0 {
		t.Errorf("expected one Execute with the planned moves; got %v", rb.executed)
	}
}

func TestHTTPPinger_LiveServerIsAlive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewHTTPPinger(time.Second)
	addr := srv.Listener.Addr().String()
	if !p.Ping(context.Background(), "node-x", addr) {
		t.Error("expected live server to ping alive")
	}
}

func TestHTTPPinger_DeadServerIsDown(t *testing.T) {
	p := NewHTTPPinger(200 * time.Millisecond)
	if p.Ping(context.Background(), "node-x", "127.0.0.1:1") {
		t.Error("expected dead server to ping down")
	}
}

func TestHTTPPinger_5xxIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewHTTPPinger(time.Second)
	addr := srv.Listener.Addr().String()
	if p.Ping(context.Background(), "node-x", addr) {
		t.Error("5xx must be treated as down")
	}
}
