package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

// crossNode wires up everything a single node needs for the cross-node
// router test: a shard.Manager hosting one shard, an httptest server
// running the internal /internal/query handler, and the local-shard
// pointer the router slots into its `shards` array.
type crossNode struct {
	nodeID  string
	shardID int
	mgr     *shard.Manager
	srv     *httptest.Server
	local   *shard.Shard
	store   *shard.MemoryStore
}

func newCrossNode(t *testing.T, nodeID string, shardID int, seed map[string]map[string]any) *crossNode {
	t.Helper()
	mgr := shard.NewTestManager(nodeID)
	mgr.UpdateAssignments(map[int]shard.Assignment{
		shardID: {Primary: nodeID},
	})
	s := mgr.GetShard(shardID)
	if s == nil {
		t.Fatalf("setup: shard %d not opened on %s", shardID, nodeID)
	}
	store, ok := s.Store.(*shard.MemoryStore)
	if !ok {
		t.Fatalf("setup: expected MemoryStore on %s, got %T", nodeID, s.Store)
	}
	for k, props := range seed {
		store.PutNode(k, props)
	}
	mux := http.NewServeMux()
	transport.NewHandler(mgr).RegisterRoutes(mux)
	return &crossNode{
		nodeID:  nodeID,
		shardID: shardID,
		mgr:     mgr,
		srv:     httptest.NewServer(mux),
		local:   s,
		store:   store,
	}
}

func (n *crossNode) close() { n.srv.Close() }

// twoNodePlacement maps shard ID → owner node ID.
type twoNodePlacement struct {
	primary map[int]string
}

func (p *twoNodePlacement) PrimaryForShard(shardID int) string { return p.primary[shardID] }

// TestRouter_CrossNodeScatterGather is the integration test from
// issue #6's acceptance criteria: 2 nodes, shards split across them,
// a scatter-gather read returns merged results from both. The local
// shard hits the in-process path; the non-local shard goes over HTTP
// via transport.Client → transport.Handler on the peer.
func TestRouter_CrossNodeScatterGather(t *testing.T) {
	nodeA := newCrossNode(t, "node-A", 0, map[string]map[string]any{
		"alice": {"name": "Alice", "home_shard": int64(0)},
	})
	defer nodeA.close()
	nodeB := newCrossNode(t, "node-B", 1, map[string]map[string]any{
		"bob": {"name": "Bob", "home_shard": int64(1)},
	})
	defer nodeB.close()

	placement := &twoNodePlacement{primary: map[int]string{0: "node-A", 1: "node-B"}}

	// Build node A's router: shard 0 local, shard 1 remote.
	clientA := transport.NewClient(2 * time.Second)
	clientA.SetPeer("node-B", nodeB.srv.Listener.Addr().String())
	rA := NewRouter([]*shard.Shard{nodeA.local, nil}, 2*time.Second)
	rA.SetRemoteTransport("node-A", transport.NewRouterAdapter(clientA), placement)

	// Build node B's router: shard 1 local, shard 0 remote.
	clientB := transport.NewClient(2 * time.Second)
	clientB.SetPeer("node-A", nodeA.srv.Listener.Addr().String())
	rB := NewRouter([]*shard.Shard{nil, nodeB.local}, 2*time.Second)
	rB.SetRemoteTransport("node-B", transport.NewRouterAdapter(clientB), placement)

	// Scatter-gather read against node A. Should hit shard 0 locally
	// and shard 1 over HTTP to node B; merged result has both rows.
	res, err := rA.Execute(context.Background(), "MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatalf("scatter-gather from node-A: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("expected 2 merged rows from cross-node scatter, got %d (rows=%v)", len(res.Rows), res.Rows)
	}

	gotA := len(nodeA.store.QueryLog())
	gotB := len(nodeB.store.QueryLog())
	if gotA == 0 {
		t.Errorf("local shard on node-A was not queried")
	}
	if gotB == 0 {
		t.Errorf("remote shard on node-B was not queried via HTTP")
	}

	// Symmetric check from node B: should still see both nodes' data.
	res2, err := rB.Execute(context.Background(), "MATCH (p:Person) RETURN p")
	if err != nil {
		t.Fatalf("scatter-gather from node-B: %v", err)
	}
	if len(res2.Rows) != 2 {
		t.Errorf("expected 2 merged rows from node-B scatter, got %d", len(res2.Rows))
	}
}

// TestRouter_CrossNodeRemoteFailureSurfacesError pins the failure
// behavior: when a remote node is unreachable the scatter-gather
// returns a partial result with the failed shard recorded as an
// error rather than silently dropping rows.
func TestRouter_CrossNodeRemoteFailureSurfacesError(t *testing.T) {
	nodeA := newCrossNode(t, "node-A", 0, map[string]map[string]any{
		"alice": {"name": "Alice"},
	})
	defer nodeA.close()
	nodeB := newCrossNode(t, "node-B", 1, map[string]map[string]any{
		"bob": {"name": "Bob"},
	})
	// kill node-B before the query so node-A's remote dispatch fails.
	nodeB.close()

	placement := &twoNodePlacement{primary: map[int]string{0: "node-A", 1: "node-B"}}

	clientA := transport.NewClient(500 * time.Millisecond)
	clientA.SetPeer("node-B", nodeB.srv.Listener.Addr().String())
	rA := NewRouter([]*shard.Shard{nodeA.local, nil}, 2*time.Second)
	rA.SetRemoteTransport("node-A", transport.NewRouterAdapter(clientA), placement)

	res, _ := rA.Execute(context.Background(), "MATCH (p:Person) RETURN p")
	if res == nil {
		t.Fatal("expected partial result, got nil")
	}
	if !res.Partial {
		t.Errorf("expected Partial=true when remote node unreachable, got %+v", res)
	}
	foundShard1Error := false
	for _, e := range res.Errors {
		if e.ShardID == 1 {
			foundShard1Error = true
		}
	}
	if !foundShard1Error {
		t.Errorf("expected shard 1 error in partial result, got errors=%v", res.Errors)
	}
}
