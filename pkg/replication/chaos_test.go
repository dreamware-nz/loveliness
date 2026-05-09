package replication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

// chaosReplica wraps an httptest server + the underlying MemoryStore so
// tests can both serve replication HTTP and inspect what writes the
// store actually saw.
type chaosReplica struct {
	srv   *httptest.Server
	store *shard.MemoryStore
	mgr   *shard.Manager
}

func newChaosReplica(t *testing.T, nodeID string) *chaosReplica {
	t.Helper()
	mgr := shard.NewTestManager(nodeID)
	mgr.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: nodeID},
	})
	s := mgr.GetShard(0)
	if s == nil {
		t.Fatalf("test setup: shard 0 not opened on %s", nodeID)
	}
	store, ok := s.Store.(*shard.MemoryStore)
	if !ok {
		t.Fatalf("test setup: expected MemoryStore on %s, got %T", nodeID, s.Store)
	}
	mux := http.NewServeMux()
	transport.NewHandler(mgr).RegisterRoutes(mux)
	return &chaosReplica{
		srv:   httptest.NewServer(mux),
		store: store,
		mgr:   mgr,
	}
}

func (c *chaosReplica) close() { c.srv.Close() }
func (c *chaosReplica) addr() string {
	return c.srv.Listener.Addr().String()
}

// countWrites is a stand-in for "the replica observed this mutation"
// — every CREATE … {tag: 'N'} we send leaves a substring on the store
// log, so we can verify the durability invariant by counting tags.
func (c *chaosReplica) countWrites(tagPrefix string) int {
	n := 0
	for _, q := range c.store.QueryLog() {
		if strings.Contains(q, tagPrefix) {
			n++
		}
	}
	return n
}

// TestChaos_ReplicaFailureMidStream_QuorumDurability validates the
// "no acked write is lost" guarantee from issue #7's acceptance
// criteria, scoped to the replication layer.
//
// With 2 replicas and Quorum (which needs ⌊RF/2⌋ = 1 replica ack):
//   - Replicate 3 writes; both replicas see all 3.
//   - Kill replica-2.
//   - Replicate 2 more writes; only replica-1 needs to ack and does.
//   - Assert replica-1 (the survivor) has all 5 writes.
//
// The promotion path itself is covered by FailureDetector + Rebalancer
// tests; this test pins down the durability invariant they rely on:
// every Replicate that returns "ok" leaves the data on a replica that
// would survive a primary kill.
func TestChaos_ReplicaFailureMidStream_QuorumDurability(t *testing.T) {
	r1 := newChaosReplica(t, "replica-1")
	defer r1.close()
	r2 := newChaosReplica(t, "replica-2")
	defer r2.close()

	client := transport.NewClient(2 * time.Second)
	client.SetPeer("replica-1", r1.addr())
	client.SetPeer("replica-2", r2.addr())

	repl := NewReplicator(client, 2*time.Second)
	owners := ShardOwnership{
		ShardID:  0,
		Primary:  "primary",
		Replicas: []string{"replica-1", "replica-2"},
	}

	const tag = "chaos-tag"
	writes := []string{
		"CREATE (n:Test {" + tag + ": '1'})",
		"CREATE (n:Test {" + tag + ": '2'})",
		"CREATE (n:Test {" + tag + ": '3'})",
	}
	for _, c := range writes {
		res := repl.Replicate(context.Background(), owners, c, ConsistencyQuorum)
		if !res.PrimaryOK {
			t.Fatalf("primary should be OK for %q", c)
		}
		if res.ReplicaACK < 1 {
			t.Fatalf("quorum needs ≥1 replica ack for %q, got %d (errs=%v)",
				c, res.ReplicaACK, res.ReplicaErr)
		}
	}

	r2.close()

	// Two more writes after replica-2 dies. Quorum=1 replica ack is
	// still satisfiable by replica-1 alone, so these must succeed.
	survived := []string{
		"CREATE (n:Test {" + tag + ": '4'})",
		"CREATE (n:Test {" + tag + ": '5'})",
	}
	for _, c := range survived {
		res := repl.Replicate(context.Background(), owners, c, ConsistencyQuorum)
		if !res.PrimaryOK {
			t.Fatalf("primary should be OK after replica-2 kill for %q", c)
		}
		if res.ReplicaACK < 1 {
			t.Fatalf("quorum still needs ≥1 ack after replica-2 kill, got %d (errs=%v)",
				res.ReplicaACK, res.ReplicaErr)
		}
	}

	if got := r1.countWrites(tag); got != 5 {
		t.Errorf("survivor replica-1 missing acked writes: got %d, want 5 (log=%v)",
			got, r1.store.QueryLog())
	}
}

// TestChaos_ConsistencyAllFailsOnReplicaKill confirms the contract for
// ConsistencyAll: if any replica is unreachable, the call is reported
// as a shortfall — we never silently lose durability for "all".
func TestChaos_ConsistencyAllFailsOnReplicaKill(t *testing.T) {
	r1 := newChaosReplica(t, "replica-1")
	defer r1.close()
	r2 := newChaosReplica(t, "replica-2")

	client := transport.NewClient(500 * time.Millisecond)
	client.SetPeer("replica-1", r1.addr())
	client.SetPeer("replica-2", r2.addr())

	repl := NewReplicator(client, 500*time.Millisecond)
	owners := ShardOwnership{
		ShardID:  0,
		Primary:  "primary",
		Replicas: []string{"replica-1", "replica-2"},
	}

	r2.close()

	res := repl.Replicate(context.Background(), owners,
		"CREATE (n:Test {chaos: 'all-strict'})", ConsistencyAll)
	if res.ReplicaACK == 2 {
		t.Errorf("ConsistencyAll must not report 2 acks when one replica is dead: %+v", res)
	}
	if len(res.ReplicaErr) == 0 {
		t.Errorf("ConsistencyAll must surface a replica error on kill, got none")
	}
}
