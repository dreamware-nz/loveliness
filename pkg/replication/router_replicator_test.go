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

func TestRouterReplicator_NoReplicasIsNoop(t *testing.T) {
	rr := NewRouterReplicator(
		NewReplicator(transport.NewClient(time.Second), time.Second),
		ShardResolverFunc(func(shardID int) ShardOwnership {
			return ShardOwnership{ShardID: shardID, Primary: "primary-1"}
		}),
		ConsistencyQuorum,
	)
	if err := rr.ReplicateWrite(context.Background(), 0, "CREATE (n:Test)"); err != nil {
		t.Errorf("expected nil error with no replicas, got %v", err)
	}
}

func TestRouterReplicator_ConsistencyOneAlwaysSucceeds(t *testing.T) {
	// ConsistencyOne is fire-and-forget — even an unreachable replica must
	// not surface an error to the router.
	client := transport.NewClient(200 * time.Millisecond)
	client.SetPeer("dead-replica", "127.0.0.1:1") // nothing listening

	rr := NewRouterReplicator(
		NewReplicator(client, 200*time.Millisecond),
		ShardResolverFunc(func(shardID int) ShardOwnership {
			return ShardOwnership{ShardID: shardID, Primary: "primary-1", Replicas: []string{"dead-replica"}}
		}),
		ConsistencyOne,
	)
	if err := rr.ReplicateWrite(context.Background(), 0, "CREATE (n:Test)"); err != nil {
		t.Errorf("ConsistencyOne must not surface async errors, got %v", err)
	}
}

func TestRouterReplicator_QuorumSucceedsWithLiveReplica(t *testing.T) {
	srv := newReplicaTestServer(t, "replica-live")
	defer srv.Close()

	client := transport.NewClient(time.Second)
	client.SetPeer("replica-live", srv.Listener.Addr().String())

	rr := NewRouterReplicator(
		NewReplicator(client, time.Second),
		ShardResolverFunc(func(shardID int) ShardOwnership {
			return ShardOwnership{ShardID: shardID, Primary: "primary-1", Replicas: []string{"replica-live"}}
		}),
		ConsistencyQuorum,
	)
	if err := rr.ReplicateWrite(context.Background(), 0, "CREATE (n:Test)"); err != nil {
		t.Errorf("quorum should succeed with one live replica, got %v", err)
	}
}

func TestRouterReplicator_QuorumFailsWhenAllReplicasDown(t *testing.T) {
	client := transport.NewClient(200 * time.Millisecond)
	client.SetPeer("dead-replica", "127.0.0.1:1")

	rr := NewRouterReplicator(
		NewReplicator(client, 200*time.Millisecond),
		ShardResolverFunc(func(shardID int) ShardOwnership {
			return ShardOwnership{ShardID: shardID, Primary: "primary-1", Replicas: []string{"dead-replica"}}
		}),
		ConsistencyQuorum,
	)
	err := rr.ReplicateWrite(context.Background(), 0, "CREATE (n:Test)")
	if err == nil {
		t.Fatal("expected quorum failure when only replica is down")
	}
	if !strings.Contains(err.Error(), "consistency=QUORUM") {
		t.Errorf("error should describe consistency level; got %v", err)
	}
}

func TestRouterReplicator_AllRequiresEveryReplica(t *testing.T) {
	srv := newReplicaTestServer(t, "replica-up")
	defer srv.Close()

	client := transport.NewClient(200 * time.Millisecond)
	client.SetPeer("replica-up", srv.Listener.Addr().String())
	client.SetPeer("replica-down", "127.0.0.1:1")

	rr := NewRouterReplicator(
		NewReplicator(client, 300*time.Millisecond),
		ShardResolverFunc(func(shardID int) ShardOwnership {
			return ShardOwnership{
				ShardID:  shardID,
				Primary:  "primary-1",
				Replicas: []string{"replica-up", "replica-down"},
			}
		}),
		ConsistencyAll,
	)
	err := rr.ReplicateWrite(context.Background(), 0, "CREATE (n:Test)")
	if err == nil {
		t.Fatal("ConsistencyAll must fail when any replica is unreachable")
	}
}

func newReplicaTestServer(t *testing.T, nodeID string) *httptest.Server {
	t.Helper()
	m := shard.NewTestManager(nodeID)
	m.UpdateAssignments(map[int]shard.Assignment{0: {Primary: nodeID}})
	h := transport.NewHandler(m)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return httptest.NewServer(mux)
}
