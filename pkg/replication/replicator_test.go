package replication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

func setupReplicaServer(nodeID string) (*httptest.Server, *shard.Manager) {
	m := shard.NewTestManager(nodeID)
	m.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: nodeID},
	})
	h := transport.NewHandler(m)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return httptest.NewServer(mux), m
}

func TestReplicator_ConsistencyOne_NoWait(t *testing.T) {
	srv, _ := setupReplicaServer("replica-1")
	defer srv.Close()

	client := transport.NewClient(5 * time.Second)
	client.SetPeer("replica-1", srv.Listener.Addr().String())

	r := NewReplicator(client, 5*time.Second)

	result := r.Replicate(context.Background(), ShardOwnership{
		ShardID:  0,
		Primary:  "primary-1",
		Replicas: []string{"replica-1"},
	}, "CREATE (n:Test {name: 'Alice'})", ConsistencyOne)

	if !result.PrimaryOK {
		t.Error("primary should be OK")
	}
	// ONE doesn't wait for replicas.
}

func TestReplicator_ConsistencyQuorum(t *testing.T) {
	srv, _ := setupReplicaServer("replica-1")
	defer srv.Close()

	client := transport.NewClient(5 * time.Second)
	client.SetPeer("replica-1", srv.Listener.Addr().String())

	r := NewReplicator(client, 5*time.Second)

	result := r.Replicate(context.Background(), ShardOwnership{
		ShardID:  0,
		Primary:  "primary-1",
		Replicas: []string{"replica-1"},
	}, "CREATE (n:Test {name: 'Bob'})", ConsistencyQuorum)

	if !result.PrimaryOK {
		t.Error("primary should be OK")
	}
	if result.ReplicaACK < 1 {
		t.Errorf("expected at least 1 replica ACK, got %d, errors: %v", result.ReplicaACK, result.ReplicaErr)
	}
}

func TestReplicator_ConsistencyAll(t *testing.T) {
	srv1, _ := setupReplicaServer("replica-1")
	defer srv1.Close()
	srv2, _ := setupReplicaServer("replica-2")
	defer srv2.Close()

	client := transport.NewClient(5 * time.Second)
	client.SetPeer("replica-1", srv1.Listener.Addr().String())
	client.SetPeer("replica-2", srv2.Listener.Addr().String())

	r := NewReplicator(client, 5*time.Second)

	result := r.Replicate(context.Background(), ShardOwnership{
		ShardID:  0,
		Primary:  "primary-1",
		Replicas: []string{"replica-1", "replica-2"},
	}, "CREATE (n:Test {name: 'Charlie'})", ConsistencyAll)

	if result.ReplicaACK != 2 {
		t.Errorf("expected 2 replica ACKs, got %d, errors: %v", result.ReplicaACK, result.ReplicaErr)
	}
}

func TestReplicator_ReplicaDown(t *testing.T) {
	client := transport.NewClient(1 * time.Second)
	client.SetPeer("replica-1", "127.0.0.1:1") // nothing listening

	r := NewReplicator(client, 2*time.Second)

	result := r.Replicate(context.Background(), ShardOwnership{
		ShardID:  0,
		Primary:  "primary-1",
		Replicas: []string{"replica-1"},
	}, "CREATE (n:Test {name: 'Dave'})", ConsistencyQuorum)

	if result.ReplicaACK != 0 {
		t.Errorf("expected 0 ACKs with replica down, got %d", result.ReplicaACK)
	}
	if len(result.ReplicaErr) == 0 {
		t.Error("expected errors with replica down")
	}
}

func TestReplicator_NoReplicas(t *testing.T) {
	client := transport.NewClient(5 * time.Second)
	r := NewReplicator(client, 5*time.Second)

	result := r.Replicate(context.Background(), ShardOwnership{
		ShardID: 0,
		Primary: "primary-1",
	}, "CREATE (n:Test {name: 'Eve'})", ConsistencyQuorum)

	if !result.PrimaryOK {
		t.Error("primary should be OK even with no replicas")
	}
}

func TestParseConsistency(t *testing.T) {
	tests := map[string]Consistency{
		"one":     ConsistencyOne,
		"ONE":     ConsistencyOne,
		"1":       ConsistencyOne,
		"quorum":  ConsistencyQuorum,
		"QUORUM":  ConsistencyQuorum,
		"all":     ConsistencyAll,
		"ALL":     ConsistencyAll,
		"invalid": ConsistencyQuorum, // default
		"":        ConsistencyQuorum,
	}
	for input, want := range tests {
		got := ParseConsistency(input)
		if got != want {
			t.Errorf("ParseConsistency(%q) = %v, want %v", input, got, want)
		}
	}
}
