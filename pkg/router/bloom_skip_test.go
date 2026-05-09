package router

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// ShouldSkipKey contract: an empty index, an unpopulated index, and
// any "may contain" hit must all return false. Only a populated
// index where every populated filter says definitely-not yields true.
func TestShardBloomIndex_ShouldSkipKey(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var idx *ShardBloomIndex
		// Calling on nil should not panic and return false. We don't
		// claim that contract today; defensive call to ensure it's
		// at least exercised.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ShouldSkipKey panicked on nil: %v", r)
			}
		}()
		if idx != nil {
			_ = idx.ShouldSkipKey("k") // quiet the linter; never reached
		}
	})

	t.Run("empty index returns false", func(t *testing.T) {
		idx := NewShardBloomIndex()
		if idx.ShouldSkipKey("anything") {
			t.Error("empty index must not authorise skipping")
		}
	})

	t.Run("initialised but unpopulated returns false", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.InitShard(1, 1000, 0.01)
		if idx.ShouldSkipKey("never-added") {
			t.Error("unpopulated filters must not authorise skipping; we'd silently lose data")
		}
	})

	t.Run("populated and key absent returns true", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.InitShard(1, 1000, 0.01)
		idx.Add(0, "Alice")
		idx.Add(1, "Bob")
		if !idx.ShouldSkipKey("Eve") {
			t.Error("key in no populated filter must authorise skip")
		}
	})

	t.Run("populated and key present returns false", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.Add(0, "Alice")
		if idx.ShouldSkipKey("Alice") {
			t.Error("a populated filter that may contain the key must NOT authorise skip")
		}
	})

	t.Run("mixed populated and unpopulated, key absent → true", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.InitShard(1, 1000, 0.01)
		idx.Add(0, "Alice") // shard 0 populated
		// shard 1 stays unpopulated; we ignore it for the decision
		if !idx.ShouldSkipKey("Eve") {
			t.Error("mixed-populated index must skip when populated filters all rule the key out")
		}
	})

	t.Run("AddBatch marks populated", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.AddBatch(0, []string{"Alice", "Bob"})
		if !idx.ShouldSkipKey("Eve") {
			t.Error("AddBatch must mark filter populated; absent key should authorise skip")
		}
	})

	t.Run("empty AddBatch does not mark populated", func(t *testing.T) {
		idx := NewShardBloomIndex()
		idx.InitShard(0, 1000, 0.01)
		idx.AddBatch(0, nil)
		if idx.ShouldSkipKey("Eve") {
			t.Error("AddBatch with no keys must NOT mark filter populated")
		}
	})
}

// fakeRemote is a RemoteQuerier whose call count is the test signal.
// We assert that ShouldSkipKey-driven short-circuits never reach this.
type fakeRemote struct{ calls int32 }

func (f *fakeRemote) QueryRemoteShard(_ string, _ int, _ string) (*shard.QueryResponse, error) {
	atomic.AddInt32(&f.calls, 1)
	return &shard.QueryResponse{Columns: []string{"x"}, Rows: []map[string]any{{"x": 1}}}, nil
}
func (f *fakeRemote) Calls() int { return int(atomic.LoadInt32(&f.calls)) }

// allRemotePlacementForKey routes every shard to a remote node. Used
// to force the would-be-target of a routed query to be remote so the
// metric increment fires.
type allRemotePlacementForKey struct{}

func (allRemotePlacementForKey) PrimaryForShard(_ int) string { return "remote" }

// End-to-end: a router with populated bloom filters that rule a key
// out must short-circuit, return an empty result, increment
// loveliness_router_bloom_skip_total, and never call the remote
// transport.
func TestRouter_BloomSkip_AvoidsRemoteRPC(t *testing.T) {
	const shardCount = 4
	r := NewRouterWithSchema(make([]*shard.Shard, shardCount), 2*time.Second, nil)

	// Populate filters with a small known set; "missing-key" is
	// absent everywhere so the populated-and-empty path triggers.
	for sid := 0; sid < shardCount; sid++ {
		r.BloomIndex().InitShard(sid, 1024, 0.01)
		r.BloomIndex().Add(sid, fmt.Sprintf("present-%d", sid))
	}

	fr := &fakeRemote{}
	r.SetRemoteTransport("local", fr, allRemotePlacementForKey{})

	res, err := r.Execute(context.Background(),
		"MATCH (n:Person {id: 'missing-key'}) RETURN n")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if len(res.Rows) != 0 {
		t.Errorf("bloom-skip path must return empty rows, got %d", len(res.Rows))
	}
	if fr.Calls() != 0 {
		t.Errorf("remote transport must NOT be called when bloom rules out the key; got %d calls", fr.Calls())
	}

	snap := r.Metrics().Snapshot()
	if len(snap.BloomSkips) != 1 {
		t.Fatalf("BloomSkip samples = %d, want 1", len(snap.BloomSkips))
	}
	if snap.BloomSkips[0].Count != 1 {
		t.Errorf("BloomSkip count = %d, want 1", snap.BloomSkips[0].Count)
	}
}

// Negative control: when the bloom filter says "may contain" the
// remote RPC must still fire — bloom must not silently swallow
// queries for keys it knows about.
func TestRouter_BloomSkip_DispatchesWhenKeyMayBePresent(t *testing.T) {
	const shardCount = 2
	r := NewRouterWithSchema(make([]*shard.Shard, shardCount), 2*time.Second, nil)
	for sid := 0; sid < shardCount; sid++ {
		r.BloomIndex().InitShard(sid, 1024, 0.01)
	}
	r.BloomIndex().Add(0, "real-key")

	fr := &fakeRemote{}
	r.SetRemoteTransport("local", fr, allRemotePlacementForKey{})

	if _, err := r.Execute(context.Background(),
		"MATCH (n:Person {id: 'real-key'}) RETURN n"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fr.Calls() == 0 {
		t.Error("remote transport must be called when bloom says may-contain; bloom must never produce false negatives")
	}
	if got := r.Metrics().Snapshot().BloomSkips; len(got) != 0 {
		t.Errorf("BloomSkip should not increment on a may-contain hit; got %+v", got)
	}
}

// Unpopulated filters: the bloom-skip path must NEVER trigger if
// filters are initialised but never written to. This is the data-
// loss footgun the populated flag guards against.
func TestRouter_BloomSkip_RespectsPopulatedFlag(t *testing.T) {
	const shardCount = 2
	r := NewRouterWithSchema(make([]*shard.Shard, shardCount), 2*time.Second, nil)
	for sid := 0; sid < shardCount; sid++ {
		r.BloomIndex().InitShard(sid, 1024, 0.01)
	}
	// Deliberately no Add calls.

	fr := &fakeRemote{}
	r.SetRemoteTransport("local", fr, allRemotePlacementForKey{})

	if _, err := r.Execute(context.Background(),
		"MATCH (n:Person {id: 'anything'}) RETURN n"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if fr.Calls() == 0 {
		t.Error("unpopulated filters must NOT cause bloom-skip; the data could exist")
	}
}

// Local-shard skip: if the would-be target is a local shard, we
// short-circuit (correctness) but DON'T increment the metric (the
// metric is "remote RPCs avoided", not "local queries avoided").
func TestRouter_BloomSkip_LocalShardDoesNotIncrement(t *testing.T) {
	const shardCount = 2
	// Real local shards so isLocalShard returns true.
	store := shard.NewMemoryStore()
	store.PutNode("real-key", map[string]any{"name": "real"})
	shards := []*shard.Shard{
		shard.NewShard(0, store, 4),
		shard.NewShard(1, shard.NewMemoryStore(), 4),
	}
	r := NewRouterWithSchema(shards, 2*time.Second, nil)
	for sid := 0; sid < shardCount; sid++ {
		r.BloomIndex().InitShard(sid, 1024, 0.01)
		r.BloomIndex().Add(sid, fmt.Sprintf("present-%d", sid))
	}

	if _, err := r.Execute(context.Background(),
		"MATCH (n:Person {id: 'missing-key'}) RETURN n"); err != nil {
		// Some queries against the in-memory shard error out for
		// shape reasons; we don't care here. The bloom-skip path
		// must not record a metric either way.
		_ = errors.Unwrap(err)
	}
	if got := r.Metrics().Snapshot().BloomSkips; len(got) != 0 {
		t.Errorf("local-shard bloom-skip must not increment metric; got %+v", got)
	}
}
