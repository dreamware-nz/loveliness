package router

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

// blockingRemote is a RemoteQuerier that records peak concurrency. It
// blocks every call on a barrier so we can observe how many calls are
// in flight simultaneously — that's how we prove the scatter cap is
// being honored. Without the cap, peak concurrency equals shardCount;
// with a cap of N, it must be ≤ N.
type blockingRemote struct {
	mu      sync.Mutex
	inFlight int
	peak    int32
	gate    chan struct{}
}

func newBlockingRemote() *blockingRemote {
	return &blockingRemote{gate: make(chan struct{})}
}

func (b *blockingRemote) QueryRemoteShard(_ string, shardID int, _ string) (*shard.QueryResponse, error) {
	b.mu.Lock()
	b.inFlight++
	if int32(b.inFlight) > atomic.LoadInt32(&b.peak) {
		atomic.StoreInt32(&b.peak, int32(b.inFlight))
	}
	b.mu.Unlock()

	<-b.gate

	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
	return &shard.QueryResponse{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": shardID}},
	}, nil
}

func (b *blockingRemote) Peak() int { return int(atomic.LoadInt32(&b.peak)) }
func (b *blockingRemote) Release()  { close(b.gate) }

type allRemotePlacement struct{}

func (allRemotePlacement) PrimaryForShard(_ int) string { return "remote" }

// TestScatterConcurrency_RespectsCap is the headline assertion: when
// the scatter cap is set, no more than that many remote calls are
// in flight at once. Without the cap (cap == 0), all shards dispatch
// in parallel.
func TestScatterConcurrency_RespectsCap(t *testing.T) {
	const shardCount = 32
	const cap = 4

	// All shards nil → router treats every shard as remote.
	shards := make([]*shard.Shard, shardCount)
	r := NewRouter(shards, 5*time.Second)

	br := newBlockingRemote()
	r.SetRemoteTransport("local", br, allRemotePlacement{})
	r.SetScatterConcurrency(cap)

	// Release the gate after a beat so all in-flight goroutines have a
	// chance to pile up against the semaphore. 50ms is comfortably
	// longer than goroutine spin-up and short enough not to slow CI.
	go func() {
		time.Sleep(50 * time.Millisecond)
		br.Release()
	}()

	res, err := r.Execute(context.Background(), "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res == nil {
		t.Fatal("Execute returned nil result")
	}

	peak := br.Peak()
	if peak == 0 {
		t.Fatalf("expected at least one in-flight call, got 0")
	}
	if peak > cap {
		t.Errorf("scatter exceeded cap: peak=%d, cap=%d", peak, cap)
	}
}

// TestScatterConcurrency_ZeroDisablesCap confirms backwards compat:
// without an explicit cap or remote transport, scatterGather still
// fans out to every shard concurrently.
func TestScatterConcurrency_ZeroDisablesCap(t *testing.T) {
	const shardCount = 16
	shards := make([]*shard.Shard, shardCount)
	r := NewRouter(shards, 5*time.Second)

	br := newBlockingRemote()
	// SetRemoteTransport would auto-set the default cap; bypass it by
	// assigning the fields via the public setters in a way that keeps
	// the cap at 0.
	r.SetRemoteTransport("local", br, allRemotePlacement{})
	r.SetScatterConcurrency(0) // explicit override back to unbounded

	go func() {
		time.Sleep(50 * time.Millisecond)
		br.Release()
	}()
	if _, err := r.Execute(context.Background(), "MATCH (n) RETURN n"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if br.Peak() < shardCount {
		t.Errorf("expected unbounded fan-out (peak=%d) to reach shardCount=%d", br.Peak(), shardCount)
	}
}

// TestSetRemoteTransport_DefaultsConcurrency pins the wiring side
// effect: the default cap is installed automatically when remote
// transport is configured, so production callers don't have to
// remember to set it.
func TestSetRemoteTransport_DefaultsConcurrency(t *testing.T) {
	r := NewRouter(make([]*shard.Shard, 16), time.Second)
	r.SetRemoteTransport("local", newBlockingRemote(), allRemotePlacement{})
	got := r.scatterConcurrency
	want := defaultScatterConcurrency(16)
	if got != want {
		t.Errorf("scatterConcurrency after SetRemoteTransport = %d, want %d", got, want)
	}
}

func TestDefaultScatterConcurrency_Floor(t *testing.T) {
	// max(8, 2*shardCount) — verify the floor at 8 for tiny shard counts.
	for _, tc := range []struct {
		shards int
		want   int
	}{
		{shards: 1, want: 8},
		{shards: 3, want: 8},
		{shards: 4, want: 8},
		{shards: 5, want: 10},
		{shards: 16, want: 32},
		{shards: 64, want: 128},
	} {
		if got := defaultScatterConcurrency(tc.shards); got != tc.want {
			t.Errorf("defaultScatterConcurrency(%d) = %d, want %d", tc.shards, got, tc.want)
		}
	}
}
