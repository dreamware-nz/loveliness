package cluster

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Pinger reports whether a peer is alive. Decoupled from the failure
// detector so unit tests can drive it with deterministic results without
// real HTTP round-trips.
type Pinger interface {
	Ping(ctx context.Context, nodeID, httpAddr string) bool
}

// HTTPPinger issues GET <addr>/health and treats any 2xx response as alive.
type HTTPPinger struct {
	Client  *http.Client
	Timeout time.Duration
}

// NewHTTPPinger returns a pinger with sensible defaults.
func NewHTTPPinger(timeout time.Duration) *HTTPPinger {
	return &HTTPPinger{
		Client:  &http.Client{Timeout: timeout},
		Timeout: timeout,
	}
}

// Ping issues a GET /health and returns true on a 2xx response. Any
// network failure, non-2xx status, or timeout is treated as down.
func (p *HTTPPinger) Ping(ctx context.Context, nodeID, httpAddr string) bool {
	if httpAddr == "" {
		return false
	}
	url := "http://" + httpAddr + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// rebalancerOps is the surface FailureDetector uses from the rebalancer.
// It's an interface so tests can plug in a fake.
type rebalancerOps interface {
	Plan() []Move
	Execute(moves []Move) error
}

// failureDetectorCluster is the surface FailureDetector uses from the
// cluster. Interface for testability.
type failureDetectorCluster interface {
	IsLeader() bool
	NodeID() string
	GetShardMap() ShardMap
	MarkNodeDown(nodeID string) error
}

// FailureDetector runs on the leader, pings every other alive peer in the
// shard map, and marks unresponsive peers as down after `Threshold`
// consecutive failures. After each tick it asks the rebalancer for moves
// and executes them — that's where dead-primary auto-promotion happens
// (see Rebalancer.planMoves Phase 1).
//
// Non-leaders no-op; if leadership flips mid-loop the next tick will
// notice and resume work on the new leader.
type FailureDetector struct {
	cluster    failureDetectorCluster
	rebalancer rebalancerOps
	pinger     Pinger
	interval   time.Duration
	threshold  int

	mu       sync.Mutex
	failures map[string]int
}

// NewFailureDetector wires the failure detector to a live cluster + rebalancer.
// `interval` is the tick period; `threshold` is how many consecutive failed
// pings flip a node to Alive=false. Sensible defaults: interval=2s, threshold=3.
func NewFailureDetector(c *Cluster, interval time.Duration, threshold int) *FailureDetector {
	return newFailureDetector(c, NewRebalancer(c), NewHTTPPinger(interval), interval, threshold)
}

// newFailureDetector is the test-friendly constructor.
func newFailureDetector(c failureDetectorCluster, rb rebalancerOps, p Pinger, interval time.Duration, threshold int) *FailureDetector {
	return &FailureDetector{
		cluster:    c,
		rebalancer: rb,
		pinger:     p,
		interval:   interval,
		threshold:  threshold,
		failures:   map[string]int{},
	}
}

// Run loops on the configured interval until ctx is cancelled. Spawn it
// once per process; it's leader-gated internally so multiple nodes with
// detectors running is safe.
func (fd *FailureDetector) Run(ctx context.Context) {
	ticker := time.NewTicker(fd.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fd.Tick(ctx)
		}
	}
}

// Tick performs one round of pings + rebalance. Exported for tests.
func (fd *FailureDetector) Tick(ctx context.Context) {
	if !fd.cluster.IsLeader() {
		return
	}
	selfID := fd.cluster.NodeID()
	sm := fd.cluster.GetShardMap()
	for id, info := range sm.Nodes {
		if id == selfID || !info.Alive {
			continue
		}
		alive := fd.pinger.Ping(ctx, id, info.HTTPAddr)
		fd.recordResult(id, alive)
	}
	moves := fd.rebalancer.Plan()
	if len(moves) == 0 {
		return
	}
	if err := fd.rebalancer.Execute(moves); err != nil {
		slog.Warn("failure detector: rebalance execute failed", "err", err, "moves", len(moves))
		return
	}
	slog.Info("failure detector: rebalanced after liveness change", "moves", len(moves))
}

// recordResult updates the failure counter for a peer and marks the
// peer down once threshold is reached. Exported via Tick only.
func (fd *FailureDetector) recordResult(nodeID string, alive bool) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	if alive {
		delete(fd.failures, nodeID)
		return
	}
	fd.failures[nodeID]++
	if fd.failures[nodeID] < fd.threshold {
		return
	}
	delete(fd.failures, nodeID)
	if err := fd.cluster.MarkNodeDown(nodeID); err != nil {
		slog.Warn("failure detector: mark down failed", "node", nodeID, "err", err)
		return
	}
	slog.Warn("failure detector: marked node down", "node", nodeID, "threshold", fd.threshold)
}
