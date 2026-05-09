package transport_test

// Cross-node scatter-gather benchmarks. These exercise the full
// router → adapter → transport → remote-shard path, so a regression
// in any of those layers shows up here. The pkg/transport benches
// (loopback_test.go) only measure raw RPC throughput; this file
// measures end-to-end router latency including bounded scatter
// concurrency, retry, bloom-skip, and result merge.
//
// External test package so we can import pkg/router cleanly. The
// router does not import pkg/transport, so there is no real cycle.
//
// Topology: N "remote" nodes, each running its own TCP server with
// its own shard.Manager. The router under test has zero local
// shards — every shard ID dispatches as a remote RPC. This isolates
// the network-bounded scatter path that production cross-node
// queries actually take.
//
// Compared to a real WAN deployment we still pay only loopback
// latency, so the absolute numbers are best treated as floors. The
// scaling shape (concurrency cap vs shard count, scatter cost vs
// rows/shard) is the signal these benchmarks are designed to catch
// regressions in.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/transport"
)

// crossNodePlacement maps shard IDs to remote node names by integer
// division. With nodesCount=2, shardsPerNode=4 the layout is:
//
//	shard 0..3 → node-0
//	shard 4..7 → node-1
//
// shardsPerNode must be > 0; the constructor enforces it.
type crossNodePlacement struct {
	shardsPerNode int
	nodes         []string
}

func (p *crossNodePlacement) PrimaryForShard(shardID int) string {
	if shardID < 0 || p.shardsPerNode <= 0 {
		return ""
	}
	idx := shardID / p.shardsPerNode
	if idx >= len(p.nodes) {
		return ""
	}
	return p.nodes[idx]
}

// crossNodeRemote is a TCPServer + its Manager, kept together so
// the cleanup function can reach into both.
type crossNodeRemote struct {
	nodeID  string
	manager *shard.Manager
	server  *transport.TCPServer
}

// crossNodeEnv is the assembled test stand: one router (zero local
// shards) wired through a Client/Adapter to N TCP-served remote
// nodes. cleanup tears the whole stack down so benchmarks can defer
// it.
type crossNodeEnv struct {
	router  *router.Router
	client  *transport.Client
	remotes []*crossNodeRemote
	cleanup func()
}

// setupCrossNodeEnv builds the topology and starts every server.
// All remote transport runs over the TCP+msgpack fast path — the
// existing loopback_test benchmarks already cover HTTP+JSON in
// isolation. rowsPerShard controls the dataset size on each shard
// so we can compare empty/light/heavy scatter results.
func setupCrossNodeEnv(b *testing.B, nodesCount, shardsPerNode, rowsPerShard int) *crossNodeEnv {
	b.Helper()
	if nodesCount <= 0 || shardsPerNode <= 0 {
		b.Fatalf("invalid topology: nodes=%d shardsPerNode=%d", nodesCount, shardsPerNode)
	}

	totalShards := nodesCount * shardsPerNode
	client := transport.NewClient(5 * time.Second)

	remotes := make([]*crossNodeRemote, 0, nodesCount)
	nodeNames := make([]string, 0, nodesCount)

	for n := 0; n < nodesCount; n++ {
		nodeID := fmt.Sprintf("xn-node-%d", n)
		nodeNames = append(nodeNames, nodeID)

		mgr := shard.NewTestManager(nodeID)
		assignments := make(map[int]shard.Assignment, shardsPerNode)
		// Assign ONLY the shards this node hosts. The Manager doesn't
		// need to know about shards that live elsewhere — the router
		// forwards those via the remote transport, never touching this
		// node's manager.
		for k := 0; k < shardsPerNode; k++ {
			globalSID := n*shardsPerNode + k
			assignments[globalSID] = shard.Assignment{Primary: nodeID}
		}
		mgr.UpdateAssignments(assignments)

		// Populate each shard with rowsPerShard fixed rows. The exact
		// payload doesn't matter — we want predictable response sizes
		// so msgpack encoding cost stays comparable across runs.
		for k := 0; k < shardsPerNode; k++ {
			globalSID := n*shardsPerNode + k
			s := mgr.GetShard(globalSID)
			ms, ok := s.Store.(*shard.MemoryStore)
			if !ok {
				b.Fatalf("shard %d store is %T, want *shard.MemoryStore", globalSID, s.Store)
			}
			for i := 0; i < rowsPerShard; i++ {
				name := fmt.Sprintf("Person-%d-%d-%d", n, k, i)
				ms.PutNode(name, map[string]any{
					"name": name,
					"age":  int64(20 + i%60),
					"city": "Auckland",
				})
			}
		}

		srv := transport.NewTCPServer(mgr)
		if err := srv.Listen("127.0.0.1:0"); err != nil {
			b.Fatalf("listen on node %s: %v", nodeID, err)
		}

		client.SetPeerTCP(nodeID, srv.Addr().String())

		remotes = append(remotes, &crossNodeRemote{
			nodeID:  nodeID,
			manager: mgr,
			server:  srv,
		})
	}

	// Local router has zero local shards — every shard slot is nil so
	// queryShardRaw routes everything through r.remote.
	localShards := make([]*shard.Shard, totalShards)
	r := router.NewRouter(localShards, 5*time.Second)
	adapter := transport.NewRouterAdapter(client)
	placement := &crossNodePlacement{
		shardsPerNode: shardsPerNode,
		nodes:         nodeNames,
	}
	// Use a synthetic local node ID — the router only consults it for
	// write replication gating, which our read benchmarks never hit.
	r.SetRemoteTransport("xn-local", adapter, placement)

	cleanup := func() {
		// Close the client first so all pooled TCP connections drop;
		// that lets the server's per-connection read goroutines exit
		// immediately with EOF instead of blocking on the 60s idle
		// read deadline (see TCPServer.handleConn). Then Stop each
		// server in parallel so multi-node teardown is bounded by
		// the slowest server, not the sum.
		client.Close()
		var wg sync.WaitGroup
		wg.Add(len(remotes))
		for _, rem := range remotes {
			go func(s *transport.TCPServer) {
				defer wg.Done()
				s.Stop()
			}(rem.server)
		}
		wg.Wait()
	}

	return &crossNodeEnv{
		router:  r,
		client:  client,
		remotes: remotes,
		cleanup: cleanup,
	}
}

// runScatter benchmarks one Execute call per b.N iteration. The
// router's bounded scatter concurrency means b.N=1 doesn't fan out
// further than the cap, which is exactly what we want — we're
// measuring the path a single user query takes, not throughput
// under load (that's the Concurrent variant below).
func runScatter(b *testing.B, env *crossNodeEnv) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := env.router.Execute(ctx, "MATCH (n) RETURN n")
		if err != nil {
			b.Fatalf("Execute: %v", err)
		}
		if res == nil {
			b.Fatal("nil result from scatter")
		}
		if res.Partial {
			// A partial result here means a scatter timeout or a
			// remote shard error — either way the bench is no
			// longer measuring the steady-state scatter path. Fail
			// loudly so a regression doesn't hide as a benign
			// "still passes, just slower" run.
			b.Fatalf("scatter returned partial: errors=%+v", res.Errors)
		}
	}
}

// runScatterConcurrent benchmarks W concurrent in-flight Execute
// calls. Loopback can saturate quickly so this is the variant where
// the scatter-concurrency cap actually starts to matter.
func runScatterConcurrent(b *testing.B, env *crossNodeEnv, workers int) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()

	var wg sync.WaitGroup
	work := make(chan struct{}, workers*2)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for range work {
				if _, err := env.router.Execute(ctx, "MATCH (n) RETURN n"); err != nil {
					b.Errorf("Execute: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
}

// --- Single-call scatter, varying topology ---

// 2 nodes × 4 shards = 8 shards, modest rows. Smallest realistic
// cross-node topology: enough fan-out to exercise scatter merge,
// few enough shards that the concurrency cap doesn't bind.
func BenchmarkCrossNodeScatter_2x4_100rows(b *testing.B) {
	env := setupCrossNodeEnv(b, 2, 4, 100)
	defer env.cleanup()
	runScatter(b, env)
}

// 4 nodes × 4 shards = 16 shards. Default scatter cap is
// max(8, 2*shardCount)=32, so this still fits under the cap;
// difference vs 2x4 is dominated by parallel TCP handshake +
// msgpack encode on the remote side.
func BenchmarkCrossNodeScatter_4x4_100rows(b *testing.B) {
	env := setupCrossNodeEnv(b, 4, 4, 100)
	defer env.cleanup()
	runScatter(b, env)
}

// 2 nodes × 16 shards = 32 shards on only 2 hosts. The TCP pool
// per peer is finite (default 4 in NewClient) so this is where
// connection contention starts to matter — useful regression
// signal for the pool sizing in pkg/transport.
func BenchmarkCrossNodeScatter_2x16_100rows(b *testing.B) {
	env := setupCrossNodeEnv(b, 2, 16, 100)
	defer env.cleanup()
	runScatter(b, env)
}

// Light/heavy payload comparison at fixed topology.

func BenchmarkCrossNodeScatter_2x4_10rows(b *testing.B) {
	env := setupCrossNodeEnv(b, 2, 4, 10)
	defer env.cleanup()
	runScatter(b, env)
}

func BenchmarkCrossNodeScatter_2x4_1000rows(b *testing.B) {
	env := setupCrossNodeEnv(b, 2, 4, 1000)
	defer env.cleanup()
	runScatter(b, env)
}

// --- Concurrent scatter, fixed mid-size topology ---

// 8 in-flight scatter queries against 2x4. This is where the
// router's scatter-concurrency cap and the TCP pool interact —
// regressions in either show up as a throughput cliff.
func BenchmarkCrossNodeScatter_2x4_Concurrent8(b *testing.B) {
	env := setupCrossNodeEnv(b, 2, 4, 100)
	defer env.cleanup()
	runScatterConcurrent(b, env, 8)
}
