package transport

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func setupLoopbackEnv(b *testing.B) (*TCPServer, *TCPPool, *shard.Manager) {
	b.Helper()
	m := shard.NewTestManager("bench-node")
	m.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: "bench-node"},
		1: {Primary: "bench-node"},
		2: {Primary: "bench-node"},
		3: {Primary: "bench-node"},
	})

	for sid := 0; sid < 4; sid++ {
		s := m.GetShard(sid)
		if ms, ok := s.Store.(*shard.MemoryStore); ok {
			for i := 0; i < 250; i++ {
				name := fmt.Sprintf("Person-%d-%d", sid, i)
				ms.PutNode(name, map[string]any{
					"name": name,
					"age":  int64(20 + i%60),
					"city": "Auckland",
				})
			}
		}
	}

	srv := NewTCPServer(m)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}

	pool := NewTCPPool(8, 5*time.Second)
	pool.SetPeer("bench-node", srv.Addr().String())

	return srv, pool, m
}

func setupHTTPEnv(b *testing.B, nodeID string, shardCount int) (*httptest.Server, *Client) {
	b.Helper()
	m := shard.NewTestManager(nodeID)
	assignments := make(map[int]shard.Assignment)
	for i := 0; i < shardCount; i++ {
		assignments[i] = shard.Assignment{Primary: nodeID}
	}
	m.UpdateAssignments(assignments)

	for sid := 0; sid < shardCount; sid++ {
		s := m.GetShard(sid)
		if ms, ok := s.Store.(*shard.MemoryStore); ok {
			for i := 0; i < 250; i++ {
				name := fmt.Sprintf("Person-%d-%d", sid, i)
				ms.PutNode(name, map[string]any{
					"name": name,
					"age":  int64(20 + i%60),
					"city": "Auckland",
				})
			}
		}
	}

	handler := NewHandler(m)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	httpSrv := httptest.NewServer(mux)

	client := NewClient(5 * time.Second)
	client.SetPeer(nodeID, httpSrv.Listener.Addr().String())

	return httpSrv, client
}

// --- Single query ---

func BenchmarkTCPLoopback_SingleQuery(b *testing.B) {
	srv, pool, _ := setupLoopbackEnv(b)
	defer srv.Stop()
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pool.QueryRemoteTCP("bench-node", 0, "MATCH (n) RETURN n")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTPLoopback_SingleQuery(b *testing.B) {
	httpSrv, client := setupHTTPEnv(b, "http-single", 1)
	defer httpSrv.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.QueryRemote("http-single", 0, "MATCH (n) RETURN n")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Scatter-gather across 4 shards ---

func BenchmarkTCPLoopback_ScatterGather4(b *testing.B) {
	srv, pool, _ := setupLoopbackEnv(b)
	defer srv.Stop()
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for sid := 0; sid < 4; sid++ {
			wg.Add(1)
			go func(sid int) {
				defer wg.Done()
				pool.QueryRemoteTCP("bench-node", sid, "MATCH (n) RETURN n")
			}(sid)
		}
		wg.Wait()
	}
}

func BenchmarkHTTPLoopback_ScatterGather4(b *testing.B) {
	httpSrv, client := setupHTTPEnv(b, "http-sg", 4)
	defer httpSrv.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for sid := 0; sid < 4; sid++ {
			wg.Add(1)
			go func(sid int) {
				defer wg.Done()
				client.QueryRemote("http-sg", sid, "MATCH (n) RETURN n")
			}(sid)
		}
		wg.Wait()
	}
}

// --- 8-worker concurrent ---

func BenchmarkTCPLoopback_Concurrent8(b *testing.B) {
	srv, pool, _ := setupLoopbackEnv(b)
	defer srv.Stop()
	defer pool.Close()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				pool.QueryRemoteTCP("bench-node", 0, "MATCH (n) RETURN n")
			}
		}()
	}
	wg.Wait()
}

func BenchmarkHTTPLoopback_Concurrent8(b *testing.B) {
	httpSrv, client := setupHTTPEnv(b, "http-conc", 1)
	defer httpSrv.Close()

	var remaining atomic.Int64
	remaining.Store(int64(b.N))

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				client.QueryRemote("http-conc", 0, "MATCH (n) RETURN n")
			}
		}()
	}
	wg.Wait()
}
