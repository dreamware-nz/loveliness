package cluster

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// mockResolver returns a fixed set of IPs.
type mockResolver struct {
	mu  sync.Mutex
	ips []string
	err error
}

func (m *mockResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return append([]string{}, m.ips...), nil
}

func (m *mockResolver) setIPs(ips []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ips = ips
}

func TestLowestIP_IPv4(t *testing.T) {
	tests := []struct {
		name string
		ips  []string
		want string
	}{
		{
			name: "simple ordering",
			ips:  []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"},
			want: "10.0.0.1",
		},
		{
			name: "numeric not lexicographic",
			ips:  []string{"10.0.0.9", "10.0.0.10", "10.0.0.2"},
			want: "10.0.0.2",
		},
		{
			name: "single IP",
			ips:  []string{"192.168.1.1"},
			want: "192.168.1.1",
		},
		{
			name: "empty",
			ips:  []string{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lowestIP(tt.ips)
			if got != tt.want {
				t.Errorf("lowestIP(%v) = %q, want %q", tt.ips, got, tt.want)
			}
		})
	}
}

func TestLowestIP_IPv6(t *testing.T) {
	ips := []string{
		"fdaa:0:1::3",
		"fdaa:0:1::1",
		"fdaa:0:1::2",
	}
	got := lowestIP(ips)
	if got != "fdaa:0:1::1" {
		t.Errorf("lowestIP(IPv6) = %q, want %q", got, "fdaa:0:1::1")
	}
}

func TestShouldBootstrap_LowestIP(t *testing.T) {
	resolver := &mockResolver{ips: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}

	d := NewDiscovery(DiscoveryConfig{
		Addr:          "test.internal",
		Interval:      100 * time.Millisecond,
		ExpectedNodes: 3,
		Self:          JoinInfo{RaftAddr: "10.0.0.1:9000"},
		Resolver:      resolver,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	should, err := d.ShouldBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !should {
		t.Error("expected node with lowest IP to bootstrap")
	}
}

func TestShouldBootstrap_NotLowestIP(t *testing.T) {
	resolver := &mockResolver{ips: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}

	d := NewDiscovery(DiscoveryConfig{
		Addr:          "test.internal",
		Interval:      100 * time.Millisecond,
		ExpectedNodes: 3,
		Self:          JoinInfo{RaftAddr: "10.0.0.2:9000"},
		Resolver:      resolver,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	should, err := d.ShouldBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if should {
		t.Error("expected non-lowest IP node to NOT bootstrap")
	}
}

func TestShouldBootstrap_WaitsForQuorum(t *testing.T) {
	resolver := &mockResolver{ips: []string{"10.0.0.1"}}

	d := NewDiscovery(DiscoveryConfig{
		Addr:          "test.internal",
		Interval:      50 * time.Millisecond,
		ExpectedNodes: 3,
		Self:          JoinInfo{RaftAddr: "10.0.0.1:9000"},
		Resolver:      resolver,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Add more IPs after a delay.
	go func() {
		time.Sleep(150 * time.Millisecond)
		resolver.setIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})
	}()

	should, err := d.ShouldBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !should {
		t.Error("expected bootstrap after quorum reached")
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{"10.0.0.1:9000", "10.0.0.1"},
		{"[fdaa::1]:9000", "fdaa::1"},
		{"10.0.0.1", "10.0.0.1"},
	}
	for _, tt := range tests {
		got := extractIP(tt.addr)
		if got != tt.want {
			t.Errorf("extractIP(%q) = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

func TestDiscovery_JoinsPeers(t *testing.T) {
	// Start a fake /discovery endpoint.
	var joinedMu sync.Mutex
	var joinedPeers []string

	mux := http.NewServeMux()
	mux.HandleFunc("GET /discovery", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"node_id":"peer-1","raft_addr":"10.0.0.2:9000","grpc_addr":"10.0.0.2:9001","http_addr":"10.0.0.2:8080","bolt_addr":"10.0.0.2:7687"}`))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		srv := &http.Server{Handler: mux}
		srv.Serve(ln)
	}()

	_, port, _ := net.SplitHostPort(ln.Addr().String())

	resolver := &mockResolver{ips: []string{"10.0.0.1", "127.0.0.1"}}

	d := NewDiscovery(DiscoveryConfig{
		Addr:     "test.internal",
		Interval: 100 * time.Millisecond,
		HTTPPort: port,
		Self:     JoinInfo{RaftAddr: "10.0.0.1:9000"},
		Resolver: resolver,
	}, func(info JoinInfo) error {
		joinedMu.Lock()
		joinedPeers = append(joinedPeers, info.NodeID)
		joinedMu.Unlock()
		return nil
	})

	d.Start()
	time.Sleep(300 * time.Millisecond)
	d.Stop()

	joinedMu.Lock()
	defer joinedMu.Unlock()
	if len(joinedPeers) == 0 {
		t.Error("expected at least one peer to be joined")
	}
	if joinedPeers[0] != "peer-1" {
		t.Errorf("expected peer-1, got %s", joinedPeers[0])
	}
}
