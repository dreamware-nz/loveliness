package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Resolver abstracts DNS resolution for testing.
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// netResolver wraps the standard net.Resolver.
type netResolver struct{}

func (netResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// DiscoveryConfig holds settings for DNS-based peer discovery.
type DiscoveryConfig struct {
	// Addr is the DNS name to resolve (e.g., "loveliness.internal").
	Addr string
	// Interval between discovery attempts.
	Interval time.Duration
	// ExpectedNodes is the quorum gate for auto-bootstrap (0 = discover at least 2).
	ExpectedNodes int
	// HTTPPort is this node's HTTP port, used to query peers' /discovery endpoint.
	HTTPPort string
	// Self contains this node's own join info.
	Self JoinInfo
	// Bootstrap is true if LOVELINESS_BOOTSTRAP was explicitly set.
	BootstrapExplicit bool
	// Resolver is the DNS resolver (nil = default).
	Resolver Resolver
}

// JoinInfo is the minimal info needed for peer discovery.
type JoinInfo struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
	HTTPAddr string `json:"http_addr"`
	BoltAddr string `json:"bolt_addr"`
}

// Discovery manages DNS-based peer discovery and auto-bootstrap.
type Discovery struct {
	cfg      DiscoveryConfig
	resolver Resolver
	client   *http.Client
	joined   map[string]bool
	mu       sync.Mutex
	stopCh   chan struct{}
	joinFn   func(info JoinInfo) error
}

// NewDiscovery creates a new DNS discovery instance.
func NewDiscovery(cfg DiscoveryConfig, joinFn func(JoinInfo) error) *Discovery {
	r := cfg.Resolver
	if r == nil {
		r = netResolver{}
	}
	return &Discovery{
		cfg:      cfg,
		resolver: r,
		client:   &http.Client{Timeout: 5 * time.Second},
		joined:   make(map[string]bool),
		stopCh:   make(chan struct{}),
		joinFn:   joinFn,
	}
}

// ShouldBootstrap determines if this node should self-bootstrap based on
// DNS resolution. Waits for quorum, then returns true if this node has
// the numerically lowest IP.
func (d *Discovery) ShouldBootstrap(ctx context.Context) (bool, error) {
	quorum := 2
	if d.cfg.ExpectedNodes > 0 {
		quorum = d.cfg.ExpectedNodes/2 + 1
	}

	// Resolve our own IP for comparison.
	selfIP := extractIP(d.cfg.Self.RaftAddr)

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		ips, err := d.resolver.LookupHost(ctx, d.cfg.Addr)
		if err != nil {
			slog.Debug("discovery dns lookup failed", "addr", d.cfg.Addr, "err", err)
			time.Sleep(d.cfg.Interval)
			continue
		}

		if len(ips) >= quorum {
			lowest := lowestIP(ips)
			if lowest == selfIP || isLocalIP(selfIP, ips, lowest) {
				slog.Info("discovery: this node has lowest IP, bootstrapping",
					"self", selfIP, "lowest", lowest, "peers", len(ips))
				return true, nil
			}
			slog.Info("discovery: waiting for bootstrap node",
				"self", selfIP, "lowest", lowest, "peers", len(ips))
			return false, nil
		}

		slog.Debug("discovery: waiting for quorum",
			"have", len(ips), "need", quorum)
		time.Sleep(d.cfg.Interval)
	}
}

// Start begins the periodic discovery loop.
func (d *Discovery) Start() {
	go d.loop()
}

// Stop halts the discovery loop.
func (d *Discovery) Stop() {
	close(d.stopCh)
}

func (d *Discovery) loop() {
	ticker := time.NewTicker(d.cfg.Interval)
	defer ticker.Stop()

	// Run immediately on start.
	d.discover()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.discover()
		}
	}
}

func (d *Discovery) discover() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ips, err := d.resolver.LookupHost(ctx, d.cfg.Addr)
	if err != nil {
		slog.Debug("discovery dns lookup failed", "addr", d.cfg.Addr, "err", err)
		return
	}

	selfIP := extractIP(d.cfg.Self.RaftAddr)

	for _, ip := range ips {
		if ip == selfIP {
			continue
		}

		d.mu.Lock()
		alreadyJoined := d.joined[ip]
		d.mu.Unlock()
		if alreadyJoined {
			continue
		}

		info, err := d.fetchDiscoveryInfo(ctx, ip)
		if err != nil {
			slog.Debug("discovery: failed to fetch peer info", "ip", ip, "err", err)
			continue
		}

		if err := d.joinFn(info); err != nil {
			slog.Debug("discovery: join failed", "peer", info.NodeID, "err", err)
			continue
		}

		d.mu.Lock()
		d.joined[ip] = true
		d.mu.Unlock()
		slog.Info("discovery: peer joined", "node_id", info.NodeID, "ip", ip)

		// Check if we've reached expected nodes.
		if d.cfg.ExpectedNodes > 0 {
			d.mu.Lock()
			count := len(d.joined) + 1 // +1 for self
			d.mu.Unlock()
			if count >= d.cfg.ExpectedNodes {
				slog.Info("discovery: all expected nodes joined, stopping discovery",
					"count", count)
				return
			}
		}
	}
}

func (d *Discovery) fetchDiscoveryInfo(ctx context.Context, ip string) (JoinInfo, error) {
	url := fmt.Sprintf("http://[%s]:%s/discovery", ip, d.cfg.HTTPPort)
	// Try IPv4 format if the IP doesn't contain a colon.
	if !strings.Contains(ip, ":") {
		url = fmt.Sprintf("http://%s:%s/discovery", ip, d.cfg.HTTPPort)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return JoinInfo{}, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return JoinInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return JoinInfo{}, fmt.Errorf("discovery endpoint returned %d", resp.StatusCode)
	}

	var info JoinInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return JoinInfo{}, fmt.Errorf("decode discovery response: %w", err)
	}
	return info, nil
}

// lowestIP returns the numerically lowest IP from the list using raw byte comparison.
func lowestIP(ips []string) string {
	if len(ips) == 0 {
		return ""
	}
	type parsed struct {
		raw string
		ip  net.IP
	}
	var all []parsed
	for _, s := range ips {
		ip := net.ParseIP(s)
		if ip == nil {
			continue
		}
		// Normalize to 16-byte form for consistent comparison.
		all = append(all, parsed{raw: s, ip: ip.To16()})
	}
	if len(all) == 0 {
		return ips[0]
	}
	sort.Slice(all, func(i, j int) bool {
		return bytes.Compare(all[i].ip, all[j].ip) < 0
	})
	return all[0].raw
}

// extractIP extracts the IP portion from a host:port address.
func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// isLocalIP checks if selfIP matches the lowest IP (handles 0.0.0.0 and empty host).
func isLocalIP(selfIP string, allIPs []string, lowest string) bool {
	if selfIP == "" || selfIP == "0.0.0.0" || selfIP == "::" {
		// Can't determine from bind address, check if lowest is one of our IPs.
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return false
		}
		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok {
				if ipNet.IP.String() == lowest {
					return true
				}
			}
		}
	}
	return false
}
