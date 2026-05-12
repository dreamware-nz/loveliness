package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
	"github.com/johnjansen/loveliness/pkg/tlsutil"
)

// mTLS smoke + rejection tests for the cross-node TCP transport (#90).
//
// These are NOT a redesign of mTLS — the production code in
// pkg/tlsutil and pkg/transport already does the right thing, and
// pkg/tlsutil/tlsutil_test.go exercises the cert-loading paths. What
// was missing was end-to-end evidence that an attacker presenting a
// cert signed by a different CA actually gets rejected by the
// transport server before any frame is exchanged. That's what we
// assert here.
//
// On every PR touching pkg/transport or pkg/tlsutil the CI run
// covers these two tests, so a regression that silently disables
// client-cert verification shows up immediately.

// generateCA + generateLeaf are localised in this file rather than
// shared with pkg/tlsutil/tlsutil_test.go because that package's
// helpers are test-only and unexported. Copying ~40 LOC of x509
// boilerplate is cheaper than introducing an exported test-helper
// package or restructuring tlsutil's tests.
func generateCA(t *testing.T, dir, name string) (certPath, keyPath string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	writeMTLSPEM(t, certPath, "CERTIFICATE", der)
	keyDER, _ := x509.MarshalECPrivateKey(k)
	writeMTLSPEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	parsed, _ := x509.ParseCertificate(der)
	return certPath, keyPath, parsed, k
}

func generateLeaf(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (certPath, keyPath string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	writeMTLSPEM(t, certPath, "CERTIFICATE", der)
	keyDER, _ := x509.MarshalECPrivateKey(k)
	writeMTLSPEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

func writeMTLSPEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pem.Encode(f, &pem.Block{Type: blockType, Bytes: data})
}

// TestMTLS_HappyPath asserts that two peers with leaf certs signed
// by the same cluster CA can complete an RPC end-to-end. The
// transport layer is the same code path production uses; the only
// difference is the certs are generated in-process per test.
func TestMTLS_HappyPath(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath, caCert, caKey := generateCA(t, dir, "cluster-ca")
	srvCertPath, srvKeyPath := generateLeaf(t, dir, "server", caCert, caKey)
	cliCertPath, cliKeyPath := generateLeaf(t, dir, "client", caCert, caKey)

	// Server-side: trust the cluster CA for client cert verification.
	srvTLS, err := tlsutil.MutualTLSConfig(tlsutil.Config{
		CertFile: srvCertPath, KeyFile: srvKeyPath,
		CAFile: caCertPath, ClientAuth: "require",
	})
	if err != nil {
		t.Fatalf("server tls config: %v", err)
	}

	mgr := setupTLSManager(t)
	srv := NewTCPServer(mgr)
	srv.SetTLS(srvTLS)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// Client-side: present the cluster-signed leaf, verify the
	// server against the same CA.
	cliTLS, err := tlsutil.ClientTLSConfig(tlsutil.Config{
		CertFile: cliCertPath, KeyFile: cliKeyPath,
		CAFile: caCertPath,
	})
	if err != nil {
		t.Fatalf("client tls config: %v", err)
	}

	pool := NewTCPPool(1, 2*time.Second)
	defer pool.Close()
	pool.SetTLS(cliTLS)
	pool.SetPeer("server", srv.Addr().String())

	_ = caKeyPath // referenced only via the CA above
	resp, err := pool.QueryRemoteTCP("server", 0, "MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("mTLS happy-path RPC failed: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

// TestMTLS_RejectsForeignCA is the central #90 acceptance: a client
// presenting a leaf signed by a different CA must NOT complete a
// handshake against the transport server. The exact error message
// is platform-dependent (Go's tls package wraps the handshake
// failure differently across versions); we assert it's a network
// error and not a successful RPC.
func TestMTLS_RejectsForeignCA(t *testing.T) {
	dir := t.TempDir()
	// Cluster CA — the server trusts only this CA.
	clusterCertPath, _, clusterCert, clusterKey := generateCA(t, dir, "cluster-ca")
	srvCertPath, srvKeyPath := generateLeaf(t, dir, "server", clusterCert, clusterKey)
	// Foreign CA — the attacker. Never trusted by the server.
	_, _, foreignCert, foreignKey := generateCA(t, dir, "foreign-ca")
	attackerCertPath, attackerKeyPath := generateLeaf(t, dir, "attacker", foreignCert, foreignKey)

	srvTLS, err := tlsutil.MutualTLSConfig(tlsutil.Config{
		CertFile: srvCertPath, KeyFile: srvKeyPath,
		CAFile: clusterCertPath, ClientAuth: "require",
	})
	if err != nil {
		t.Fatalf("server tls config: %v", err)
	}

	mgr := setupTLSManager(t)
	srv := NewTCPServer(mgr)
	srv.SetTLS(srvTLS)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// Attacker client trusts the cluster CA for the server side
	// (so the failure is unambiguously about the *client* cert,
	// not the server cert), but presents the foreign-CA-signed
	// leaf.
	attackerTLS, err := tlsutil.ClientTLSConfig(tlsutil.Config{
		CertFile: attackerCertPath, KeyFile: attackerKeyPath,
		CAFile: clusterCertPath,
	})
	if err != nil {
		t.Fatalf("attacker tls config: %v", err)
	}

	pool := NewTCPPool(1, 2*time.Second)
	defer pool.Close()
	pool.SetTLS(attackerTLS)
	pool.SetPeer("server", srv.Addr().String())

	_, err = pool.QueryRemoteTCP("server", 0, "MATCH (n) RETURN n")
	if err == nil {
		t.Fatal("expected mTLS rejection for foreign-CA client cert, got success")
	}
	// Sanity-check the error mentions TLS / certificate so a
	// regression that fell back to plain TCP shows up here.
	msg := err.Error()
	if !strings.Contains(msg, "tls") &&
		!strings.Contains(msg, "certificate") &&
		!strings.Contains(msg, "handshake") &&
		!strings.Contains(msg, "EOF") {
		t.Errorf("error does not look like a TLS rejection: %v", err)
	}
}

func setupTLSManager(t *testing.T) ShardQuerier {
	t.Helper()
	m := shard.NewTestManager("tls-node")
	m.UpdateAssignments(map[int]shard.Assignment{
		0: {Primary: "tls-node"},
	})
	if ms, ok := m.GetShard(0).Store.(*shard.MemoryStore); ok {
		ms.PutNode("k", map[string]any{"name": "k"})
	}
	return m
}
