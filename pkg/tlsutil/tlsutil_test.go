package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCA creates a self-signed CA certificate and key.
func generateTestCA(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Test CA"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	certPath = filepath.Join(dir, "ca.crt")
	keyPath = filepath.Join(dir, "ca.key")
	writePEM(t, certPath, "CERTIFICATE", certDER)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)

	return certPath, keyPath
}

// generateTestCert creates a certificate signed by the given CA.
func generateTestCert(t *testing.T, dir, name string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	writePEM(t, certPath, "CERTIFICATE", certDER)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)

	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, data []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pem.Encode(f, &pem.Block{Type: blockType, Bytes: data})
}

func loadCA(t *testing.T, certPath, keyPath string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	certPEM, _ := os.ReadFile(certPath)
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, _ := os.ReadFile(keyPath)
	kblock, _ := pem.Decode(keyPEM)
	key, err := x509.ParseECPrivateKey(kblock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func TestConfig_Enabled(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		enabled bool
	}{
		{"off", Config{CertFile: "a", KeyFile: "b", Mode: "off"}, false},
		{"no cert", Config{KeyFile: "b", Mode: "required"}, false},
		{"no key", Config{CertFile: "a", Mode: "required"}, false},
		{"required", Config{CertFile: "a", KeyFile: "b", Mode: "required"}, true},
		{"optional", Config{CertFile: "a", KeyFile: "b", Mode: "optional"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Enabled(); got != tt.enabled {
				t.Errorf("Enabled() = %v, want %v", got, tt.enabled)
			}
		})
	}
}

func TestServerTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath := generateTestCA(t, dir)
	caCert, caKey := loadCA(t, caCertPath, caKeyPath)
	certPath, keyPath := generateTestCert(t, dir, "server", caCert, caKey)

	cfg := Config{CertFile: certPath, KeyFile: keyPath, Mode: "required"}
	tlsCfg, err := ServerTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Errorf("expected 1 cert, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Error("expected min TLS 1.2")
	}
}

func TestMutualTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath := generateTestCA(t, dir)
	caCert, caKey := loadCA(t, caCertPath, caKeyPath)
	certPath, keyPath := generateTestCert(t, dir, "node", caCert, caKey)

	cfg := Config{
		CertFile:   certPath,
		KeyFile:    keyPath,
		CAFile:     caCertPath,
		Mode:       "required",
		ClientAuth: "require",
	}
	tlsCfg, err := MutualTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected RequireAndVerifyClientCert, got %v", tlsCfg.ClientAuth)
	}
	if tlsCfg.ClientCAs == nil {
		t.Error("expected ClientCAs pool")
	}
	if tlsCfg.RootCAs == nil {
		t.Error("expected RootCAs pool")
	}
}

func TestMutualTLS_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath := generateTestCA(t, dir)
	caCert, caKey := loadCA(t, caCertPath, caKeyPath)
	serverCert, serverKey := generateTestCert(t, dir, "server", caCert, caKey)
	clientCert, clientKey := generateTestCert(t, dir, "client", caCert, caKey)

	serverCfg := Config{
		CertFile:   serverCert,
		KeyFile:    serverKey,
		CAFile:     caCertPath,
		ClientAuth: "require",
	}
	serverTLS, err := MutualTLSConfig(serverCfg)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server goroutine: accept one connection, echo back.
	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		buf := make([]byte, 5)
		n, err := conn.Read(buf)
		if err != nil {
			done <- err
			return
		}
		conn.Write(buf[:n])
		done <- nil
	}()

	// Client with valid mTLS cert.
	clientCfg := Config{
		CertFile: clientCert,
		KeyFile:  clientKey,
		CAFile:   caCertPath,
	}
	clientTLS, err := ClientTLSConfig(clientCfg)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("client dial failed: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("hello"))
	buf := make([]byte, 5)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("expected 'hello', got %q", string(buf[:n]))
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMutualTLS_RejectsUnknownClient(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath := generateTestCA(t, dir)
	caCert, caKey := loadCA(t, caCertPath, caKeyPath)
	serverCert, serverKey := generateTestCert(t, dir, "server", caCert, caKey)

	// Create a rogue CA + cert not signed by the cluster CA.
	rogueDir := t.TempDir()
	rogueCACert, rogueCAKey := generateTestCA(t, rogueDir)
	rogueCert, rogueKey := loadCA(t, rogueCACert, rogueCAKey)
	_ = rogueKey
	rogueClientCert, rogueClientKey := generateTestCert(t, rogueDir, "rogue", rogueCert, rogueKey)

	serverCfg := Config{
		CertFile:   serverCert,
		KeyFile:    serverKey,
		CAFile:     caCertPath,
		ClientAuth: "require",
	}
	serverTLS, err := MutualTLSConfig(serverCfg)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	// Client with rogue cert — should be rejected.
	rogueTLS := &tls.Config{
		InsecureSkipVerify: true,
	}
	cert, err := tls.LoadX509KeyPair(rogueClientCert, rogueClientKey)
	if err != nil {
		t.Fatal(err)
	}
	rogueTLS.Certificates = []tls.Certificate{cert}

	conn, err := tls.Dial("tcp", ln.Addr().String(), rogueTLS)
	if err != nil {
		return // rejected at dial — good
	}
	defer conn.Close()

	// Even if dial succeeds, the handshake should fail on write.
	_, err = conn.Write([]byte("hello"))
	if err == nil {
		// Try reading — server should have closed.
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
	}
	if err == nil {
		t.Error("expected rogue client to be rejected")
	}
}
