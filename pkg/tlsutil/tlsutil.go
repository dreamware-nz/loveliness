package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config holds TLS file paths and mode.
type Config struct {
	CertFile   string // server certificate
	KeyFile    string // server private key
	CAFile     string // CA certificate for mTLS verification
	Mode       string // "required", "optional", "off"
	ClientAuth string // "require", "request", "none"
}

// Enabled returns true if TLS is configured (cert+key present and mode != off).
func (c Config) Enabled() bool {
	return c.CertFile != "" && c.KeyFile != "" && c.Mode != "off"
}

// ServerTLSConfig builds a tls.Config for server listeners (HTTP, Bolt).
// Does not require client certs.
func ServerTLSConfig(cfg Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	return tlsCfg, nil
}

// MutualTLSConfig builds a tls.Config for inter-node mTLS.
// Both server and client must present certs signed by the cluster CA.
func MutualTLSConfig(cfg Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cert: %w", err)
	}

	caPool, err := loadCAPool(cfg.CAFile)
	if err != nil {
		return nil, err
	}

	clientAuth := tls.RequireAndVerifyClientCert
	switch cfg.ClientAuth {
	case "request":
		clientAuth = tls.VerifyClientCertIfGiven
	case "none":
		clientAuth = tls.NoClientCert
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		RootCAs:      caPool,
		ClientAuth:   clientAuth,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ClientTLSConfig builds a tls.Config for outbound mTLS connections.
// Presents the node cert and verifies the server against the cluster CA.
func ClientTLSConfig(cfg Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}

	caPool, err := loadCAPool(cfg.CAFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	if caFile == "" {
		return nil, fmt.Errorf("CA file required for mTLS")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file contains no valid certificates")
	}
	return pool, nil
}
