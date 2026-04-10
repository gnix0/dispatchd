package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/gnix0/dispatchd/internal/platform/config"
	"google.golang.org/grpc/credentials"
)

func loadTransportCredentials(cfg config.Service) (credentials.TransportCredentials, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
	}

	if cfg.TLSClientCAFile != "" {
		caPEM, err := os.ReadFile(cfg.TLSClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client ca file: %w", err)
		}

		clientCAPool := x509.NewCertPool()
		if !clientCAPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("append client ca certificates: invalid pem")
		}

		tlsConfig.ClientCAs = clientCAPool
		if cfg.TLSRequireClientCert {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	return credentials.NewTLS(tlsConfig), nil
}
