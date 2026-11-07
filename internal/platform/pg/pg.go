// Package pg opens pgx connection pools.
package pg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Option changes how a pool connects.
type Option func(*pgxpool.Config) error

// WithCA makes connections that use TLS (sslmode require, or verify-ca)
// check the server's certificate chain against the CA in pem, the
// platform's own (POSTGRES_CA_CERT): verify-ca, the chain and not the host
// name. An empty pem means no CA was given and changes nothing; a DSN with
// sslmode disable stays without TLS.
func WithCA(pem string) Option {
	return func(c *pgxpool.Config) error {
		if strings.TrimSpace(pem) == "" {
			return nil
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(pem)) {
			return errors.New("pg: the CA (postgres.ca_pem, POSTGRES_CA_CERT) holds no PEM certificate")
		}
		apply := func(t *tls.Config) *tls.Config {
			if t == nil {
				return nil
			}
			t = t.Clone()
			// Go's own check also matches the host name; verifyChain checks
			// the chain alone, as libpq's verify-ca does.
			t.InsecureSkipVerify = true
			t.RootCAs = roots
			t.VerifyPeerCertificate = verifyChain(roots)
			return t
		}
		c.ConnConfig.TLSConfig = apply(c.ConnConfig.TLSConfig)
		for _, f := range c.ConnConfig.Fallbacks {
			f.TLSConfig = apply(f.TLSConfig)
		}
		return nil
	}
}

// verifyChain checks that the server's certificate chains to roots.
func verifyChain(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("pg: the server sent no certificate")
		}
		certs := make([]*x509.Certificate, len(raw))
		for i, r := range raw {
			c, err := x509.ParseCertificate(r)
			if err != nil {
				return fmt.Errorf("pg: server certificate: %w", err)
			}
			certs[i] = c
		}
		inter := x509.NewCertPool()
		for _, c := range certs[1:] {
			inter.AddCert(c)
		}
		if _, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter}); err != nil {
			return fmt.Errorf("pg: the server's certificate is not from the given CA: %w", err)
		}
		return nil
	}
}

func parse(dsn string, maxConns int32, opts []Option) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	for _, o := range opts {
		if err := o(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// Open creates a pool and verifies connectivity.
func Open(ctx context.Context, dsn string, maxConns int32, opts ...Option) (*pgxpool.Pool, error) {
	cfg, err := parse(dsn, maxConns, opts)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// OpenSchema creates a pool whose connections all use schema as their
// search path (igs's tables live in schema "ig", ADR 0005 §4), and checks
// at start that the schema exists and is the current one, so a mistake can
// never point a service at another service's tables of the same name.
func OpenSchema(ctx context.Context, dsn string, maxConns int32, schema string, opts ...Option) (*pgxpool.Pool, error) {
	cfg, err := parse(dsn, maxConns, opts)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	var current *string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&current); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	if current == nil || *current != schema {
		pool.Close()
		return nil, fmt.Errorf("pg: schema %q does not exist (current schema %v); run the migrations first", schema, current)
	}
	return pool, nil
}
