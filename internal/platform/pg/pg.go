// Package pg opens pgx connection pools.
package pg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Option changes how a pool connects.
type Option func(*options) error

type options struct {
	cfg    *pgxpool.Config
	pooled bool
	// retryFor bounds how long opening keeps trying while the server is
	// busy (Retryable); 0 tries once.
	retryFor time.Duration
}

// RetryFor is how long Open and OpenSchema keep trying, by default, while
// the server is out of connections or starting (Retryable): about three
// minutes, backing off from 1 s to 15 s.
const RetryFor = 3 * time.Minute

// Backoff bounds between tries.
const (
	retryMin = time.Second
	retryMax = 15 * time.Second
)

// Retry sets how long opening keeps trying while the server is busy; 0
// tries once.
func Retry(d time.Duration) Option {
	return func(o *options) error {
		o.retryFor = d
		return nil
	}
}

// Retryable reports whether err says the server is busy or starting rather
// than that the request is wrong: out of connection slots (SQLSTATE 53300,
// also a role's connection limit), starting up (57P03), out of resources
// (53000), or PgBouncer's "no more connections allowed", "server login has
// been failing" and "query_wait_timeout" (it gave up queueing for a server
// connection). A wrong password, a missing database or a refused TLS
// handshake are not.
func Retryable(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "53300", "57P03", "53000":
			return true
		}
		m := strings.ToLower(pe.Message)
		return strings.Contains(m, "no more connections allowed") || strings.Contains(m, "server login has been failing") ||
			strings.Contains(m, "query_wait_timeout")
	}
	return false
}

// Pooled says the server sits behind a transaction-mode connection pool
// (PgBouncer): a client's session state may land on any server session.
// OpenSchema then sets no search path of its own; the role's must be the
// schema, which it checks.
func Pooled(on bool) Option {
	return func(o *options) error {
		o.pooled = o.pooled || on
		return nil
	}
}

// WithCA makes TLS connections trust the root certificates in pem (the
// platform's POSTGRES_CA_CERT) and honours the DSN's sslmode:
//   - verify-full: the server's certificate must chain to pem and name the
//     host dialled;
//   - verify-ca, or require with a CA given (as libpq does with a root
//     file): it must chain to pem, whatever host it names.
//
// An empty pem means no CA was given and changes nothing; a DSN with
// sslmode disable stays without TLS.
func WithCA(pem string) Option {
	return func(o *options) error {
		c := o.cfg
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
			t.RootCAs = roots
			// pgx's own verify-ca check reads the roots it was given (none):
			// replaced below.
			t.VerifyPeerCertificate = nil
			if !t.InsecureSkipVerify {
				// verify-full: Go checks the chain and the host name.
				return t
			}
			// verify-ca, or require: the chain alone, as libpq does.
			// VerifyConnection runs on every handshake, a resumed one's too.
			t.VerifyConnection = func(cs tls.ConnectionState) error { return verifyChain(roots, cs.PeerCertificates) }
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
func verifyChain(roots *x509.CertPool, certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return errors.New("pg: the server sent no certificate")
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

func parse(dsn string, maxConns int32, opts []Option) (*options, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	// pgx's default keeps named prepared statements on the server session,
	// which a transaction-mode pool hands to other clients. cache_describe
	// describes a statement once per connection and runs it unnamed, in one
	// round trip (a DSN's own default_query_exec_mode other than the
	// default is kept).
	if cfg.ConnConfig.DefaultQueryExecMode == pgx.QueryExecModeCacheStatement {
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	}
	o := &options{cfg: cfg, retryFor: RetryFor}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// Open creates a pool and verifies connectivity. While the server is busy
// (Retryable) it tries again for RetryFor (Retry), backing off.
func Open(ctx context.Context, dsn string, maxConns int32, opts ...Option) (*pgxpool.Pool, error) {
	o, err := parse(dsn, maxConns, opts)
	if err != nil {
		return nil, err
	}
	return connect(ctx, o, func(pool *pgxpool.Pool) error { return pool.Ping(ctx) })
}

// connect creates a pool and runs check on it, again while check fails
// with a Retryable error and o.retryFor lasts.
func connect(ctx context.Context, o *options, check func(*pgxpool.Pool) error) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(o.retryFor)
	wait := retryMin
	for {
		pool, err := pgxpool.NewWithConfig(ctx, o.cfg)
		if err != nil {
			return nil, fmt.Errorf("pg: connect: %w", err)
		}
		err = check(pool)
		if err == nil {
			return pool, nil
		}
		pool.Close()
		if !Retryable(err) || time.Now().Add(wait).After(deadline) {
			return nil, fmt.Errorf("pg: ping: %w", err)
		}
		slog.Warn("pg: the database is busy; trying again", "in", wait, "err", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("pg: ping: %w (then %w)", err, ctx.Err())
		case <-time.After(wait):
		}
		wait = min(2*wait, retryMax)
	}
}

// OpenSchema creates a pool whose connections all use schema as their
// search path (igs's tables live in schema "ig", ADR 0005 §4), and checks
// at start that the schema exists and is the current one, so a mistake can
// never point a service at another service's tables of the same name.
//
// The search path is set after connecting, never as a startup parameter
// (a connection pool may refuse those). Behind a transaction-mode pool
// (Pooled) no session state holds, so nothing is set: the role's own
// search_path must be the schema (ALTER ROLE ... SET search_path).
func OpenSchema(ctx context.Context, dsn string, maxConns int32, schema string, opts ...Option) (*pgxpool.Pool, error) {
	o, err := parse(dsn, maxConns, opts)
	if err != nil {
		return nil, err
	}
	cfg := o.cfg
	if !o.pooled {
		cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
			_, err := c.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize())
			return err
		}
	}
	var current *string
	pool, err := connect(ctx, o, func(pool *pgxpool.Pool) error {
		return pool.QueryRow(ctx, "SELECT current_schema()").Scan(&current)
	})
	if err != nil {
		return nil, err
	}
	if current == nil || *current != schema {
		pool.Close()
		if o.pooled {
			return nil, fmt.Errorf("pg: the role's search path does not start with schema %q (current schema %v): run the "+
				"migrations, and set it with ALTER ROLE <role> SET search_path TO %s, public", schema, show(current), schema)
		}
		return nil, fmt.Errorf("pg: schema %q does not exist (current schema %v); run the migrations first", schema, show(current))
	}
	return pool, nil
}

func show(s *string) string {
	if s == nil {
		return "none"
	}
	return *s
}
