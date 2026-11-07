// Package leader elects one process with a lease row in Postgres (table
// leases, in the pool's search path). A lease is taken and renewed by one
// statement each, so it works through a transaction-mode connection pool
// (PgBouncer), where session advisory locks don't: a pooled server session
// keeps the lock and can hand it to another client.
package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The lease is renewed every Renew and lasts TTL from its last renewal. The
// leader stops its work once its last good renewal is older than TTL
// minus Renew: two missed renewals, with Renew to spare before another
// process can take the lease.
const (
	Renew = 5 * time.Second
	TTL   = 15 * time.Second
)

// callTimeout bounds each lease statement.
const callTimeout = 3 * time.Second

// ErrLost is returned when another holder took the lease.
var ErrLost = errors.New("leader: the lease went to another holder")

// Holder names this process in leases: its host name and a random suffix,
// so two processes on one host differ.
var Holder = func() string {
	h, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s", h, hex.EncodeToString(b))
}()

// TryLease takes or renews the lease name for holder until ttl from now,
// and reports whether holder has it: it is free, expired, or holder's
// already.
func TryLease(ctx context.Context, pool *pgxpool.Pool, name, holder string, ttl time.Duration) (bool, error) {
	var until time.Time
	err := pool.QueryRow(ctx, `INSERT INTO leases AS l (name, holder, until)
		VALUES ($1, $2, now() + $3::bigint * interval '1 millisecond')
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, until = EXCLUDED.until
		WHERE l.holder = EXCLUDED.holder OR l.until < now()
		RETURNING until`, name, holder, ttl.Milliseconds()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ReleaseLease gives the lease back if holder has it.
func ReleaseLease(ctx context.Context, pool *pgxpool.Pool, name, holder string) error {
	_, err := pool.Exec(ctx, `DELETE FROM leases WHERE name = $1 AND holder = $2`, name, holder)
	return err
}

// Run blocks until this process holds the lease name, then calls fn with a
// context that is cancelled when the lease is lost or can't be renewed in
// time. When fn returns it gives the lease back and tries again, until ctx
// is done.
func Run(ctx context.Context, pool *pgxpool.Pool, name string, fn func(ctx context.Context) error) error {
	return RunAs(ctx, pool, name, Holder, fn)
}

// RunAs is Run with holder naming this candidate.
func RunAs(ctx context.Context, pool *pgxpool.Pool, name, holder string, fn func(ctx context.Context) error) error {
	return run(ctx, pool, name, holder, Renew, TTL, fn)
}

func run(ctx context.Context, pool *pgxpool.Pool, name, holder string, renew, ttl time.Duration, fn func(ctx context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tctx, cancel := context.WithTimeout(ctx, callTimeout)
		got, err := TryLease(tctx, pool, name, holder, ttl)
		cancel()
		if err == nil && got {
			err = lead(ctx, pool, name, holder, renew, ttl, fn)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			slog.Warn("leader: lost or failed", "lease", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(renew):
		}
	}
}

func lead(ctx context.Context, pool *pgxpool.Pool, name, holder string, renew, ttl time.Duration, fn func(ctx context.Context) error) error {
	slog.Info("leader: acquired", "lease", name)
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), callTimeout)
		defer cancel()
		if err := ReleaseLease(rctx, pool, name, holder); err != nil {
			slog.Warn("leader: release failed; the lease expires on its own", "lease", name, "err", err)
		}
	}()
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(lctx) }()
	stop := func(why error) error {
		cancel()
		if ferr := <-done; ferr != nil {
			slog.Debug("leader: work stopped", "lease", name, "err", ferr)
		}
		return why
	}
	last := time.Now()
	tick := time.NewTicker(renew)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-tick.C:
			rctx, rcancel := context.WithTimeout(ctx, callTimeout)
			got, err := TryLease(rctx, pool, name, holder, ttl)
			rcancel()
			switch {
			case err == nil && got:
				last = time.Now()
			case err == nil:
				return stop(ErrLost)
			case time.Since(last) > ttl-renew:
				// The lease may run out before another renewal lands: stop
				// before another process can take it.
				return stop(fmt.Errorf("leader: lease not renewed for %s: %w", time.Since(last).Round(time.Second), err))
			}
		}
	}
}
