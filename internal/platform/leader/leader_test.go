package leader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func leases(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testenv.Postgres(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE leases (name text PRIMARY KEY, holder text NOT NULL,
		until timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return pool
}

// A fast lease for the tests.
const renew, ttl = 100 * time.Millisecond, 400 * time.Millisecond

// A lease is taken when free or expired, renewed by its holder, refused to
// anyone else while it lasts, and free again once released.
func TestLease(t *testing.T) {
	pool := leases(t)
	ctx := context.Background()
	try := func(holder string, ttl time.Duration, want bool) {
		t.Helper()
		got, err := TryLease(ctx, pool, "job", holder, ttl)
		if err != nil || got != want {
			t.Fatalf("%s takes the lease: %v (%v), want %v", holder, got, err, want)
		}
	}
	try("a", time.Hour, true)
	try("b", time.Hour, false)
	try("a", time.Hour, true) // a renewal
	if err := ReleaseLease(ctx, pool, "job", "b"); err != nil {
		t.Fatal(err)
	}
	try("b", time.Hour, false) // b could not release a's lease
	if err := ReleaseLease(ctx, pool, "job", "a"); err != nil {
		t.Fatal(err)
	}
	try("b", 50*time.Millisecond, true)
	time.Sleep(100 * time.Millisecond)
	try("a", time.Hour, true) // b's expired
	try("a", time.Hour, true)
	if got, err := TryLease(ctx, pool, "another job", "b", time.Hour); err != nil || !got {
		t.Fatalf("another lease: %v %v", got, err)
	}
}

// One process leads at a time. The leader's work stops when another holder
// takes the lease (here: the lease is handed over by hand) or when it can't
// renew it in time (its database is gone), before the lease runs out; a
// leader that finishes gives the lease back.
func TestRun(t *testing.T) {
	pool := leases(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	lead := func(c context.Context, name string, p *pgxpool.Pool, holder string) (chan struct{}, chan error) {
		leading, stopped := make(chan struct{}, 1), make(chan error, 1)
		go func() {
			_ = run(c, p, name, holder, renew, ttl, func(lctx context.Context) error {
				leading <- struct{}{}
				<-lctx.Done()
				stopped <- lctx.Err()
				return lctx.Err()
			})
		}()
		return leading, stopped
	}

	// The lease goes to another holder: the work stops.
	leading, stopped := lead(ctx, "scheduler", pool, "first")
	select {
	case <-leading:
	case <-ctx.Done():
		t.Fatal("never led")
	}
	if _, err := pool.Exec(ctx, `UPDATE leases SET holder = 'second', until = now() + interval '1 hour' WHERE name = 'scheduler'`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("work stopped with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the work kept running after its lease went to another holder")
	}

	// A leader whose database is gone stops before its lease runs out.
	own, err := pgxpool.New(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	lctx, lcancel := context.WithCancel(ctx)
	defer lcancel()
	leading, stopped = lead(lctx, "sync", own, "third")
	select {
	case <-leading:
	case <-ctx.Done():
		t.Fatal("never led")
	}
	began := time.Now()
	own.Close()
	select {
	case <-stopped:
		if took := time.Since(began); took > ttl {
			t.Errorf("work stopped %s after the database went, past the lease's %s", took, ttl)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the work kept running without its lease renewed")
	}
	lcancel()

	// Two candidates: one leads; when it is done the other takes over.
	done := make(chan struct{})
	ctxA, cancelA := context.WithCancel(ctx)
	go func() {
		_ = run(ctxA, pool, "pair", "a", renew, ttl, func(context.Context) error {
			<-done
			cancelA()
			return nil
		})
	}()
	time.Sleep(300 * time.Millisecond)
	took := make(chan struct{}, 1)
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	go func() {
		_ = run(ctxB, pool, "pair", "b", renew, ttl, func(context.Context) error {
			took <- struct{}{}
			cancelB()
			return nil
		})
	}()
	select {
	case <-took:
		t.Fatal("b led while a held the lease")
	case <-time.After(500 * time.Millisecond):
	}
	close(done)
	select {
	case <-took:
	case <-time.After(5 * time.Second):
		t.Fatal("b never took over after a finished")
	}
}
