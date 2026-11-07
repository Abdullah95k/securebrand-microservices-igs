package pg_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Opening waits while the server is out of connection slots (here a role's
// connection limit, SQLSTATE 53300, the code a full server gives) and
// opens once one is free; it gives up when the slots stay taken past its
// time. A wrong password fails at once.
func TestOpenRetriesWhileBusy(t *testing.T) {
	admin := testenv.Postgres(t)
	ctx := context.Background()
	role := fmt.Sprintf("retry_%d", rand.Int64N(1<<40))
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw' CONNECTION LIMIT 1`, role)); err != nil {
		t.Skipf("cannot create a role: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP ROLE "+role) })
	u, err := url.Parse(admin.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	dsn := u.String()

	hold, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// The one slot is taken: a pool's connection is refused with 53300.
	_, err = pg.Open(ctx, dsn, 1, pg.Retry(0))
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "53300" || !pg.Retryable(err) {
		t.Fatalf("with the slot taken: %v", err)
	}

	// Freed 2 s in: Open, which waited, opens.
	go func() {
		time.Sleep(2 * time.Second)
		_ = hold.Close(context.Background())
	}()
	began := time.Now()
	pool, err := pg.Open(ctx, dsn, 1, pg.Retry(30*time.Second))
	if err != nil {
		t.Fatalf("Open did not wait for the slot: %v", err)
	}
	pool.Close()
	if took := time.Since(began); took < 2*time.Second {
		t.Fatalf("opened after %s, before the slot was free", took)
	}

	// Taken for longer than Open waits: it gives up with the server's error.
	hold, err = pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hold.Close(context.Background()) }()
	began = time.Now()
	if _, err := pg.Open(ctx, dsn, 1, pg.Retry(2500*time.Millisecond)); !pg.Retryable(err) {
		t.Fatalf("after its time: %v", err)
	} else if took := time.Since(began); took > 10*time.Second {
		t.Fatalf("gave up after %s", took)
	}

	// A wrong password is not the server being busy.
	u.User = url.UserPassword(role, "wrong")
	began = time.Now()
	if _, err := pg.Open(ctx, u.String(), 1); err == nil || pg.Retryable(err) {
		t.Fatalf("a wrong password: %v", err)
	}
	if took := time.Since(began); took > 2*time.Second {
		t.Fatalf("a wrong password failed after %s, not at once", took)
	}
}

// The errors that say the server is busy or starting, and only those.
func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "53300", Message: "remaining connection slots are reserved for roles with the SUPERUSER attribute"}, true},
		{&pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, true},
		{&pgconn.PgError{Code: "53000", Message: "insufficient resources"}, true},
		{&pgconn.PgError{Code: "08P01", Message: "no more connections allowed (max_client_conn)"}, true},
		{&pgconn.PgError{Code: "08P01", Message: "server login has been failing, try again later (server_login_retry)"}, true},
		{fmt.Errorf("pg: ping: %w", &pgconn.PgError{Code: "53300"}), true},
		{&pgconn.PgError{Code: "28P01", Message: "password authentication failed"}, false},
		{&pgconn.PgError{Code: "3D000", Message: `database "x" does not exist`}, false},
		{errors.New("dial tcp: connection refused"), false},
		{nil, false},
	} {
		if got := pg.Retryable(tc.err); got != tc.want {
			t.Errorf("%v: %v, want %v", tc.err, got, tc.want)
		}
	}
}
