package testenv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// AsComponent gives a component its own pool on the owner's database, as
// each binary has in production. When store.Roles names a PostgreSQL role
// for the component, its sessions run as that role (SET ROLE after
// connecting), so a statement its grants don't cover fails the test as it
// would in production. IGS_E2E_SQL_LOG names a file that records each
// component's statements, to derive the grants from (store/roles.go).
func AsComponent(t testing.TB, owner *pgxpool.Pool, component string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(owner.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 8
	role := store.RoleOf(component)
	record := os.Getenv("IGS_E2E_SQL_LOG")
	if record != "" {
		// Recording runs as the owner, so every statement shows, also those
		// the grants don't cover yet.
		role = ""
	}
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		stmt := "SET search_path TO " + pgx.Identifier{store.IGSchema}.Sanitize()
		if role != "" {
			stmt += "; SET ROLE " + pgx.Identifier{role}.Sanitize()
		}
		_, err := c.Exec(ctx, stmt)
		return err
	}
	if record != "" {
		cfg.ConnConfig.Tracer = &sqlRecorder{component: component, path: record}
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// sqlRecorder appends "component<TAB>statement" lines to a file.
type sqlRecorder struct {
	component, path string
}

var recordMu sync.Mutex

func (r *sqlRecorder) record(sql string) {
	recordMu.Lock()
	defer recordMu.Unlock()
	f, err := os.OpenFile(r.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintf(f, "%s\t%s\n", r.component, strings.Join(strings.Fields(sql), " "))
}

func (r *sqlRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	r.record(d.SQL)
	return ctx
}

func (r *sqlRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *sqlRecorder) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (r *sqlRecorder) TraceBatchQuery(_ context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	r.record(d.SQL)
}

func (r *sqlRecorder) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

func (r *sqlRecorder) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceCopyFromStartData) context.Context {
	r.record("COPY " + d.TableName.Sanitize())
	return ctx
}

func (r *sqlRecorder) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}
