package pg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Behind a transaction-mode pool (Pooled) igs sets no session state: a
// schema that is not the role's (here the database's) search path stops it
// at start, naming the setting to make; once it is, the pool works and
// sets nothing. Every pool runs statements unnamed (cache_describe), so a
// pool's server sessions keep no prepared statements. Outside a pool, the
// search path is set after connecting.
func TestPooled(t *testing.T) {
	admin := testenv.Postgres(t)
	ctx := context.Background()
	dsn := admin.Config().ConnString()
	db := admin.Config().ConnConfig.Database
	if _, err := admin.Exec(ctx, `CREATE SCHEMA ig; CREATE TABLE ig.marker (x int); INSERT INTO ig.marker VALUES (7)`); err != nil {
		t.Fatal(err)
	}

	if _, err := pg.OpenSchema(ctx, dsn, 2, "ig", pg.Pooled(true)); err == nil || !strings.Contains(err.Error(), "ALTER ROLE") {
		t.Fatalf("pooled, the search path not set: %v, want an error naming ALTER ROLE", err)
	}
	if _, err := admin.Exec(ctx, "ALTER DATABASE "+pgx.Identifier{db}.Sanitize()+" SET search_path TO ig, public"); err != nil {
		t.Fatal(err)
	}
	pooled, err := pg.OpenSchema(ctx, dsn, 2, "ig", pg.Pooled(true))
	if err != nil {
		t.Fatalf("pooled, the search path set: %v", err)
	}
	defer pooled.Close()
	var x int
	if err := pooled.QueryRow(ctx, `SELECT x FROM marker`).Scan(&x); err != nil || x != 7 {
		t.Fatalf("pooled read: %d %v", x, err)
	}
	if pooled.Config().AfterConnect != nil {
		t.Error("pooled: a session setting is made after connecting")
	}
	if m := pooled.Config().ConnConfig.DefaultQueryExecMode; m != pgx.QueryExecModeCacheDescribe {
		t.Errorf("exec mode %v, want cache_describe", m)
	}

	// Outside a pool: the search path is set after connecting.
	if _, err := admin.Exec(ctx, `CREATE SCHEMA other; CREATE TABLE other.marker (x int); INSERT INTO other.marker VALUES (9)`); err != nil {
		t.Fatal(err)
	}
	direct, err := pg.OpenSchema(ctx, dsn, 2, "other")
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	if err := direct.QueryRow(ctx, `SELECT x FROM marker`).Scan(&x); err != nil || x != 9 {
		t.Fatalf("direct read: %d %v", x, err)
	}
	if m := direct.Config().ConnConfig.DefaultQueryExecMode; m != pgx.QueryExecModeCacheDescribe {
		t.Errorf("direct exec mode %v, want cache_describe", m)
	}
}
