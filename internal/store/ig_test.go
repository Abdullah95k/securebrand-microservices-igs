package store_test

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
	igmigrations "github.com/abdullah95k/securebrand-microservices-igs/migrations/ig"
)

// igs's migrations go up, down and up again in schema ig, with goose's
// versions in ig.goose_db_version; the partition helpers act on the pool's
// schema only, leaving another service's tables in public alone.
func TestIGMigrationsAndPartitions(t *testing.T) {
	pub := testenv.Postgres(t)
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Another service's partitioned table of the same name in public (fbs
	// keeps one), which igs's helpers must leave alone.
	_, err := pub.Exec(ctx, `CREATE TABLE public.posts (taken_at timestamptz NOT NULL, id bigint) PARTITION BY RANGE (taken_at)`)
	must(err)
	other := store.Layout{Monthly: []string{"posts"}}
	tables := func(schema string) int {
		var n int
		must(pub.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = $1`, schema).Scan(&n))
		return n
	}
	if _, err := pg.OpenSchema(ctx, pub.Config().ConnString(), 2, store.IGSchema); err == nil {
		t.Fatal("a pool on a schema that does not exist yet must fail at start")
	}
	must(store.MigrateIGUp(ctx, pub))
	up := tables("ig")
	if up < 20 {
		t.Fatalf("schema ig has %d tables after up", up)
	}
	var version int64
	must(pub.QueryRow(ctx, `SELECT max(version_id) FROM ig.goose_db_version`).Scan(&version))
	files, err := fs.Glob(igmigrations.FS, "*.sql")
	must(err)
	if version != int64(len(files)) {
		t.Fatalf("ig version %d, want %d (one per migration)", version, len(files))
	}
	// Each down undoes one migration: tables only go, never come.
	prev := up
	for i := 1; i < len(files); i++ {
		must(store.MigrateIGDown(ctx, pub))
		n := tables("ig")
		if n > prev {
			t.Fatalf("schema ig has %d tables after %d downs, %d before", n, i, prev)
		}
		prev = n
	}
	must(store.MigrateIGDown(ctx, pub))
	if n := tables("ig"); n != 1 { // only goose's version table is left
		t.Fatalf("schema ig has %d tables after down", n)
	}
	must(store.MigrateIGUp(ctx, pub))
	if n := tables("ig"); n != up {
		t.Fatalf("up again: %d tables, want %d", n, up)
	}
	st, err := store.MigrateIGStatus(ctx, pub)
	if err != nil || len(st) == 0 {
		t.Fatalf("status %v %v", st, err)
	}

	ig, err := pg.OpenSchema(ctx, pub.Config().ConnString(), 4, store.IGSchema)
	must(err)
	defer ig.Close()
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	w := store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}
	created, err := store.IGLayout.EnsurePartitions(ctx, ig, now, w)
	must(err)
	if want := 4*len(store.IGLayout.Monthly) + 3*len(store.IGLayout.Daily); created != want { // 4 months, 3 days
		t.Fatalf("created %d ig partitions", created)
	}
	if _, err := other.EnsurePartitions(ctx, pub, now, w); err != nil {
		t.Fatal(err)
	}
	var inIG, inPublic int
	must(pub.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'ig' AND tablename LIKE 'posts_y%'`).Scan(&inIG))
	must(pub.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'posts_y%'`).Scan(&inPublic))
	if inIG != 4 || inPublic != 4 {
		t.Fatalf("posts partitions: ig %d, public %d", inIG, inPublic)
	}
	// Dropping through the ig pool drops ig's old partitions, not public's.
	dropped, err := store.DropPartitionsBefore(ctx, ig, "posts", now.AddDate(0, -1, 0))
	must(err)
	if len(dropped) != 1 {
		t.Fatalf("dropped %v", dropped)
	}
	must(pub.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'posts_y%'`).Scan(&inPublic))
	if inPublic != 4 {
		t.Fatalf("an ig drop removed public partitions: %d left", inPublic)
	}
	// A row lands in its partition through the ig pool's search path.
	if _, err := ig.Exec(ctx, `INSERT INTO post_index (media_pk, owner_pk, taken_at, shortcode) VALUES (1, 2, now(), 'abc')`); err != nil {
		t.Fatal(err)
	}
	var n int
	must(pub.QueryRow(ctx, `SELECT count(*) FROM ig.post_index`).Scan(&n))
	if n != 1 {
		t.Fatal("the ig pool did not write to schema ig")
	}
}
