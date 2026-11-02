package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// igs's migrations go up, down and up again in schema ig, beside fbs's
// copied schema in public, with goose's versions in ig.goose_db_version;
// each partition helper acts on its own schema only.
func TestIGMigrationsAndPartitions(t *testing.T) {
	pub := testenv.Postgres(t) // fbs's copied schema, migrated in public
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
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
	if version != 1 {
		t.Fatalf("ig version %d", version)
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
	if created != 4*3+3*1 { // 3 monthly tables x 4 months, 1 daily table x 3 days
		t.Fatalf("created %d ig partitions", created)
	}
	if _, err := store.EnsurePartitions(ctx, pub, now, w); err != nil {
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
