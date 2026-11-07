package store_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Each per-component role gets exactly its privileges on schema ig: the
// crawler reads the registry but writes no content, the persister writes
// content but no jobs, the API reads content but writes none. A privilege
// granted by hand is taken away the next time the grants are applied, and
// the scheduler (the owner) has no role of its own.
func TestRoleGrants(t *testing.T) {
	db := testenv.PostgresIG(t) // creates the roles and applies their grants
	ctx := context.Background()
	can := func(role, table, priv string) bool {
		t.Helper()
		var ok bool
		if err := db.QueryRow(ctx, `SELECT has_table_privilege($1, $2, $3)`, role, store.IGSchema+"."+table, priv).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	for _, c := range []struct {
		role, table, priv string
		want              bool
	}{
		{"igs_crawler", "queries", "SELECT", true},
		{"igs_crawler", "crawl_cursors", "UPDATE", true},
		{"igs_crawler", "posts", "INSERT", false},
		{"igs_crawler", "jobs", "SELECT", false},
		{"igs_persister", "posts", "INSERT", true},
		{"igs_persister", "comments", "UPDATE", true},
		{"igs_persister", "posts", "DELETE", false},
		{"igs_persister", "jobs", "INSERT", false},
		{"igs_api", "posts", "SELECT", true},
		{"igs_api", "posts", "INSERT", false},
		{"igs_api", "jobs", "INSERT", true},
		{"igs_media", "media", "UPDATE", true},
		{"igs_media", "profiles", "SELECT", false},
		{"igs_registry_sync", "queries", "UPDATE", true},
		{"igs_registry_sync", "leases", "DELETE", true},
		{"igs_registry_sync", "jobs", "SELECT", false},
	} {
		if got := can(c.role, c.table, c.priv); got != c.want {
			t.Errorf("%s %s on %s: %v, want %v", c.role, c.priv, c.table, got, c.want)
		}
	}

	// A privilege not on the list goes away when the grants are applied.
	if _, err := db.Exec(ctx, `GRANT DELETE ON TABLE posts TO igs_crawler`); err != nil {
		t.Fatal(err)
	}
	if !can("igs_crawler", "posts", "DELETE") {
		t.Fatal("the hand-made grant didn't take")
	}
	granted, err := store.ApplyRoleGrants(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if can("igs_crawler", "posts", "DELETE") {
		t.Error("a privilege off the list survived ApplyRoleGrants")
	}
	if len(granted) != len(store.RoleNames()) {
		t.Errorf("granted %v, want every role %v", granted, store.RoleNames())
	}
	if store.RoleOf("scheduler") != "" {
		t.Error("the scheduler must keep the owner: it creates and drops partitions")
	}
}

// The persister adds the partition an old post needs, though only the
// tables' owner may create one: create_partition runs as the owner and
// creates exactly the partition its name describes. Other roles may not
// call it, and a name that isn't a partition of the table is refused.
func TestPersisterCreatesPartitions(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	persister := testenv.AsComponent(t, db, "persister")
	old := time.Date(2012, 4, 9, 12, 0, 0, 0, time.UTC)
	if err := store.IGLayout.EnsurePartitionFor(ctx, persister, "posts", old); err != nil {
		t.Fatalf("the persister's role can't add an old month: %v", err)
	}
	if err := store.IGLayout.EnsurePartitionFor(ctx, persister, "post_metrics", old); err != nil {
		t.Fatalf("the persister's role can't add an old day: %v", err)
	}
	var bounds, owner string
	var fill []string
	if err := db.QueryRow(ctx, `SELECT pg_get_expr(c.relpartbound, c.oid), pg_get_userbyid(c.relowner), coalesce(c.reloptions, '{}')
		FROM pg_class c WHERE c.oid = 'ig.posts_y2012m04'::regclass`).Scan(&bounds, &owner, &fill); err != nil {
		t.Fatal(err)
	}
	if want := "FOR VALUES FROM ('2012-04-01 00:00:00+00') TO ('2012-05-01 00:00:00+00')"; bounds != want {
		t.Errorf("bounds %q, want %q", bounds, want)
	}
	if strings.HasPrefix(owner, "igs_") {
		t.Errorf("the partition belongs to %s, want the tables' owner", owner)
	}
	if !slices.Contains(fill, "fillfactor=85") {
		t.Errorf("posts partition options %v, want fillfactor=85", fill)
	}
	// A second call finds it and creates nothing.
	var created bool
	if err := persister.QueryRow(ctx, `SELECT ig.create_partition('posts', 'posts_y2012m04', 85, NULL)`).Scan(&created); err != nil || created {
		t.Fatalf("again: created %v, err %v", created, err)
	}
	for _, bad := range [][2]string{
		{"posts", "posts_y2012m13"},        // no such month
		{"posts", "comments_y2012m04"},     // another table's name
		{"posts", "posts_y2012m04x"},       // not a partition name
		{"leases", "leases_y2012m04"},      // not partitioned
		{"posts; DROP TABLE x", "posts_y"}, // not a table
	} {
		if _, err := persister.Exec(ctx, `SELECT ig.create_partition($1, $2, NULL, NULL)`, bad[0], bad[1]); err == nil {
			t.Errorf("create_partition(%q, %q) was accepted", bad[0], bad[1])
		}
	}
	crawler := testenv.AsComponent(t, db, "crawler")
	_, err := crawler.Exec(ctx, `SELECT ig.create_partition('posts', 'posts_y2011m01', NULL, NULL)`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Errorf("the crawler's role called create_partition: %v", err)
	}
}
