package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// pgTool runs a PostgreSQL client tool: the local binary, or through the
// command prefix IGS_TEST_PG_TOOLS names (CI runs the server's own image,
// "docker run --rm -i --network host postgres:18-alpine", since pg_dump
// refuses a server newer than itself).
func pgTool(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	prefix := strings.Fields(os.Getenv("IGS_TEST_PG_TOOLS"))
	if len(prefix) == 0 {
		if _, err := exec.LookPath(name); err != nil {
			t.Skipf("%s not installed and IGS_TEST_PG_TOOLS not set", name)
		}
		return exec.Command(name, args...) //nolint:gosec // test tool, fixed name
	}
	full := append(append(prefix[1:], name), args...)
	return exec.Command(prefix[0], full...) //nolint:gosec // the test's own setting
}

// withDB is dsn naming database db.
func withDB(t *testing.T, dsn, db string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + db
	return u.String()
}

// The backup drill (docs/runbooks/igs-backups.md): a crawl's data, dumped
// as schema ig alone and restored into an empty database, comes back whole:
// every table and partition with the same rows, every sequence where it
// was, and the restored database needs no migration and takes new
// partitions, so igs can run on it at once.
func TestBackupRestoreDrill(t *testing.T) {
	sp := spec()
	sp.Profiles = 2
	s := Start(t, Options{Spec: sp})
	lim := s.Limits()
	var ids []string
	for _, p := range s.World.Profiles() {
		ids = append(ids, s.Crawl(t, p.Username, full, lim, nil))
	}
	s.Wait(t, 2*time.Minute, ids...)
	s.Stop()

	ctx := context.Background()
	admin := os.Getenv("IGS_TEST_PG_DSN")
	var srcDB string
	if err := s.DB.QueryRow(ctx, `SELECT current_database()`).Scan(&srcDB); err != nil {
		t.Fatal(err)
	}
	dump := filepath.Join(t.TempDir(), "ig.dump")
	f, err := os.Create(dump) //nolint:gosec // the test's temp dir
	if err != nil {
		t.Fatal(err)
	}
	cmd := pgTool(t, "pg_dump", "--format=custom", "--schema=ig", "--no-owner", "--no-privileges", "--dbname="+withDB(t, admin, srcDB))
	var stderr strings.Builder
	cmd.Stdout, cmd.Stderr = f, &stderr
	err = cmd.Run()
	_ = f.Close()
	if err != nil {
		if strings.Contains(stderr.String(), "version mismatch") && os.Getenv("IGS_TEST_PG_TOOLS") == "" {
			t.Skipf("pg_dump is older than the server; set IGS_TEST_PG_TOOLS: %s", stderr.String())
		}
		t.Fatalf("pg_dump: %v: %s", err, stderr.String())
	}

	adminPool, err := pg.Open(ctx, admin, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	dstDB := fmt.Sprintf("igs_restore_%d", rand.Int64N(1<<40))
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+dstDB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dstDB+" WITH (FORCE)") })
	in, err := os.Open(dump) //nolint:gosec // the test's temp dir
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	stderr.Reset()
	cmd = pgTool(t, "pg_restore", "--exit-on-error", "--no-owner", "--no-privileges", "--dbname="+withDB(t, admin, dstDB))
	cmd.Stdin, cmd.Stderr = in, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("pg_restore: %v: %s", err, stderr.String())
	}

	dst, err := pg.Open(ctx, withDB(t, admin, dstDB), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dst.Close)
	src := s.DB

	// Every table and partition, with its rows.
	rows, err := src.Query(ctx, `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'ig' AND c.relkind IN ('r', 'p') ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	if len(tables) < 10 {
		t.Fatalf("only %d tables in schema ig: %v", len(tables), tables)
	}
	var withRows int
	for _, tb := range tables {
		q := fmt.Sprintf(`SELECT count(*) FROM ig.%q`, tb)
		var a, b int64
		if err := src.QueryRow(ctx, q).Scan(&a); err != nil {
			t.Fatal(err)
		}
		if err := dst.QueryRow(ctx, q).Scan(&b); err != nil {
			t.Fatalf("%s after restore: %v", tb, err)
		}
		if a != b {
			t.Errorf("%s: %d rows, %d after restore", tb, a, b)
		}
		if a > 0 {
			withRows++
		}
	}
	for _, tb := range []string{"profiles", "posts", "comments", "goose_db_version"} {
		var n int64
		_ = dst.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM ig.%q`, tb)).Scan(&n)
		if n == 0 {
			t.Errorf("%s is empty after restore", tb)
		}
	}

	// Every sequence where it was, so new rows take new ids.
	seqs, err := src.Query(ctx, `SELECT sequencename, coalesce(last_value, 0) FROM pg_sequences WHERE schemaname = 'ig' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	type seq struct {
		name string
		last int64
	}
	var want []seq
	for seqs.Next() {
		var x seq
		if err := seqs.Scan(&x.name, &x.last); err != nil {
			t.Fatal(err)
		}
		want = append(want, x)
	}
	seqs.Close()
	for _, x := range want {
		var got int64
		if err := dst.QueryRow(ctx, `SELECT coalesce(last_value, 0) FROM pg_sequences WHERE schemaname = 'ig' AND sequencename = $1`, x.name).Scan(&got); err != nil {
			t.Fatalf("sequence %s after restore: %v", x.name, err)
		}
		if got != x.last {
			t.Errorf("sequence %s at %d, %d after restore", x.name, x.last, got)
		}
	}

	// igs runs on it: no migration is due, and the partition job works.
	before, err := store.MigrateIGStatus(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateIGUp(ctx, dst); err != nil {
		t.Fatalf("migrate after restore: %v", err)
	}
	after, _ := store.MigrateIGStatus(ctx, dst)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Errorf("restore left migrations to apply:\n%s\nthen\n%s", strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	ig, err := pg.OpenSchema(ctx, withDB(t, admin, dstDB), 4, store.IGSchema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ig.Close)
	if _, err := store.IGLayout.EnsurePartitions(ctx, ig, time.Now().AddDate(0, 6, 0), store.PartitionWindow{}); err != nil {
		t.Fatalf("partitions after restore: %v", err)
	}
	t.Logf("restored %d tables (%d with rows) and %d sequences", len(tables), withRows, len(want))
}
