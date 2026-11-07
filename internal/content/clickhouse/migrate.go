// Package clickhouse is igs's own ClickHouse schema (migrations/clickhouse,
// architecture §6), applied for a single server or a replicated cluster by
// `igsctl clickhouse migrate`. It dates from before the shared contract:
// igs no longer writes to ClickHouse, and the platform's writer fills the
// shared store from content.* (REQ-019). The schema stays until the
// platform says it can go.
package clickhouse

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	chmigrations "github.com/abdullah95k/securebrand-microservices-igs/migrations/clickhouse"
)

// Options shape the schema for the deployment.
type Options struct {
	// Cluster, when set, creates tables ON CLUSTER with Replicated engines.
	Cluster string
	// StoragePolicy and ColdAfter move partitions older than ColdAfter to
	// the policy's 'cold' volume (object storage).
	StoragePolicy string
	ColdAfter     time.Duration
}

// Migration is one numbered schema file.
type Migration struct {
	Version    int
	Statements []string
}

// Migrations returns the embedded migrations, rendered for o, in order.
func Migrations(o Options) ([]Migration, error) {
	files, err := fs.Glob(chmigrations.FS, "*.sql")
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	var out []Migration
	for _, f := range files {
		v, err := strconv.Atoi(strings.SplitN(f, "_", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("clickhouse: migration %s: no version", f)
		}
		b, err := chmigrations.FS.ReadFile(f)
		if err != nil {
			return nil, err
		}
		m := Migration{Version: v}
		for _, st := range split(string(b)) {
			m.Statements = append(m.Statements, Render(st, o))
		}
		out = append(out, m)
	}
	return out, nil
}

// split cuts a file into statements: they end with a semicolon at the end
// of a line; comment lines go.
func split(sql string) []string {
	var out []string
	var cur []string
	for _, line := range strings.Split(sql, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		if i := strings.Index(line, " -- "); i >= 0 {
			line = line[:i]
		}
		cur = append(cur, line)
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			st := strings.TrimSuffix(strings.TrimSpace(strings.Join(cur, "\n")), ";")
			out = append(out, st)
			cur = nil
		}
	}
	return out
}

var (
	createRe = regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS (\w+) \(`)
	engineRe = regexp.MustCompile(`ENGINE = (ReplacingMergeTree|MergeTree)(\(([^)]*)\))?`)
	partRe   = regexp.MustCompile(`PARTITION BY toYYYYMM\((\w+)\)`)
)

// Render shapes one CREATE TABLE statement for o: ON CLUSTER and
// Replicated engines on a cluster, and the cold-storage TTL and storage
// policy. Other statements pass unchanged.
func Render(st string, o Options) string {
	m := createRe.FindStringSubmatch(st)
	if m == nil {
		return st
	}
	if o.Cluster != "" {
		st = createRe.ReplaceAllString(st, "CREATE TABLE IF NOT EXISTS $1 ON CLUSTER '"+o.Cluster+"' (")
		st = engineRe.ReplaceAllStringFunc(st, func(e string) string {
			p := engineRe.FindStringSubmatch(e)
			args := "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'"
			if p[3] != "" {
				args += ", " + p[3]
			}
			return "ENGINE = Replicated" + p[1] + "(" + args + ")"
		})
	}
	if o.StoragePolicy != "" {
		if col := partRe.FindStringSubmatch(st); col != nil && o.ColdAfter > 0 {
			days := max(1, int(o.ColdAfter.Hours()/24))
			st += fmt.Sprintf("\nTTL toDateTime(%s) + INTERVAL %d DAY TO VOLUME 'cold'", col[1], days)
		}
		st += "\nSETTINGS storage_policy = '" + o.StoragePolicy + "'"
	}
	return st
}

// Up applies the migrations not applied yet and records each in
// schema_migrations. It returns the versions it applied.
func Up(ctx context.Context, conn driver.Conn, o Options) ([]int, error) {
	track := Render(`CREATE TABLE IF NOT EXISTS schema_migrations (
    version    UInt32,
    applied_at DateTime
) ENGINE = ReplacingMergeTree
ORDER BY version`, Options{Cluster: o.Cluster})
	if err := conn.Exec(ctx, track); err != nil {
		return nil, fmt.Errorf("clickhouse: schema_migrations: %w", err)
	}
	done := map[int]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			_ = rows.Close()
			return nil, err
		}
		done[int(v)] = true
	}
	_ = rows.Close()
	ms, err := Migrations(o)
	if err != nil {
		return nil, err
	}
	var applied []int
	for _, m := range ms {
		if done[m.Version] {
			continue
		}
		for _, st := range m.Statements {
			if err := conn.Exec(ctx, st); err != nil {
				return applied, fmt.Errorf("clickhouse: migration %d: %w", m.Version, err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, now())`, uint32(m.Version)); err != nil {
			return applied, err
		}
		applied = append(applied, m.Version)
	}
	return applied, nil
}
