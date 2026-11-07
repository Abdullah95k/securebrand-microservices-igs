package clickhouse

import (
	"strings"
	"testing"
	"time"
)

// The schema has every table of architecture §6 and the entity layer.
func TestMigrationsHoldEveryTable(t *testing.T) {
	ms, err := Migrations(Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, m := range ms {
		for _, st := range m.Statements {
			if c := createRe.FindStringSubmatch(st); c != nil {
				got[c[1]] = true
			} else {
				t.Errorf("not a CREATE TABLE: %.80s", st)
			}
		}
	}
	for _, want := range []string{"posts", "comments", "post_metrics", "comment_metrics", "accounts", "account_snapshots", "authors",
		"media", "entities", "entity_accounts"} {
		if !got[want] {
			t.Errorf("no table %s", want)
		}
	}
}

// On a cluster every table is created ON CLUSTER with a Replicated engine
// (its arguments kept); a storage policy moves old partitions to 'cold'.
func TestRenderForClusterAndTiering(t *testing.T) {
	ms, _ := Migrations(Options{Cluster: "sb", StoragePolicy: "tiered", ColdAfter: 180 * 24 * time.Hour})
	for _, m := range ms {
		for _, st := range m.Statements {
			name := createRe.FindStringSubmatch(strings.Replace(st, " ON CLUSTER 'sb'", "", 1))
			if !strings.Contains(st, " ON CLUSTER 'sb' (") || !strings.Contains(st, "ENGINE = Replicated") ||
				!strings.Contains(st, "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'") ||
				!strings.HasSuffix(st, "SETTINGS storage_policy = 'tiered'") {
				t.Errorf("%v not rendered for the cluster:\n%s", name, st)
			}
			if strings.Contains(st, "PARTITION BY") != strings.Contains(st, "TO VOLUME 'cold'") {
				t.Errorf("%v: a partitioned table moves to cold, an unpartitioned one cannot:\n%s", name, st)
			}
		}
	}
	posts := Render("CREATE TABLE IF NOT EXISTS posts (\n x UInt8\n) ENGINE = ReplacingMergeTree(version, is_deleted)\nPARTITION BY toYYYYMM(published_at)\nORDER BY x",
		Options{Cluster: "sb", StoragePolicy: "tiered", ColdAfter: 180 * 24 * time.Hour})
	if !strings.Contains(posts, "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', version, is_deleted)") ||
		!strings.Contains(posts, "TTL toDateTime(published_at) + INTERVAL 180 DAY TO VOLUME 'cold'") {
		t.Fatalf("posts:\n%s", posts)
	}
	if one := Render("CREATE TABLE IF NOT EXISTS posts (\n x UInt8\n) ENGINE = MergeTree\nORDER BY x", Options{}); strings.Contains(one, "Replicated") ||
		strings.Contains(one, "SETTINGS") {
		t.Fatalf("a single server's table was changed:\n%s", one)
	}
}
