package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
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

type fakeIns struct{ rows map[string][][]any }

func (f *fakeIns) Insert(_ context.Context, table string, cols []string, rows [][]any) error {
	for _, r := range rows {
		if len(r) != len(cols) {
			return &mismatch{table, len(cols), len(r)}
		}
	}
	f.rows[table] = append(f.rows[table], rows...)
	return nil
}

type mismatch struct {
	table      string
	cols, vals int
}

func (m *mismatch) Error() string { return m.table + ": columns and values differ" }

func env(id string) *contentv1.Envelope {
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	return &contentv1.Envelope{EventId: "e" + id, Platform: "instagram", NativeId: id, ObservedAt: timestamppb.New(at),
		PublishedAt: timestamppb.New(at.Add(-time.Hour)), Version: at.UnixMicro(), Collector: "igs", RawRef: "s3://r/k#1"}
}

func i64(n int64) *int64 { return &n }

// Each content event goes to its table with exactly its columns; metrics
// go to the series of their target, and a comment's author to authors.
func TestRowsRouteToTables(t *testing.T) {
	r := Rows{}
	r.Add(&contentv1.Post{Envelope: env("1"), AccountId: "42", Kind: "highlight", Likes: i64(3)})
	r.Add(&contentv1.Comment{Envelope: env("2"), PostId: "1", Author: &contentv1.Author{AuthorKey: []byte{0xab}, Name: "fan"}})
	r.Add(&contentv1.Comment{Envelope: env("3"), PostId: "1"})
	r.Add(&contentv1.Account{Envelope: env("42"), Handle: "brand", Links: []*contentv1.Link{{Url: "https://brand.iq"}}})
	r.Add(&contentv1.Metrics{Envelope: env("1"), Target: "post", ParentId: "42", Likes: i64(3)})
	r.Add(&contentv1.Metrics{Envelope: env("2"), Target: "comment", ParentId: "1", Likes: i64(1)})
	r.Add(&contentv1.Metrics{Envelope: env("42"), Target: "account", Followers: i64(100)})
	r.Add(&contentv1.Media{Envelope: env("m1"), OwnerType: "post", OwnerId: "1", Type: "image"})
	f := &fakeIns{rows: map[string][][]any{}}
	if err := Write(context.Background(), f, r); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"posts": 1, "comments": 2, "authors": 1, "accounts": 1, "post_metrics": 1, "comment_metrics": 1,
		"account_snapshots": 1, "media": 1}
	for table, n := range want {
		if len(f.rows[table]) != n {
			t.Errorf("%s: %d rows, want %d", table, len(f.rows[table]), n)
		}
	}
	if ref := f.rows["authors"][0][1]; ref != "ab" {
		t.Errorf("a pseudonymous author is keyed by its hex key: %v", ref)
	}
}
