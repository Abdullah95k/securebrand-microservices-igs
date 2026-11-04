package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/content/clickhouse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Against a real server: the migrations apply once (again: nothing), every
// table takes its rows, the newest version of a post wins, a deleted
// comment is gone from FINAL reads, and a redelivered batch adds nothing.
func TestStoreRoundTrip(t *testing.T) {
	cfg := testenv.ClickHouse(t)
	ctx := context.Background()
	conn, err := clickhouse.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	applied, err := clickhouse.Up(ctx, conn, clickhouse.Options{})
	if err != nil || len(applied) == 0 {
		t.Fatalf("migrate: %v %v", applied, err)
	}
	if again, err := clickhouse.Up(ctx, conn, clickhouse.Options{}); err != nil || len(again) != 0 {
		t.Fatalf("second migrate applied %v, %v", again, err)
	}
	t0 := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	env := func(id string, at time.Time) *contentv1.Envelope {
		return &contentv1.Envelope{EventId: id + at.String(), Platform: "instagram", NativeId: id, ObservedAt: timestamppb.New(at),
			PublishedAt: timestamppb.New(t0.Add(-24 * time.Hour)), Version: at.UnixMicro(), Collector: "igs"}
	}
	i64 := func(n int64) *int64 { return &n }
	r := clickhouse.Rows{}
	r.Add(&contentv1.Post{Envelope: env("p1", t0), AccountId: "a1", Kind: "post", Text: "first", Likes: i64(1)})
	r.Add(&contentv1.Post{Envelope: env("p1", t0.Add(time.Hour)), AccountId: "a1", Kind: "post", Text: "edited", Likes: i64(5)})
	r.Add(&contentv1.Post{Envelope: env("h1", t0), AccountId: "a1", Kind: "highlight"})
	r.Add(&contentv1.Comment{Envelope: env("c1", t0), PostId: "p1", Text: "hi", Author: &contentv1.Author{AuthorId: "u9", Name: "fan"}})
	r.Add(&contentv1.Comment{Envelope: env("c2", t0), PostId: "p1", Text: "bye"})
	r.Add(&contentv1.Comment{Envelope: env("c2", t0.Add(time.Hour)), PostId: "p1", IsDeleted: true})
	r.Add(&contentv1.Account{Envelope: env("a1", t0), Handle: "brand", Links: []*contentv1.Link{{Url: "https://brand.iq", Title: "site"}}})
	r.Add(&contentv1.Metrics{Envelope: env("p1", t0), Target: "post", ParentId: "a1", Likes: i64(1)})
	r.Add(&contentv1.Metrics{Envelope: env("p1", t0.Add(time.Hour)), Target: "post", ParentId: "a1", Likes: i64(5)})
	r.Add(&contentv1.Metrics{Envelope: env("c1", t0), Target: "comment", ParentId: "p1", Likes: i64(2)})
	r.Add(&contentv1.Metrics{Envelope: env("a1", t0), Target: "account", Followers: i64(100)})
	r.Add(&contentv1.Media{Envelope: env("m1", t0), OwnerType: "post", OwnerId: "p1", Type: "image", Url: "https://cdn/x.jpg"})
	ins := clickhouse.Conn{C: conn}
	for range 2 { // the second time is a redelivery
		if err := clickhouse.Write(ctx, ins, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE post_metrics FINAL"); err != nil {
		t.Fatal(err)
	}
	count := func(q string) uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}
	var text string
	if err := conn.QueryRow(ctx, "SELECT text FROM posts FINAL WHERE post_id = 'p1'").Scan(&text); err != nil || text != "edited" {
		t.Fatalf("newest post version: %q %v", text, err)
	}
	for q, want := range map[string]uint64{
		"SELECT count() FROM posts FINAL":                                                2,
		"SELECT count() FROM posts FINAL WHERE kind = 'highlight'":                       1,
		"SELECT count() FROM comments FINAL WHERE is_deleted = 0":                        1, // c2 deleted
		"SELECT count() FROM authors FINAL":                                              1,
		"SELECT count() FROM accounts FINAL WHERE has(link_urls, 'https://brand.iq')":    1,
		"SELECT count() FROM post_metrics":                                               2, // two observations, the redelivery merged
		"SELECT count() FROM comment_metrics FINAL":                                      1,
		"SELECT count() FROM account_snapshots FINAL":                                    1,
		"SELECT count() FROM media FINAL":                                                1,
		"SELECT count() FROM posts FINAL WHERE post_key = cityHash64('instagram', 'p1')": 1,
	} {
		if got := count(q); got != want {
			t.Errorf("%s = %d, want %d", q, got, want)
		}
	}
}
