package persist

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// Bisection finds every record that fails on its own data and writes the
// rest; an error that is not a record's stops it and is returned, so the
// batch is retried whole.
func TestIsolateFindsTheBadRecords(t *testing.T) {
	items := make([]item, 10)
	for i := range items {
		items[i] = item{rec: &kgo.Record{Offset: int64(i)}}
	}
	poison := map[int64]bool{3: true, 7: true}
	written := map[int64]bool{}
	write := func(_ context.Context, xs []item) error {
		for _, x := range xs {
			if poison[x.rec.Offset] {
				return &pgconn.PgError{Code: "22P02", Message: "invalid input syntax for type uuid"}
			}
		}
		for _, x := range xs {
			written[x.rec.Offset] = true
		}
		return nil
	}
	bads, err := isolate(context.Background(), items, write)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int64
	for _, b := range bads {
		offs = append(offs, b.rec.Offset)
		if b.reason != "sqlstate_22P02" {
			t.Fatalf("reason %q", b.reason)
		}
	}
	if !slices.Equal(offs, []int64{3, 7}) {
		t.Fatalf("quarantined %v, want [3 7]", offs)
	}
	for i := int64(0); i < 10; i++ {
		if !poison[i] && !written[i] {
			t.Fatalf("record %d was not written", i)
		}
	}
	down := errors.New("dial tcp: connection refused")
	if _, err := isolate(context.Background(), items, func(context.Context, []item) error { return down }); !errors.Is(err, down) {
		t.Fatalf("an outage must stop the bisection: %v", err)
	}
	cancelled := &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}
	if _, err := isolate(context.Background(), items, func(context.Context, []item) error { return cancelled }); err == nil {
		t.Fatal("a statement timeout is not the records' fault")
	}
	deadlock := &pgconn.PgError{Code: "40P01"}
	if bads, err := isolate(context.Background(), items, func(context.Context, []item) error { return deadlock }); err == nil || bads != nil {
		t.Fatal("a deadlock is not the records' fault")
	}
}

// A record that does not decode is quarantined, not written half-decoded.
func TestUndecodableRecordsAreQuarantined(t *testing.T) {
	c := &Consumer{prefix: "t."}
	good, _ := proto.Marshal(&fbv1.Post{PostId: 1})
	items, bads := c.decode([]*kgo.Record{
		{Topic: "t." + topics.Post, Value: good},
		{Topic: "t." + topics.Comment, Value: []byte{0xff, 0xff, 0x01}},
		{Topic: "t.other", Value: []byte("x")},
	})
	if len(items) != 1 || len(bads) != 1 || bads[0].reason != "undecodable" {
		t.Fatalf("items %d, bads %+v", len(items), bads)
	}
}

// Against Postgres and an in-memory Kafka: a media record Postgres rejects
// (an id that is not a UUID) goes to fbs.persist.dlq with its origin, the
// batch's other records are written, and the batch is committed.
func TestConsumerQuarantinesAndMovesOn(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	kc, err := kfake.NewCluster(kfake.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	cfg := kafka.ClientConfig{Brokers: kc.ListenAddrs(), ClientID: "test", TopicPrefix: "q."}
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	b := p.NewBatch()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	const good = "00000000-0000-0000-0000-0000000000a7"
	must(b.AddProto(ctx, topics.Media, "good", &fbv1.Media{Obs: obs(now, "g"), MediaId: good, PageId: 70, PostId: 77, Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO}))
	must(b.AddProto(ctx, topics.Media, "bad", &fbv1.Media{Obs: obs(now, "m"), MediaId: "not-a-uuid", PageId: 70, PostId: 77, Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO}))
	must(b.Wait(ctx))

	c, err := NewConsumer(cfg, &Writer{DB: db}, p, 100, 100*time.Millisecond)
	must(err)
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(rctx) }()
	var n int
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		_ = db.QueryRow(ctx, "SELECT count(*) FROM media WHERE media_id = $1", good).Scan(&n)
		if n == 1 {
			break
		}
	}
	cancel()
	must(<-done)
	if n != 1 {
		t.Fatal("the good record was not written")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(kc.ListenAddrs()...), kgo.ConsumeTopics("q."+topics.PersistDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	must(err)
	defer cl.Close()
	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	defer pcancel()
	recs := cl.PollFetches(pctx).Records()
	if len(recs) != 1 || kafka.HeaderValue(recs[0], kafka.HeaderOrigTopic) != topics.Media ||
		kafka.HeaderValue(recs[0], kafka.HeaderReason) != "encode" || kafka.HeaderValue(recs[0], kafka.HeaderOrigOffset) == "" {
		t.Fatalf("quarantine: %v", recs)
	}
	// The batch was committed: a new consumer of the group starts after it.
	adm, err := kgo.NewClient(kgo.SeedBrokers(kc.ListenAddrs()...))
	must(err)
	defer adm.Close()
	ends, err := kafka.EndOffsets(ctx, adm, "q."+topics.Media)
	must(err)
	ok, err := kafka.GroupCaughtUp(ctx, adm, "q."+Group, ends)
	must(err)
	if !ok {
		t.Fatal("the group did not commit past the quarantined record")
	}
}

// Records published before an erasure do not write the erased data back:
// a suppressed page's entities are dropped, a suppressed author's comment
// is redacted, and the media of a redacted comment (in the batch, or
// redacted earlier by an erasure) is dropped.
func TestSuppressFilters(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', '501'), ('author', '6b6579')`)
	must(err)
	created := timestamppb.New(now.Add(-time.Hour))
	cid, other := int64(9001), int64(9002)
	in := Batch{
		Pages: []*fbv1.Page{{PageId: 501, Name: "Erased"}, {PageId: 502, Name: "Kept"}},
		Posts: []*fbv1.Post{{PostId: 5011, PageId: 501, CreatedAt: created}, {PostId: 5021, PageId: 502, CreatedAt: created}},
		Comments: []*fbv1.Comment{
			{CommentId: cid, PostId: 5021, PageId: 502, Text: "secret", Author: &fbv1.Author{Id: "a", Name: "Name", AuthorKey: []byte("key")}},
			{CommentId: 9003, PostId: 5011, PageId: 501, Text: "on an erased page"},
			{CommentId: 9004, PostId: 5021, PageId: 502, Text: "kept", Author: &fbv1.Author{Id: "b", AuthorKey: []byte("other")}},
		},
		Media: []*fbv1.Media{{MediaId: "m1", PageId: 502, CommentId: &cid}, {MediaId: "m2", PageId: 502, CommentId: &other},
			{MediaId: "m3", PageId: 502}, {MediaId: "m4", PageId: 501}},
	}
	out, err := (&Writer{DB: db}).Suppress(ctx, in)
	must(err)
	if len(out.Pages) != 1 || out.Pages[0].PageId != 502 || len(out.Posts) != 1 || out.Posts[0].PostId != 5021 {
		t.Fatalf("pages %v posts %v", out.Pages, out.Posts)
	}
	if len(out.Comments) != 2 || !out.Comments[0].Redacted || out.Comments[0].Text != "" || out.Comments[0].Author.Name != "" ||
		string(out.Comments[0].Author.AuthorKey) != "key" || out.Comments[1].Redacted {
		t.Fatalf("comments %v", out.Comments)
	}
	if len(out.Media) != 2 || out.Media[0].MediaId != "m2" || out.Media[1].MediaId != "m3" {
		t.Fatalf("media %v", out.Media)
	}
}

// A suppressed page's row is never written back, by the batch or by the
// page upsert itself (which used to set status 'active').
func TestSuppressedPageStaysSuppressed(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.Exec(ctx, `INSERT INTO pages (page_id, status) VALUES (601, 'suppressed')`)
	must(err)
	w := &Writer{DB: db}
	must(w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "pg"), PageId: 601, Name: "Again"}}))
	var status string
	var name *string
	must(db.QueryRow(ctx, "SELECT status, name FROM pages WHERE page_id = 601").Scan(&status, &name))
	if status != "suppressed" || name != nil {
		t.Fatalf("page upsert revived a suppressed page: %s %v", status, name)
	}
}
