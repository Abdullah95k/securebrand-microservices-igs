package persist

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func obs(at time.Time, hash string) *commonv1.Observation {
	return &commonv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(hash)}
}

func i64(n int64) *int64 { return &n }

// The writer stores every entity of schema ig, keeps the newest
// observation, carries a reel's views between the reels tab and the post
// page in either order, and never writes back a suppressed profile.
func TestWriter(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	w := &Writer{DB: db}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	taken := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	const owner, reel, photo = int64(5001), int64(3_000_000_000_000_000_001), int64(3_000_000_000_000_000_002)

	// Views from the reels tab before the reel's page was read.
	must(w.Write(ctx, Batch{
		Profiles: []*igv1.Profile{{Obs: obs(t0, "p1"), UserPk: owner, Username: "Brand", FullName: "Brand", FollowerCount: i64(100),
			AuthorKey: []byte("k"), Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE, HighlightsHash: []byte("h1")}},
		Metrics: []*igv1.PostMetrics{{Obs: obs(t0, "m"), MediaPk: reel, OwnerPk: owner, TakenAt: timestamppb.New(taken),
			CapturedAt: timestamppb.New(t0), PlayCount: i64(900), LikeCount: i64(10), CommentCount: i64(2)}},
	}))
	post := func(pk int64, at time.Time, hash string, likes *int64, hidden bool) *igv1.Post {
		return &igv1.Post{Obs: obs(at, hash), MediaPk: pk, Shortcode: "C" + hash, OwnerPk: owner, TakenAt: timestamppb.New(taken),
			MediaType: igv1.MediaType_MEDIA_TYPE_VIDEO, ProductType: "clips", Caption: "c " + hash, LikeCount: likes, CountsHidden: hidden,
			CommentCount: i64(2), Status: igv1.PostStatus_POST_STATUS_VISIBLE,
			Coauthors: []*igv1.Coauthor{{UserPk: 7007, Username: "collab"}}}
	}
	t1 := t0.Add(time.Minute)
	must(w.Write(ctx, Batch{
		Posts: []*igv1.Post{post(reel, t1, "r1", i64(11), false), post(photo, t1, "p1", nil, true)},
		Metrics: []*igv1.PostMetrics{
			{Obs: obs(t1, "m1"), MediaPk: reel, OwnerPk: owner, TakenAt: timestamppb.New(taken), CapturedAt: timestamppb.New(t1), LikeCount: i64(11), CommentCount: i64(2)},
			{Obs: obs(t1, "m2"), MediaPk: photo, OwnerPk: owner, TakenAt: timestamppb.New(taken), CapturedAt: timestamppb.New(t1), CountsHidden: true, CommentCount: i64(2)},
		},
		Comments: []*igv1.Comment{{Obs: obs(t1, "c1"), CommentPk: 1, MediaPk: reel, OwnerPk: owner, PostTakenAt: timestamppb.New(taken),
			Text: "hello", CreatedAt: timestamppb.New(t1), Author: &igv1.Author{UserPk: 6001, Username: "fan", AuthorKey: []byte("a1")},
			ChildCommentCount: i64(2)}},
		Media: []*igv1.Media{{Obs: obs(t1, "x"), MediaId: "6f1c2b8e-0000-5000-8000-000000000001", SourceMediaId: "1", Variant: "original",
			Kind: commonv1.MediaKind_MEDIA_KIND_PHOTO, OwnerType: "post", UserPk: owner, MediaPk: photo, SourceUrl: "https://cdn/a.jpg"}},
		Highlights: []*igv1.Highlight{{Obs: obs(t1, "h"), HighlightId: 9, UserPk: owner, Title: "News",
			Items: []*igv1.HighlightItem{{ItemPk: 91, MediaType: 1}, {ItemPk: 92, MediaType: 2}}}},
	}))
	var likes, plays, comments *int64
	var pinned *bool
	must(db.QueryRow(ctx, `SELECT like_count, play_count, comment_count, is_pinned FROM posts WHERE media_pk = $1`, reel).Scan(&likes, &plays, &comments, &pinned))
	if likes == nil || *likes != 11 || plays == nil || *plays != 900 || *comments != 2 {
		t.Fatalf("reel: likes %v plays %v comments %v", likes, plays, comments)
	}
	must(db.QueryRow(ctx, `SELECT like_count FROM posts WHERE media_pk = $1`, photo).Scan(&likes))
	if likes != nil {
		t.Fatalf("hidden likes stored: %d", *likes)
	}
	// The reel's page sample lacked views and carried them forward: no
	// new sample for the same counts but likes.
	var samples int
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_metrics WHERE media_pk = $1`, reel).Scan(&samples))
	if samples != 2 {
		t.Fatalf("reel samples %d, want 2", samples)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_metrics WHERE media_pk = $1 AND play_count = 900`, reel).Scan(&samples))
	if samples != 2 {
		t.Fatalf("views not carried forward: %d samples with them", samples)
	}

	// The same reels sample again changes nothing; new views add a sample
	// and reach the post.
	t2 := t1.Add(time.Hour)
	must(w.Write(ctx, Batch{Metrics: []*igv1.PostMetrics{{Obs: obs(t2, "m3"), MediaPk: reel, OwnerPk: owner, TakenAt: timestamppb.New(taken),
		CapturedAt: timestamppb.New(t2), LikeCount: i64(11), CommentCount: i64(2), PlayCount: i64(900)}}}))
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_metrics WHERE media_pk = $1`, reel).Scan(&samples))
	if samples != 2 {
		t.Fatalf("unchanged sample stored: %d", samples)
	}
	t3 := t2.Add(time.Hour)
	must(w.Write(ctx, Batch{Metrics: []*igv1.PostMetrics{{Obs: obs(t3, "m4"), MediaPk: reel, OwnerPk: owner, TakenAt: timestamppb.New(taken),
		CapturedAt: timestamppb.New(t3), LikeCount: i64(15), CommentCount: i64(3), PlayCount: i64(1200)}}}))
	must(db.QueryRow(ctx, `SELECT like_count, play_count, comment_count FROM posts WHERE media_pk = $1`, reel).Scan(&likes, &plays, &comments))
	if *likes != 15 || *plays != 1200 || *comments != 3 {
		t.Fatalf("post counts not updated: %d %d %d", *likes, *plays, *comments)
	}

	// An older observation of the post changes nothing; a newer page read
	// keeps the views the page lacks.
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{post(reel, t0, "old", i64(1), false)}}))
	var caption string
	must(db.QueryRow(ctx, `SELECT caption, play_count FROM posts WHERE media_pk = $1`, reel).Scan(&caption, &plays))
	if caption != "c r1" || *plays != 1200 {
		t.Fatalf("older observation applied: %q %d", caption, *plays)
	}
	t4 := t3.Add(time.Hour)
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{post(reel, t4, "r2", i64(16), false)}}))
	must(db.QueryRow(ctx, `SELECT caption, play_count FROM posts WHERE media_pk = $1`, reel).Scan(&caption, &plays))
	if caption != "c r2" || *plays != 1200 {
		t.Fatalf("page read: %q views %v", caption, plays)
	}

	// Index, owners, comments, authors, media, highlights.
	var n int
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_index WHERE owner_pk = $1`, owner).Scan(&n))
	if n != 2 {
		t.Fatalf("post_index %d", n)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_owners WHERE user_pk = 7007 AND role = 'coauthor'`).Scan(&n))
	if n != 2 {
		t.Fatalf("coauthor rows %d", n)
	}
	var child *int64
	var author *int64
	must(db.QueryRow(ctx, `SELECT child_comment_count, author_pk FROM comments WHERE comment_pk = 1`).Scan(&child, &author))
	if child == nil || *child != 2 || author == nil || *author != 6001 {
		t.Fatalf("comment: replies %v author %v", child, author)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM comment_authors WHERE username = 'fan'`).Scan(&n))
	must(db.QueryRow(ctx, `SELECT count(*) FROM highlight_items WHERE highlight_id = 9`).Scan(&n))
	if n != 2 {
		t.Fatalf("highlight items %d", n)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM media WHERE media_pk = $1`, photo).Scan(&n))
	if n != 1 {
		t.Fatalf("media %d", n)
	}

	// A comment edited later counts one edit.
	must(w.Write(ctx, Batch{Comments: []*igv1.Comment{{Obs: obs(t4, "c2"), CommentPk: 1, MediaPk: reel, OwnerPk: owner,
		PostTakenAt: timestamppb.New(taken), Text: "hello!", CreatedAt: timestamppb.New(t1), Author: &igv1.Author{UserPk: 6001, AuthorKey: []byte("a1")}}}}))
	var edits int
	must(db.QueryRow(ctx, `SELECT edit_count FROM comments WHERE comment_pk = 1`).Scan(&edits))
	if edits != 1 {
		t.Fatalf("edits %d", edits)
	}

	// A username another account now holds moves to it.
	must(w.Write(ctx, Batch{Profiles: []*igv1.Profile{{Obs: obs(t4, "p2"), UserPk: 5002, Username: "brand", AuthorKey: []byte("k2"),
		Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE}}}))
	var old *string
	must(db.QueryRow(ctx, `SELECT username FROM profiles WHERE user_pk = $1`, owner).Scan(&old))
	if old != nil {
		t.Fatalf("old owner kept the username %q", *old)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM profile_snapshots WHERE user_pk = $1`, owner).Scan(&n))
	if n != 1 {
		t.Fatalf("snapshots %d", n)
	}

	// A post gone from Instagram changes status only.
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{{Obs: obs(t4.Add(time.Minute), "gone"), MediaPk: photo, OwnerPk: owner, TakenAt: timestamppb.New(taken),
		Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE}}}))
	var status string
	must(db.QueryRow(ctx, `SELECT status, caption FROM posts WHERE media_pk = $1`, photo).Scan(&status, &caption))
	if status != "unavailable" || caption != "c p1" {
		t.Fatalf("gone post: %s %q", status, caption)
	}

	// Nothing of a suppressed profile is written.
	_, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('profile', '5001')`)
	must(err)
	t5 := t4.Add(time.Hour)
	must(w.Write(ctx, Batch{
		Profiles: []*igv1.Profile{{Obs: obs(t5, "p3"), UserPk: owner, Username: "again", AuthorKey: []byte("k"), Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE}},
		Posts:    []*igv1.Post{post(reel, t5, "r3", i64(99), false)},
	}))
	must(db.QueryRow(ctx, `SELECT caption FROM posts WHERE media_pk = $1`, reel).Scan(&caption))
	if caption != "c r2" {
		t.Fatalf("suppressed profile's post written: %q", caption)
	}
}
