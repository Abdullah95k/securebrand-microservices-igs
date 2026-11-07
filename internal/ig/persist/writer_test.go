package persist

import (
	"context"
	"strconv"
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
			AuthorKey: []byte("k"), Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE, HighlightsHash: []byte("h1"),
			LinkedFacebookPage: "Brand Page", LinkedFacebookPageId: "540790833730240", ExternalUrl: "https://brand.example/"}},
		Metrics: []*igv1.PostMetrics{{Obs: obs(t0, "m"), MediaPk: reel, OwnerPk: owner, TakenAt: timestamppb.New(taken),
			CapturedAt: timestamppb.New(t0), PlayCount: i64(900), LikeCount: i64(10), CommentCount: i64(2)}},
	}))
	// The linked Facebook page: its name and its ID (REQ-008 fix 9); the
	// website when served as external_url (fix 18).
	var fbName, fbID, website string
	must(db.QueryRow(ctx, `SELECT linked_facebook_page, linked_facebook_page_id, external_url FROM profiles WHERE user_pk = $1`,
		owner).Scan(&fbName, &fbID, &website))
	if fbName != "Brand Page" || fbID != "540790833730240" || website != "https://brand.example/" {
		t.Errorf("linked Facebook page %q, id %q, external_url %q", fbName, fbID, website)
	}
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
	must(db.QueryRow(ctx, `SELECT count(*) FROM posts WHERE kind = 'highlight' AND highlight_id = 9 AND status = 'visible'`).Scan(&n))
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
	// ... and keeps the text it replaced; its likes and replies are a series.
	var before string
	must(db.QueryRow(ctx, `SELECT text FROM comment_edits WHERE comment_pk = 1`).Scan(&before))
	if before != "hello" {
		t.Fatalf("comment edit kept %q", before)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM comment_metrics WHERE comment_pk = 1`).Scan(&n))
	if n != 1 {
		t.Fatalf("comment metrics %d, want 1 (first sighting; the edit sent no counts)", n)
	}
	t4b := t4.Add(time.Second)
	must(w.Write(ctx, Batch{Comments: []*igv1.Comment{{Obs: obs(t4b, "c3"), CommentPk: 1, MediaPk: reel, OwnerPk: owner,
		PostTakenAt: timestamppb.New(taken), Text: "hello!", CreatedAt: timestamppb.New(t1), LikeCount: i64(5), ChildCommentCount: i64(2),
		Author: &igv1.Author{UserPk: 6001, AuthorKey: []byte("a1")}}}}))
	must(db.QueryRow(ctx, `SELECT count(*) FROM comment_metrics WHERE comment_pk = 1`).Scan(&n))
	if n != 2 {
		t.Fatalf("comment metrics %d, want 2 after the likes changed", n)
	}

	// A deletion (REQ-008 fix 14) marks the comment and keeps its text; a
	// later sighting brings it back.
	t4d := t4b.Add(time.Second)
	must(w.Write(ctx, Batch{Comments: []*igv1.Comment{{Obs: obs(t4d, "del"), CommentPk: 1, MediaPk: reel, OwnerPk: owner,
		PostTakenAt: timestamppb.New(taken), Status: igv1.CommentStatus_COMMENT_STATUS_DELETED}}}))
	var cst, ctext string
	must(db.QueryRow(ctx, `SELECT status, text FROM comments WHERE comment_pk = 1`).Scan(&cst, &ctext))
	if cst != "deleted" || ctext != "hello!" {
		t.Fatalf("deleted comment: %s %q, want deleted with its text", cst, ctext)
	}
	// The post's visible top-level comments are counted (REQ-008 fix 15).
	collected := func(want int64) {
		t.Helper()
		var n *int64
		must(db.QueryRow(ctx, `SELECT comments_collected FROM posts WHERE media_pk = $1`, reel).Scan(&n))
		if n == nil || *n != want {
			t.Fatalf("comments collected %v, want %d", n, want)
		}
	}
	collected(0)
	t4e := t4d.Add(time.Second)
	must(w.Write(ctx, Batch{Comments: []*igv1.Comment{{Obs: obs(t4e, "back"), CommentPk: 1, MediaPk: reel, OwnerPk: owner,
		PostTakenAt: timestamppb.New(taken), Text: "hello!", CreatedAt: timestamppb.New(t1), LikeCount: i64(5), ChildCommentCount: i64(2),
		Author: &igv1.Author{UserPk: 6001, AuthorKey: []byte("a1")}}}}))
	must(db.QueryRow(ctx, `SELECT status FROM comments WHERE comment_pk = 1`).Scan(&cst))
	if cst != "visible" {
		t.Fatalf("comment seen after its deletion: %s, want visible", cst)
	}
	collected(1)

	// Kinds, and the caption a post's edit replaced.
	var kind string
	must(db.QueryRow(ctx, `SELECT kind FROM posts WHERE media_pk = $1`, reel).Scan(&kind))
	if kind != KindReel {
		t.Fatalf("reel kind %q", kind)
	}
	must(db.QueryRow(ctx, `SELECT count(*) FROM post_edits WHERE media_pk = $1`, reel).Scan(&n))
	if n == 0 {
		t.Fatal("the reel's earlier caption was not kept")
	}

	// Highlight items are posts of kind highlight; one gone from the
	// highlight is marked unavailable, not deleted.
	t4c := t4b.Add(time.Second)
	must(w.Write(ctx, Batch{Highlights: []*igv1.Highlight{{Obs: obs(t4c, "h2"), HighlightId: 9, UserPk: owner, Title: "Trips",
		Items: []*igv1.HighlightItem{{ItemPk: 901, TakenAt: timestamppb.New(taken), MediaType: 1}}}}}))
	var visible, gone int
	must(db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'visible'), count(*) FILTER (WHERE status = 'unavailable')
		FROM posts WHERE kind = 'highlight' AND highlight_id = 9`).Scan(&visible, &gone))
	if visible != 1 || gone != 2 {
		t.Fatalf("highlight items: %d visible, %d gone (want 1 and 2)", visible, gone)
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
	// A walled read of a stored post changes its status and reason only.
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{{Obs: obs(t4.Add(2*time.Minute), "walled"), MediaPk: reel, Shortcode: "Cr2", OwnerPk: owner,
		TakenAt: timestamppb.New(taken), MediaType: igv1.MediaType_MEDIA_TYPE_VIDEO, Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE,
		StatusReason: "login_wall"}}}))
	var reason string
	must(db.QueryRow(ctx, `SELECT status, status_reason, caption, like_count FROM posts WHERE media_pk = $1`, reel).Scan(&status, &reason, &caption, &likes))
	if status != "unavailable" || reason != "login_wall" || caption != "c r2" || likes == nil {
		t.Fatalf("walled stored post: %s %s %q likes %v, want its caption and counts kept", status, reason, caption, likes)
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

// A post whose page is walled is stored from the grid's fields, unavailable
// with reason login_wall, its counts NULL unless the reels tab gave them, in
// either order; a later read of its page makes it visible (REQ-008 fix 15).
func TestWalledPosts(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	w := &Writer{DB: db}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	taken := time.Date(2026, 10, 25, 8, 0, 0, 0, time.UTC)
	const owner, photo, reelBefore, reelAfter = int64(5101), int64(3_100_000_000_000_000_001), int64(3_100_000_000_000_000_002),
		int64(3_100_000_000_000_000_003)
	walled := func(pk int64, at time.Time, mt igv1.MediaType) *igv1.Post {
		pinned := false
		return &igv1.Post{Obs: obs(at, "w"), MediaPk: pk, Shortcode: "W" + strconv.FormatInt(pk%1000, 10), OwnerPk: owner,
			TakenAt: timestamppb.New(taken), MediaType: mt, ProductType: map[bool]string{true: "clips", false: "feed"}[mt == 2],
			IsPinned: &pinned, DiscoveredVia: igv1.DiscoveredVia_DISCOVERED_VIA_GRID,
			Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusReason: "login_wall"}
	}
	sample := func(pk int64, at time.Time) *igv1.PostMetrics {
		return &igv1.PostMetrics{Obs: obs(at, "m"), MediaPk: pk, OwnerPk: owner, TakenAt: timestamppb.New(taken),
			CapturedAt: timestamppb.New(at), PlayCount: i64(700), LikeCount: i64(30), CommentCount: i64(4)}
	}
	// The reels tab before the post's walled page, and after it (an older
	// sample persisted later).
	must(w.Write(ctx, Batch{Metrics: []*igv1.PostMetrics{sample(reelBefore, t0)}}))
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{walled(photo, t0.Add(time.Minute), igv1.MediaType_MEDIA_TYPE_PHOTO),
		walled(reelBefore, t0.Add(time.Minute), igv1.MediaType_MEDIA_TYPE_VIDEO),
		walled(reelAfter, t0.Add(time.Minute), igv1.MediaType_MEDIA_TYPE_VIDEO)}}))
	must(w.Write(ctx, Batch{Metrics: []*igv1.PostMetrics{sample(reelAfter, t0)}}))

	type row struct {
		status, reason, shortcode, state string
		mediaType                        int16
		likes, comments, plays           *int64
	}
	get := func(pk int64) row {
		t.Helper()
		var r row
		var reason *string
		must(db.QueryRow(ctx, `SELECT p.status, p.status_reason, p.shortcode, p.comments_state, p.media_type, p.like_count,
			p.comment_count, p.play_count FROM posts p JOIN post_index i USING (media_pk) JOIN post_owners o USING (media_pk)
			WHERE p.media_pk = $1 AND o.user_pk = $2`, pk, owner).Scan(&r.status, &reason, &r.shortcode, &r.state, &r.mediaType,
			&r.likes, &r.comments, &r.plays))
		if reason != nil {
			r.reason = *reason
		}
		return r
	}
	if r := get(photo); r.status != "unavailable" || r.reason != "login_wall" || r.mediaType != 1 || r.state != "none" ||
		r.likes != nil || r.comments != nil || r.plays != nil {
		t.Fatalf("walled photo: %+v, want unavailable, login_wall, no counts", r)
	}
	for _, pk := range []int64{reelBefore, reelAfter} {
		if r := get(pk); r.status != "unavailable" || r.mediaType != 2 || r.likes == nil || *r.likes != 30 || r.plays == nil ||
			*r.plays != 700 || r.comments == nil || *r.comments != 4 {
			t.Fatalf("walled reel %d: %+v, want the reels tab's counts", pk, r)
		}
	}
	// The page is read later: the post is visible, with no reason.
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{{Obs: obs(t0.Add(time.Hour), "open"), MediaPk: photo, Shortcode: "W1", OwnerPk: owner,
		TakenAt: timestamppb.New(taken), MediaType: igv1.MediaType_MEDIA_TYPE_PHOTO, Caption: "now open", LikeCount: i64(3),
		CommentCount: i64(0), Status: igv1.PostStatus_POST_STATUS_VISIBLE}}}))
	if r := get(photo); r.status != "visible" || r.reason != "" || r.likes == nil || *r.likes != 3 {
		t.Fatalf("photo read later: %+v, want visible", r)
	}
	// A gone post is marked not_found.
	must(w.Write(ctx, Batch{Posts: []*igv1.Post{{Obs: obs(t0.Add(2*time.Hour), "gone"), MediaPk: photo, OwnerPk: owner,
		TakenAt: timestamppb.New(taken), Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusReason: "not_found"}}}))
	if r := get(photo); r.status != "unavailable" || r.reason != "not_found" || r.likes == nil {
		t.Fatalf("gone photo: %+v, want unavailable, not_found, its counts kept", r)
	}
}
