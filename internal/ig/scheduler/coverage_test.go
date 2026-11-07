package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// The job's end writes each visible post's comment coverage from how its
// read ended (REQ-008 fix 15): complete, truncated, partial (capped, not
// finished or failed); a scheduled read since the last one keeps that
// one's state; a post with no comments is complete; other skipped,
// walled and unavailable posts keep theirs. Collected counts the visible
// top-level comments.
func TestCoverage(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	w := &persist.Writer{DB: db}
	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	taken := time.Date(2026, 10, 28, 8, 0, 0, 0, time.UTC)
	const owner = int64(5201)
	obs := func(h string) *commonv1.Observation {
		return &commonv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(h)}
	}
	cases := []struct {
		name, prior, state, want string
		reported                 int64
		status                   igv1.PostStatus
		reason                   string
	}{
		{name: "complete", state: jobs.PostComplete, want: "complete", reported: 5},
		{name: "truncated", prior: "complete", state: jobs.PostTruncated, want: "truncated", reported: 5},
		{name: "capped", state: jobs.PostCapped, want: "partial", reported: 5},
		{name: "unfinished", prior: "complete", state: jobs.PostPending, want: "partial", reported: 5},
		{name: "failed", state: jobs.PostFailed, want: "partial", reported: 5},
		{name: "updated after complete", prior: "complete", state: jobs.PostUpdated, want: "complete", reported: 5},
		{name: "updated after truncated", prior: "truncated", state: jobs.PostUpdated, want: "truncated", reported: 5},
		{name: "updated, none before", state: jobs.PostUpdated, want: "partial", reported: 5},
		{name: "no comments", state: jobs.PostSkipped, want: "complete"},
		{name: "unchanged", prior: "truncated", state: jobs.PostSkipped, want: "truncated", reported: 5},
		{name: "walled now, stored before", prior: "complete", state: jobs.PostGated, want: "complete"},
		{name: "walled", state: jobs.PostGated, want: "none", status: igv1.PostStatus_POST_STATUS_UNAVAILABLE, reason: "login_wall"},
		{name: "gone", prior: "complete", state: jobs.PostComplete, want: "complete", status: igv1.PostStatus_POST_STATUS_UNAVAILABLE,
			reason: "not_found"},
	}
	var posts []jobs.PostState
	var batch persist.Batch
	for i, c := range cases {
		pk := int64(3_200_000_000_000_000_001 + i)
		p := &igv1.Post{Obs: obs(c.name), MediaPk: pk, Shortcode: "K" + string(rune('a'+i)), OwnerPk: owner, TakenAt: timestamppb.New(taken),
			MediaType: igv1.MediaType_MEDIA_TYPE_PHOTO, Status: igv1.PostStatus_POST_STATUS_VISIBLE}
		if c.reported > 0 {
			p.CommentCount = &c.reported
		}
		batch.Posts = append(batch.Posts, p)
		posts = append(posts, jobs.PostState{PostID: pk, CreatedAt: taken, Reported: c.reported, State: c.state})
	}
	// The first post's comments: three visible, one deleted, one reply.
	for i, depth := range []int32{0, 0, 0, 0, 1} {
		c := &igv1.Comment{Obs: obs("c"), CommentPk: int64(900 + i), MediaPk: posts[0].PostID, OwnerPk: owner,
			PostTakenAt: timestamppb.New(taken), Text: "synthetic", CreatedAt: timestamppb.New(at), Depth: depth}
		if depth == 1 {
			parent := int64(900)
			c.ParentCommentPk = &parent
		}
		batch.Comments = append(batch.Comments, c)
	}
	if err := w.Write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := persist.MarkDeleted(ctx, db, []*igv1.Comment{{Obs: &commonv1.Observation{ObservedAt: timestamppb.New(at.Add(time.Minute))},
		CommentPk: 903, MediaPk: posts[0].PostID, PostTakenAt: timestamppb.New(taken)}}); err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		if _, err := db.Exec(ctx, `UPDATE posts SET comments_state = coalesce($2, 'none'), comments_collected = NULL,
			status = CASE WHEN $3 = '' THEN 'visible' ELSE 'unavailable' END, status_reason = nullif($3, '') WHERE media_pk = $1`,
			posts[i].PostID, nilIfEmpty(c.prior), c.reason); err != nil {
			t.Fatal(err)
		}
	}

	if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error { return coverage(ctx, tx, posts) }); err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		var state string
		var collected *int64
		if err := db.QueryRow(ctx, `SELECT comments_state, comments_collected FROM posts WHERE media_pk = $1`, posts[i].PostID).
			Scan(&state, &collected); err != nil {
			t.Fatal(err)
		}
		if state != c.want {
			t.Errorf("%s: comments %q, want %q", c.name, state, c.want)
		}
		wantCollected := int64(0)
		if i == 0 {
			wantCollected = 3
		}
		visible := c.reason == "" && c.state != jobs.PostGated
		if visible && (collected == nil || *collected != wantCollected) {
			t.Errorf("%s: collected %v, want %d", c.name, collected, wantCollected)
		}
		if !visible && collected != nil {
			t.Errorf("%s: collected %d written, want it left alone", c.name, *collected)
		}
	}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
