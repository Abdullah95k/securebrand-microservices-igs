package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
)

// Deleted comments and posts are seen and marked, never removed (REQ-008
// fix 14):
//   - a scheduled refresh that finds fewer comments than last time reads
//     them all, and the job's end marks the ones no longer returned;
//   - a post's one full re-read (here at 8 days old, in production 30)
//     marks its deleted comments the same way, and a deleted post
//     unavailable;
//   - a deleted comment keeps its text, and the other comments stay
//     visible; a post is re-read once.
func TestCommentDeletions(t *testing.T) {
	const recheckAfter = 8 * 24 * time.Hour
	s := Start(t, Options{Spec: spec(), Faults: fakeig.Faults{MaxPageSize: 12}, Config: func(c *config.Config) {
		c.IG.Schedule.PostRecheckAfter = recheckAfter
	}})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	p := s.World.Profiles()[2]
	var reg struct {
		BackfillJob *dispatch.Job `json:"backfill_job"`
	}
	if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, &reg); code != http.StatusCreated {
		t.Fatalf("register: %d", code)
	}
	if st := s.WaitStatus(t, reg.BackfillJob.ID.String(), 3*time.Minute); st != "completed" {
		t.Fatalf("backfill %s", st)
	}
	// A recent post (refreshed) and two older ones (due their re-read),
	// each with comments stored.
	var recent, older, gone int64
	if err := s.DB.QueryRow(ctx, `SELECT r.media_pk FROM post_refresh r JOIN posts p ON p.media_pk = r.media_pk
		WHERE r.user_pk = $1 AND p.comment_count >= 3 ORDER BY r.taken_at DESC LIMIT 1`, p.PK).Scan(&recent); err != nil {
		t.Fatalf("recent post: %v", err)
	}
	window := `owner_pk = $1 AND status = 'visible' AND kind <> 'highlight' AND comment_count >= 3
		AND taken_at <= now() - $2::int * interval '1 second' AND taken_at > now() - ($2::int + 604800) * interval '1 second'`
	if err := s.DB.QueryRow(ctx, `SELECT media_pk FROM posts WHERE `+window+` ORDER BY taken_at DESC LIMIT 1`,
		p.PK, int(recheckAfter.Seconds())).Scan(&older); err != nil {
		t.Fatalf("older post: %v", err)
	}
	if err := s.DB.QueryRow(ctx, `SELECT media_pk FROM posts WHERE `+window+` AND media_pk <> $3 ORDER BY taken_at LIMIT 1`,
		p.PK, int(recheckAfter.Seconds()), older).Scan(&gone); err != nil {
		t.Fatalf("a post to delete: %v", err)
	}
	stored := func(media int64, n int) []int64 {
		t.Helper()
		rows, err := s.DB.Query(ctx, `SELECT comment_pk FROM comments WHERE media_pk = $1 ORDER BY created_at LIMIT $2`, media, n)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var pk int64
			_ = rows.Scan(&pk)
			out = append(out, pk)
		}
		return out
	}
	delRecent, delOlder := stored(recent, 2), stored(older, 1)
	for media, cs := range map[int64][]int64{recent: delRecent, older: delOlder} {
		if n, err := s.World.DeleteComments(media, cs...); err != nil || n != len(cs) {
			t.Fatalf("delete comments of %d: %d %v", media, n, err)
		}
	}
	if err := s.World.DeletePost(gone); err != nil {
		t.Fatal(err)
	}

	run := func(prev string) string {
		t.Helper()
		if _, err := s.DB.Exec(ctx, `UPDATE post_refresh SET next_refresh_at = now() WHERE media_pk = $1`, recent); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET next_run_at = now()`); err != nil {
			t.Fatal(err)
		}
		var job string
		deadline := time.Now().Add(30 * time.Second)
		for job == "" || job == prev {
			_ = s.DB.QueryRow(ctx, `SELECT coalesce(last_job_id::text, '') FROM profile_schedule`).Scan(&job)
			if time.Now().After(deadline) {
				t.Fatal("the planner started no scheduled run")
			}
			time.Sleep(100 * time.Millisecond)
		}
		if st := s.WaitStatus(t, job, 3*time.Minute); st != "completed" && st != "partial" {
			t.Fatalf("scheduled run %s", st)
		}
		return job
	}
	job := run(reg.BackfillJob.ID.String())

	status := func(pk int64) (st string, text bool) {
		t.Helper()
		if err := s.DB.QueryRow(ctx, `SELECT status, text IS NOT NULL AND deleted_at IS NOT NULL = (status = 'deleted')
			FROM comments WHERE comment_pk = $1`, pk).Scan(&st, &text); err != nil {
			t.Fatal(err)
		}
		return st, text
	}
	for _, pk := range append(append([]int64{}, delRecent...), delOlder...) {
		if st, ok := status(pk); st != "deleted" || !ok {
			t.Errorf("comment %d: %s (text kept and deleted_at set: %v), want deleted", pk, st, ok)
		}
	}
	var visible int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM comments WHERE media_pk IN ($1, $2) AND status = 'visible'`,
		recent, older).Scan(&visible); err != nil || visible == 0 {
		t.Errorf("no comment left visible on the two posts: %d %v", visible, err)
	}
	var deleted int64
	if err := s.DB.QueryRow(ctx, `SELECT coalesce((counters->>'comments_deleted')::bigint, 0) FROM jobs WHERE job_id = $1`, job).
		Scan(&deleted); err != nil || deleted != int64(len(delRecent)+len(delOlder)) {
		t.Errorf("job counted %d deleted comments (%v), want %d", deleted, err, len(delRecent)+len(delOlder))
	}
	var gst string
	if err := s.DB.QueryRow(ctx, `SELECT status FROM posts WHERE media_pk = $1`, gone).Scan(&gst); err != nil || gst != "unavailable" {
		t.Errorf("deleted post: %s %v, want unavailable", gst, err)
	}
	var rechecked time.Time
	if err := s.DB.QueryRow(ctx, `SELECT rechecked_at FROM posts WHERE media_pk = $1`, older).Scan(&rechecked); err != nil {
		t.Fatalf("older post not marked re-read: %v", err)
	}

	// A post is re-read once.
	run(job)
	var again time.Time
	if err := s.DB.QueryRow(ctx, `SELECT rechecked_at FROM posts WHERE media_pk = $1`, older).Scan(&again); err != nil || !again.Equal(rechecked) {
		t.Errorf("older post re-read again: %s, was %s (%v)", again, rechecked, err)
	}
}
