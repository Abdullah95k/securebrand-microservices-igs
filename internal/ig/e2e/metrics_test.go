package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
)

// Every re-read post has a metrics series, not only reels (REQ-008 fix
// 16): each read of a post's page publishes a sample on
// ig.post_metrics.v1, and the store keeps one when the counts changed. A
// scheduled refresh of a photo and a carousel whose likes and comments
// changed adds a sample with the new counts; one whose counts did not
// change adds none.
func TestPostMetricsSeries(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
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
	job := reg.BackfillJob.ID.String()
	if st := s.WaitStatus(t, job, 3*time.Minute); st != "completed" {
		t.Fatalf("backfill %s", st)
	}
	// The newest refreshed photo and carousel with likes shown, and one
	// more photo left as it is.
	pick := func(mediaType int, skip int64) int64 {
		t.Helper()
		var pk int64
		if err := s.DB.QueryRow(ctx, `SELECT p.media_pk FROM post_refresh r JOIN posts p ON p.media_pk = r.media_pk
			WHERE r.user_pk = $1 AND p.media_type = $2 AND p.like_count IS NOT NULL AND p.media_pk <> $3
			ORDER BY r.taken_at DESC LIMIT 1`, p.PK, mediaType, skip).Scan(&pk); err != nil {
			t.Fatalf("a refreshed post of type %d: %v", mediaType, err)
		}
		return pk
	}
	photo, carousel := pick(1, 0), pick(8, 0)
	still := pick(1, photo)
	samples := func(pk int64) (n int, likes, comments *int64) {
		t.Helper()
		if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM post_metrics WHERE media_pk = $1`, pk).Scan(&n); err != nil {
			t.Fatal(err)
		}
		_ = s.DB.QueryRow(ctx, `SELECT like_count, comment_count FROM post_metrics WHERE media_pk = $1
			ORDER BY captured_at DESC LIMIT 1`, pk).Scan(&likes, &comments)
		return n, likes, comments
	}
	before := map[int64]int{}
	for _, pk := range []int64{photo, carousel, still} {
		n, _, _ := samples(pk)
		if n != 1 {
			t.Fatalf("post %d: %d samples after its first read, want 1", pk, n)
		}
		before[pk] = n
	}
	for i, pk := range []int64{photo, carousel} {
		if err := s.World.SetLikes(pk, int64(5000+i)); err != nil {
			t.Fatal(err)
		}
		if err := s.World.AddComments(pk, 2); err != nil {
			t.Fatal(err)
		}
	}

	// A scheduled run refreshes the three.
	if _, err := s.DB.Exec(ctx, `UPDATE post_refresh SET next_refresh_at = now() WHERE media_pk = ANY($1)`,
		[]int64{photo, carousel, still}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET next_run_at = now()`); err != nil {
		t.Fatal(err)
	}
	var run string
	deadline := time.Now().Add(30 * time.Second)
	for run == "" || run == job {
		_ = s.DB.QueryRow(ctx, `SELECT coalesce(last_job_id::text, '') FROM profile_schedule`).Scan(&run)
		if time.Now().After(deadline) {
			t.Fatal("the planner started no scheduled run")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st := s.WaitStatus(t, run, 3*time.Minute); st != "completed" {
		t.Fatalf("scheduled run %s", st)
	}
	s.Wait(t, time.Minute, run) // the persister has written what the run published

	for i, pk := range []int64{photo, carousel} {
		post := s.World.Post(pk)
		n, likes, comments := samples(pk)
		if n != before[pk]+1 || likes == nil || *likes != int64(5000+i) || comments == nil || *comments != post.CommentCount() {
			t.Errorf("post %d: %d samples, newest likes %v comments %v; want %d, likes %d, comments %d", pk, n, show(likes),
				show(comments), before[pk]+1, 5000+i, post.CommentCount())
		}
	}
	if n, _, _ := samples(still); n != before[still] {
		t.Errorf("unchanged post %d: %d samples, want %d (a sample only when the counts change)", still, n, before[still])
	}
}
