package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
)

// A registered profile is backfilled; the finalizer completes the job and
// records the recent posts for comment refreshes. When the schedule is due,
// the planner starts a scheduled run on lane p1 that reads the new posts,
// reads the profile again when its daily refresh is due, and refreshes the
// due posts' comments; the finalizer completes it too.
func TestScheduledRuns(t *testing.T) {
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
	if st := s.WaitStatus(t, reg.BackfillJob.ID.String(), 3*time.Minute); st != "completed" {
		t.Fatalf("backfill %s", st)
	}
	var comp struct {
		Posts, PostsComplete, PostsGated, CommentsReported, CommentsCollected int64
	}
	var cb []byte
	if err := s.DB.QueryRow(ctx, `SELECT completeness FROM jobs WHERE job_id = $1`, reg.BackfillJob.ID).Scan(&cb); err != nil {
		t.Fatal(err)
	}
	var cm map[string]int64
	_ = json.Unmarshal(cb, &cm)
	comp.Posts, comp.PostsComplete, comp.PostsGated = cm["posts"], cm["posts_complete"], cm["posts_gated"]
	if comp.Posts == 0 || comp.PostsGated == 0 || cm["comments_collected"] == 0 {
		t.Fatalf("backfill completeness %s", cb)
	}
	var refresh int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM post_refresh WHERE user_pk = $1`, p.PK).Scan(&refresh); err != nil || refresh == 0 {
		t.Fatalf("post refreshes %d %v", refresh, err)
	}

	// Between runs: two new posts, and new comments on a recent post.
	added, err := s.World.AddPosts(p.Username, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var recent int64
	if err := s.DB.QueryRow(ctx, `SELECT r.media_pk FROM post_refresh r JOIN posts p ON p.media_pk = r.media_pk
		WHERE r.user_pk = $1 AND p.comment_count > 0 ORDER BY r.taken_at DESC LIMIT 1`, p.PK).Scan(&recent); err != nil {
		t.Fatal(err)
	}
	if err := s.World.AddComments(recent, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE post_refresh SET next_refresh_at = now() WHERE media_pk = $1`, recent); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET next_run_at = now(), next_profile_read_at = now()`); err != nil {
		t.Fatal(err)
	}
	var job string
	deadline := time.Now().Add(30 * time.Second)
	for job == "" || job == reg.BackfillJob.ID.String() {
		_ = s.DB.QueryRow(ctx, `SELECT coalesce(last_job_id::text, '') FROM profile_schedule`).Scan(&job)
		if time.Now().After(deadline) {
			t.Fatal("the planner started no scheduled run")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st := s.WaitStatus(t, job, 3*time.Minute); st != "completed" {
		t.Fatalf("scheduled run %s", st)
	}
	var kind, mode string
	var lane int16
	var params []byte
	if err := s.DB.QueryRow(ctx, `SELECT kind, mode, lane, params FROM jobs WHERE job_id = $1`, job).Scan(&kind, &mode, &lane, &params); err != nil {
		t.Fatal(err)
	}
	var pm map[string]any
	_ = json.Unmarshal(params, &pm)
	if kind != dispatch.KindScheduled || mode != "incremental" || lane != 2 || pm["refresh_profile"] != true {
		t.Fatalf("scheduled job: %s %s lane %d params %s", kind, mode, lane, params)
	}
	since := time.Now().Add(-365 * 24 * time.Hour)
	truth, _ := s.World.Truth(p.Username, since)
	checkTruth(t, s, truth)
	for _, x := range added {
		var n int
		_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM posts WHERE media_pk = $1`, x.PK).Scan(&n)
		if n != 1 && !x.Gated {
			t.Errorf("new post %d not stored", x.PK)
		}
	}
	var refreshes int
	if err := s.DB.QueryRow(ctx, `SELECT refreshes FROM post_refresh WHERE media_pk = $1`, recent).Scan(&refreshes); err != nil || refreshes != 1 {
		t.Errorf("refreshes of the post %d %v", refreshes, err)
	}
	var reads int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM profile_schedule WHERE next_profile_read_at > now() + interval '23 hours'`).Scan(&reads); err != nil || reads != 1 {
		t.Errorf("next profile read not moved a day ahead: %d %v", reads, err)
	}
}
