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

// Each post's comment coverage is written when its job ends (REQ-008 fix
// 15): after a backfill every visible post is complete, with its visible
// top-level comments counted, and each walled post is stored unavailable
// (login_wall) with no comments read. A scheduled refresh that reads the
// comments since the last read keeps the post complete and counts the new
// ones.
func TestPostCoverage(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	p := s.World.Profiles()[1]
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
	truth, _ := s.World.Truth(p.Username, time.Now().Add(-time.Duration(s.Limits().MaxAgeSeconds)*time.Second))
	type cov struct {
		status, state string
		reason        *string
		collected     *int64
	}
	read := func(pk int64) (cov, bool) {
		var c cov
		err := s.DB.QueryRow(ctx, `SELECT status, status_reason, comments_state, comments_collected FROM posts WHERE media_pk = $1`, pk).
			Scan(&c.status, &c.reason, &c.state, &c.collected)
		return c, err == nil
	}
	walled := int64(0)
	for _, pt := range truth.Posts {
		c, ok := read(pt.PK)
		switch {
		case !ok:
			t.Errorf("post %d not stored", pt.PK)
		case pt.Gated:
			walled++
			if c.status != "unavailable" || c.reason == nil || *c.reason != "login_wall" || c.state != "none" {
				t.Errorf("walled post %d: %s %v %s, want unavailable (login_wall), none", pt.PK, c.status, show(c.reason), c.state)
			}
		case c.state != "complete" || c.collected == nil || *c.collected != int64(len(pt.Comments)):
			t.Errorf("post %d: %s, %v collected, want complete with %d", pt.PK, c.state, show(c.collected), len(pt.Comments))
		}
	}
	var published int64
	if err := s.DB.QueryRow(ctx, `SELECT coalesce((counters->>'posts_walled_published')::bigint, 0) FROM jobs WHERE job_id = $1`, job).
		Scan(&published); err != nil || walled == 0 || published != walled {
		t.Errorf("walled posts published %d (%v), want %d", published, err, walled)
	}

	// New comments on the newest refreshed post; a scheduled run reads them.
	var recent int64
	if err := s.DB.QueryRow(ctx, `SELECT r.media_pk FROM post_refresh r JOIN posts p ON p.media_pk = r.media_pk
		WHERE r.user_pk = $1 AND p.comment_count > 0 ORDER BY r.taken_at DESC LIMIT 1`, p.PK).Scan(&recent); err != nil {
		t.Fatalf("a refreshed post: %v", err)
	}
	before, _ := read(recent)
	if err := s.World.AddComments(recent, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `UPDATE post_refresh SET next_refresh_at = now() WHERE media_pk = $1`, recent); err != nil {
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
	after, _ := read(recent)
	if after.state != "complete" || before.collected == nil || after.collected == nil || *after.collected != *before.collected+3 {
		t.Errorf("refreshed post: %s, %v collected (was %v), want complete with 3 more", after.state, show(after.collected),
			show(before.collected))
	}
}
