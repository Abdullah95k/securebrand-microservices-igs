package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
)

type page[T any] struct {
	Posts      []T    `json:"posts"`
	Comments   []T    `json:"comments"`
	NextCursor string `json:"next_cursor"`
}

// A job started through the API crawls the profile; the API then serves
// its profile, every post (paged), comments with author identity only for
// pii:read, metrics and media. Jobs are private to their client, an
// idempotency key returns the same job, the next job runs incremental,
// and a profile id igs never read is refused.
func TestAPIJobsAndReads(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	key := s.Key(t, httpapi.ScopeJobsWrite, httpapi.ScopeDataRead)
	pii := s.Key(t, httpapi.ScopeDataRead, httpapi.ScopePIIRead)
	other := s.Key(t, httpapi.ScopeJobsWrite, httpapi.ScopeDataRead)
	p := s.World.Profiles()[0]

	var j dispatch.Job
	if code := s.Do(t, key, "POST", "/v1/jobs", map[string]any{"profile": "@" + p.Username}, &j, "Idempotency-Key", "k1"); code != http.StatusAccepted {
		t.Fatalf("create job: %d", code)
	}
	if j.Mode != "full" || j.ProfileRef != "u:"+p.Username {
		t.Fatalf("job %+v", j)
	}
	var again dispatch.Job
	if code := s.Do(t, key, "POST", "/v1/jobs", map[string]any{"profile": p.Username}, &again, "Idempotency-Key", "k1"); code != http.StatusOK || again.ID != j.ID {
		t.Fatalf("idempotent create: %d %s", code, again.ID)
	}
	s.Wait(t, 3*time.Minute, j.ID.String())
	if code := s.Do(t, other, "GET", "/v1/jobs/"+j.ID.String(), nil, nil); code != http.StatusNotFound {
		t.Fatalf("another client's job: %d", code)
	}
	var got dispatch.Job
	if code := s.Do(t, key, "GET", "/v1/jobs/"+j.ID.String(), nil, &got); code != http.StatusOK || got.Counters["profiles"] != 1 {
		t.Fatalf("get job: %d %+v", code, got)
	}

	var prof api.Profile
	if code := s.Do(t, key, "GET", "/v1/profiles/"+p.Username, nil, &prof); code != http.StatusOK || prof.FollowerCount == nil || *prof.FollowerCount != p.Followers {
		t.Fatalf("profile: %d %+v", code, prof)
	}
	since := time.Now().Add(-365 * 24 * time.Hour)
	truth, _ := s.World.Truth(p.Username, since)
	want := 0
	for _, pt := range truth.Posts {
		if !pt.Gated {
			want++
		}
	}
	var posts []api.Post
	cursor := ""
	for range 20 {
		var pg page[api.Post]
		if code := s.Do(t, key, "GET", "/v1/profiles/"+p.Username+"/posts?limit=7&cursor="+cursor, nil, &pg); code != http.StatusOK {
			t.Fatalf("posts: %d", code)
		}
		posts = append(posts, pg.Posts...)
		if pg.NextCursor == "" {
			break
		}
		cursor = pg.NextCursor
	}
	if len(posts) != want {
		t.Fatalf("paged %d posts, want %d", len(posts), want)
	}
	for i := 1; i < len(posts); i++ {
		if posts[i].TakenAt.After(posts[i-1].TakenAt) {
			t.Fatalf("posts out of order at %d", i)
		}
	}
	var withComments api.Post
	for _, x := range posts {
		if x.CommentCount != nil && *x.CommentCount > 0 {
			withComments = x
			break
		}
	}
	var anon, named page[api.Comment]
	s.Do(t, key, "GET", "/v1/posts/"+withComments.Shortcode+"/comments", nil, &anon)
	s.Do(t, pii, "GET", "/v1/posts/"+withComments.Shortcode+"/comments", nil, &named)
	if len(anon.Comments) == 0 || anon.Comments[0].AuthorUsername != nil || anon.Comments[0].AuthorKey == "" {
		t.Fatalf("comments without pii:read: %+v", anon.Comments)
	}
	if len(named.Comments) == 0 || named.Comments[0].AuthorUsername == nil {
		t.Fatalf("comments with pii:read: %+v", named.Comments)
	}
	var media struct {
		Media []api.Media `json:"media"`
	}
	if code := s.Do(t, key, "GET", "/v1/posts/"+withComments.Shortcode+"/media", nil, &media); code != http.StatusOK || len(media.Media) == 0 {
		t.Fatalf("media: %d %d", code, len(media.Media))
	}
	var metrics struct {
		Metrics []map[string]any `json:"metrics"`
	}
	if code := s.Do(t, key, "GET", "/v1/posts/"+withComments.Shortcode+"/metrics", nil, &metrics); code != http.StatusOK || len(metrics.Metrics) == 0 {
		t.Fatalf("metrics: %d %d", code, len(metrics.Metrics))
	}

	var inc dispatch.Job
	if code := s.Do(t, key, "POST", "/v1/jobs", map[string]any{"profile": p.Username}, &inc); code != http.StatusAccepted || inc.Mode != "incremental" {
		t.Fatalf("second job: %d mode %q", code, inc.Mode)
	}
	s.Wait(t, 3*time.Minute, inc.ID.String())
	if code := s.Do(t, key, "POST", "/v1/jobs", map[string]any{"profile": "id:999"}, nil); code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown id: %d", code)
	}
	if code := s.Do(t, key, "POST", "/v1/jobs", map[string]any{"profile": p.Username, "max_age_days": 4000}, nil); code != http.StatusBadRequest {
		t.Fatalf("age over the cap: %d", code)
	}
	if code := s.Do(t, pii, "POST", "/v1/jobs", map[string]any{"profile": p.Username}, nil); code != http.StatusForbidden {
		t.Fatalf("no jobs:write: %d", code)
	}
}

// Registering a profile schedules it and starts one backfill; registering
// it again starts nothing; each client sees its own registrations; the
// schedule ends with the last registration.
func TestAPIRegistration(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	a := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	b := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	p := s.World.Profiles()[1]
	var reg struct {
		ProfileRef  string        `json:"profile_ref"`
		BackfillJob *dispatch.Job `json:"backfill_job"`
	}
	if code := s.Do(t, a, "POST", "/v1/profiles", map[string]any{"profile": p.Username, "max_age_days": 200}, &reg); code != http.StatusCreated || reg.BackfillJob == nil {
		t.Fatalf("register: %d %+v", code, reg)
	}
	if reg.BackfillJob.Kind != dispatch.KindBackfill || reg.BackfillJob.Lane != "p2" {
		t.Fatalf("backfill %+v", reg.BackfillJob)
	}
	s.Wait(t, 3*time.Minute, reg.BackfillJob.ID.String())
	reg.BackfillJob = nil
	if code := s.Do(t, b, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, &reg); code != http.StatusOK || reg.BackfillJob != nil {
		t.Fatalf("second registration: %d %+v", code, reg.BackfillJob)
	}
	var list struct {
		Profiles []api.Schedule `json:"profiles"`
	}
	s.Do(t, a, "GET", "/v1/profiles", nil, &list)
	if len(list.Profiles) != 1 || list.Profiles[0].UserPK == nil || *list.Profiles[0].UserPK != p.PK {
		t.Fatalf("a's profiles: %+v", list.Profiles)
	}
	// The backfill job (no client) is visible to the registrants.
	var j dispatch.Job
	if code := s.Do(t, b, "GET", "/v1/jobs/"+list.Profiles[0].LastJobID.String(), nil, &j); code != http.StatusOK {
		t.Fatalf("scheduled job for a registrant: %d", code)
	}
	var sc api.Schedule
	if code := s.Do(t, a, "PATCH", "/v1/profiles/"+p.Username, map[string]any{"schedule": map[string]any{"interval_hours": 6}}, &sc); code != http.StatusOK || sc.IntervalSeconds != 6*3600 {
		t.Fatalf("patch: %d %+v", code, sc)
	}
	if code := s.Do(t, a, "DELETE", "/v1/profiles/"+p.Username, nil, nil); code != http.StatusNoContent {
		t.Fatalf("delete a: %d", code)
	}
	if code := s.Do(t, a, "DELETE", "/v1/profiles/"+p.Username, nil, nil); code != http.StatusNotFound {
		t.Fatalf("delete a again: %d", code)
	}
	var n int
	_ = s.DB.QueryRow(t.Context(), `SELECT count(*) FROM profile_schedule`).Scan(&n)
	if n != 1 {
		t.Fatalf("schedule gone while b is registered: %d", n)
	}
	s.Do(t, b, "DELETE", "/v1/profiles/"+p.Username, nil, nil)
	_ = s.DB.QueryRow(t.Context(), `SELECT count(*) FROM profile_schedule`).Scan(&n)
	if n != 0 {
		t.Fatalf("schedule left after the last registration: %d", n)
	}

	admin := s.Key(t, httpapi.ScopeAdmin)
	var e api.Erasure
	if code := s.Do(t, admin, "POST", "/v1/erasure-requests", map[string]any{"subject_type": "profile", "username": p.Username}, &e); code != http.StatusAccepted || e.Status != "pending" {
		t.Fatalf("erasure: %d %+v", code, e)
	}
	if code := s.Do(t, a, "POST", "/v1/erasure-requests", map[string]any{"subject_type": "profile", "user_pk": p.PK}, nil); code != http.StatusForbidden {
		t.Fatalf("erasure without admin: %d", code)
	}
}

// A bulk registration schedules every listed profile (usernames, @names,
// links; duplicates once), starts their backfills, and reports invalid
// entries; a second run starts nothing and only updates. A paused list runs
// nothing until resumed.
func TestAPIBulkRegistration(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	ps := s.World.Profiles()
	entries := []string{ps[0].Username, "@" + ps[1].Username, "https://www.instagram.com/" + ps[2].Username + "/", ps[0].Username, "not a username!"}
	var res api.BulkResult
	if code := s.Do(t, key, "POST", "/v1/profiles/bulk", map[string]any{"profiles": entries, "max_age_days": 200}, &res); code != http.StatusOK {
		t.Fatalf("bulk: %d", code)
	}
	if res.Registered != 3 || res.Updated != 0 || res.Backfills != 3 || len(res.Invalid) != 1 || len(res.Failed) != 0 {
		t.Fatalf("bulk %+v", res)
	}
	var ids []string
	rows, err := s.DB.Query(t.Context(), `SELECT last_job_id::text FROM profile_schedule WHERE last_job_id IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 3 {
		t.Fatalf("%d backfills recorded", len(ids))
	}
	s.Wait(t, 3*time.Minute, ids...)
	since := time.Now().Add(-200 * 24 * time.Hour)
	for _, p := range ps[:3] {
		truth, _ := s.World.Truth(p.Username, since)
		checkTruth(t, s, truth)
	}
	res = api.BulkResult{}
	s.Do(t, key, "POST", "/v1/profiles/bulk", map[string]any{"profiles": entries[:3], "schedule": map[string]any{"enabled": false}}, &res)
	if res.Registered != 0 || res.Updated != 3 || res.Backfills != 0 {
		t.Fatalf("second bulk %+v", res)
	}
	var paused int
	_ = s.DB.QueryRow(t.Context(), `SELECT count(*) FROM profile_schedule WHERE NOT enabled`).Scan(&paused)
	if paused != 3 {
		t.Fatalf("%d profiles paused, want 3", paused)
	}
	if code := s.Do(t, key, "POST", "/v1/profiles/bulk", map[string]any{"profiles": make([]string, api.MaxBulkProfiles+1)}, nil); code != http.StatusBadRequest {
		t.Fatalf("an oversized list: %d", code)
	}
}
