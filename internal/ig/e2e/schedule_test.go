package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
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

// One planner tick starts every due profile's run, however many batches
// that takes: scheduler.planner_batch is a batch size, not a cap on how
// many profiles the schedule can hold.
func TestPlannerTakesEveryDueProfile(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) { c.Scheduler.PlannerBatch = 2 }})
	s.StartScheduler() // the finalizer completes the backfills
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	var backfills []string
	for _, p := range s.World.Profiles() {
		var reg struct {
			BackfillJob *dispatch.Job `json:"backfill_job"`
		}
		if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, &reg); code != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Username, code)
		}
		backfills = append(backfills, reg.BackfillJob.ID.String())
	}
	for _, j := range backfills {
		if st := s.WaitStatus(t, j, 3*time.Minute); st != "completed" {
			t.Fatalf("backfill %s", st)
		}
	}
	s.stop("scheduler") // one tick, by hand
	if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET next_run_at = now() - interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	planner := &scheduler.Planner{DB: s.DB, Dispatch: s.Dispatch, Cfg: s.Cfg, Redis: s.Redis}
	if err := planner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	n := len(s.World.Profiles())
	if got := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'scheduled'`); got != n || n <= 2 {
		t.Fatalf("one tick with batches of 2 started %d scheduled runs, want %d", got, n)
	}
	if due := count(t, s, `SELECT count(*) FROM profile_schedule WHERE next_run_at <= now()`); due != 0 {
		t.Fatalf("%d profiles still due", due)
	}
}

// While the daily budget holds backfill (p2), a new profile's backfill
// waits and an on-demand read (p0) still runs; when the budget lets go,
// the backfill completes. With a stretch, the planner spaces the next
// scheduled run that much further.
func TestBudgetHoldsBackfill(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Budget = config.Budget{Enabled: true, RequestsPerDay: 1_000_000, Interval: 200 * time.Millisecond, Stretch: 2, ProjectionSpan: time.Hour}
	}})
	ctx := context.Background()
	stateKey := redisx.KeyPrefix + s.Cfg.Kafka.TopicPrefix + "budget:state"
	hold := func(st ratelimit.BudgetState) {
		b, _ := json.Marshal(st)
		if err := s.Redis.Set(ctx, stateKey, b, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	hold(ratelimit.BudgetState{PauseP2: true, Stretch: 1, Explanation: "test"})
	time.Sleep(time.Second) // the crawler's gate reads it (every 200ms here)
	s.StartScheduler()      // the finalizer completes jobs

	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead, httpapi.ScopeJobsWrite)
	profiles := s.World.Profiles()
	var reg struct {
		BackfillJob *dispatch.Job `json:"backfill_job"`
	}
	if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": profiles[0].Username}, &reg); code != http.StatusCreated {
		t.Fatalf("register: %d", code)
	}
	onDemand := s.Crawl(t, profiles[1].Username, full, s.Limits(), nil)
	s.Wait(t, 2*time.Minute, onDemand)
	if posts := count(t, s, `SELECT count(*) FROM posts WHERE owner_pk = $1`, profiles[0].PK); posts != 0 {
		t.Fatalf("the held backfill stored %d posts", posts)
	}
	if st, _ := s.Jobs.Status(ctx, reg.BackfillJob.ID.String()); st.Pending == 0 {
		t.Fatalf("the held backfill has no pending task: %+v", st)
	}

	hold(ratelimit.BudgetState{Stretch: 2})
	if st := s.WaitStatus(t, reg.BackfillJob.ID.String(), 2*time.Minute); st != "completed" {
		t.Fatalf("backfill after the budget let go: %s", st)
	}
	s.stop("scheduler") // one tick, by hand
	if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET next_run_at = now() - interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	planner := &scheduler.Planner{DB: s.DB, Dispatch: s.Dispatch, Cfg: s.Cfg, Redis: s.Redis}
	if err := planner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var gap float64
	if err := s.DB.QueryRow(ctx, `SELECT extract(epoch FROM next_run_at - now()) / interval_seconds FROM profile_schedule`).Scan(&gap); err != nil {
		t.Fatal(err)
	}
	if j := s.Cfg.IG.Schedule.Jitter; gap < 2*(1-j)-0.01 || gap > 2*(1+j)+0.01 {
		t.Fatalf("next run in %.2f intervals, want about 2 (the budget's stretch)", gap)
	}
}

// A profile pinned to the hot tier is read again and again within minutes
// (here seconds); the API shows and changes its placement: a custom
// interval in minutes, back to auto; a tier with an interval, or an
// interval over 30 days, is refused (TestPlace covers min_interval).
func TestScheduleTiers(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Schedule.MinInterval = time.Second
		c.IG.Schedule.Tiers.Hot = 2 * time.Second
		c.IG.Schedule.Jitter = 0
	}})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	p := s.World.Profiles()[0]
	var reg struct {
		BackfillJob *dispatch.Job `json:"backfill_job"`
	}
	if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username, "schedule": map[string]any{"tier": "hot"}}, &reg); code != http.StatusCreated {
		t.Fatalf("register hot: %d", code)
	}
	if st := s.WaitStatus(t, reg.BackfillJob.ID.String(), 3*time.Minute); st != "completed" {
		t.Fatalf("backfill %s", st)
	}
	runs := func() int {
		var n int
		_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE kind = 'scheduled' AND status = 'completed'`).Scan(&n)
		return n
	}
	for deadline := time.Now().Add(time.Minute); runs() < 3; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d scheduled runs of a hot profile in a minute", runs())
		}
	}
	var sc api.Schedule
	if code := s.Do(t, key, "PATCH", "/v1/profiles/"+p.Username, map[string]any{"schedule": map[string]any{"interval_minutes": 30}}, &sc); code != http.StatusOK ||
		sc.Tier != dispatch.TierCustom || sc.IntervalSeconds != 1800 || sc.Adaptive {
		t.Fatalf("custom: %d %+v", code, sc)
	}
	if code := s.Do(t, key, "PATCH", "/v1/profiles/"+p.Username, map[string]any{"schedule": map[string]any{"tier": "auto"}}, &sc); code != http.StatusOK ||
		!sc.Adaptive {
		t.Fatalf("auto: %d %+v", code, sc)
	}
	// The tierer places it by its posting rate at once (its pk is known).
	for deadline := time.Now().Add(10 * time.Second); sc.Tier != dispatch.TierWarm && sc.Tier != dispatch.TierLongTail; time.Sleep(200 * time.Millisecond) {
		s.Do(t, key, "GET", "/v1/profiles/"+p.Username, nil, &struct {
			Schedule *api.Schedule `json:"schedule"`
		}{&sc})
		if time.Now().After(deadline) {
			t.Fatalf("not placed: %+v", sc)
		}
	}
	if code := s.Do(t, key, "PATCH", "/v1/profiles/"+p.Username, map[string]any{"schedule": map[string]any{"interval_hours": 1, "tier": "hot"}}, nil); code != http.StatusBadRequest {
		t.Fatalf("a tier and an interval: %d", code)
	}
	if code := s.Do(t, key, "PATCH", "/v1/profiles/"+p.Username, map[string]any{"schedule": map[string]any{"interval_hours": 721}}, nil); code != http.StatusBadRequest {
		t.Fatalf("over 30 days: %d", code)
	}
}
