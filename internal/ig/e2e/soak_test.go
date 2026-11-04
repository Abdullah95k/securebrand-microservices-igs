package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
)

// TestSoak runs igs's scheduled pipeline for IGS_SOAK_DURATION (default
// 20m) while the fakeig world keeps changing (new posts, comments on older
// posts, like counts) under occasional rate limits, and a component
// restarts every IGS_SOAK_RESTART_EVERY (default 90s; 0 = never): the
// crawler, the persister, or the scheduler (planner and finalizer).
// Profiles re-run every 30s instead of every 3 hours. It fails on a job
// that ends failed, partial or cancelled, one running for over 10 minutes,
// and goroutine growth. At the end, a full read of every profile must leave
// the store exactly like the world (checkTruth: every post with its counts,
// every top-level comment with its text and reply count), and no job may
// be left in the finalize set. Opt-in: IGS_SOAK=1 (make igs-soak);
// IGS_SOAK_PROFILES (default 6); IGS_SOAK_OUT writes the report.
func TestSoak(t *testing.T) {
	if os.Getenv("IGS_SOAK") == "" {
		t.Skip("set IGS_SOAK=1 (make igs-soak)")
	}
	duration := envDuration("IGS_SOAK_DURATION", 20*time.Minute)
	restartEvery := envDuration("IGS_SOAK_RESTART_EVERY", 90*time.Second)
	sp := spec()
	sp.Seed, sp.Profiles, sp.PostsPerProfile = 41, envInt("IGS_SOAK_PROFILES", 6), 20
	s := Start(t, Options{Spec: sp, Faults: fakeig.Faults{RateLimitEvery: 37}, CrawlerDrain: 50 * time.Millisecond,
		Config: func(c *config.Config) {
			c.IG.Schedule.PostRefreshPerRun = 10
		}})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeJobsWrite, httpapi.ScopeDataRead)
	profiles := s.World.Profiles()
	for _, p := range profiles {
		if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, nil); code != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Username, code)
		}
	}
	// Re-read every 30s instead of every 3 hours, the profile query too.
	reschedule := func() {
		if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET interval_seconds = 30,
			next_run_at = least(next_run_at, now() + random() * interval '30 seconds'), next_profile_read_at = now()`); err != nil {
			t.Fatal(err)
		}
	}
	reschedule()

	rng := rand.New(rand.NewPCG(41, 1))
	mutations := map[string]int{}
	restarts := map[string]int{}
	var maxGoroutines, baseGoroutines int
	var maxHeap uint64
	var report strings.Builder
	fmt.Fprintf(&report, "# igs soak: %d profiles, %s, restart every %s\n\n| t | goroutines | heap in use MB | redis keys | jobs done | running | oldest running |\n|---|---|---|---|---|---|---|\n",
		len(profiles), duration, restartEvery)

	mutate := func() {
		p := profiles[rng.IntN(len(profiles))]
		switch n := rng.IntN(10); {
		case n < 3:
			if _, err := s.World.AddPosts(p.Username, 1, time.Minute); err != nil {
				t.Fatal(err)
			}
			mutations["new posts"]++
		case n < 7:
			post := p.Posts[rng.IntN(min(len(p.Posts), 10))]
			if post.Gated {
				return
			}
			if err := s.World.AddComments(post.PK, 1+rng.IntN(4)); err != nil {
				t.Fatal(err)
			}
			mutations["comments on recent posts"]++
		default:
			post := p.Posts[rng.IntN(len(p.Posts))]
			if err := s.World.SetLikes(post.PK, post.Likes+int64(1+rng.IntN(50))); err != nil {
				t.Fatal(err)
			}
			mutations["like counts"]++
		}
	}
	sampleHealth := func(elapsed time.Duration) {
		var running, done int
		var oldest time.Duration
		rows, err := s.DB.Query(ctx, `SELECT job_id::text, kind, status, now() - created_at, coalesce(counters::text, ''),
			coalesce(completeness::text, ''), coalesce(error::text, '') FROM jobs`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, kind, st, counters, comp, jerr string
			var age time.Duration
			if err := rows.Scan(&id, &kind, &st, &age, &counters, &comp, &jerr); err != nil {
				t.Fatal(err)
			}
			switch st {
			case "queued", "running", "finalizing", "cancelling":
				running++
				oldest = max(oldest, age)
			case "failed", "partial", "cancelled":
				rows.Close()
				t.Fatalf("%s job %s ended %s (after %s): counters %s, completeness %s, error %s", kind, id, st, elapsed, counters, comp, jerr)
			default:
				done++
			}
		}
		rows.Close()
		if oldest > 10*time.Minute {
			t.Fatalf("a job has been running for %s", oldest)
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		g := runtime.NumGoroutine()
		maxGoroutines, maxHeap = max(maxGoroutines, g), max(maxHeap, m.HeapInuse)
		if baseGoroutines == 0 && elapsed > 2*time.Minute {
			baseGoroutines = g
		}
		keys, _ := s.Redis.DBSize(ctx).Result()
		fmt.Fprintf(&report, "| %s | %d | %.0f | %d | %d | %d | %s |\n", elapsed.Round(time.Second), g, float64(m.HeapInuse)/1e6,
			keys, done, running, oldest.Round(time.Second))
	}

	start := time.Now()
	nextMutation, nextRestart, nextSample := start, start.Add(restartEvery), start
	for time.Since(start) < duration {
		now := time.Now()
		if now.After(nextMutation) {
			mutate()
			nextMutation = now.Add(time.Duration(1+rng.IntN(4)) * time.Second)
		}
		if restartEvery > 0 && now.After(nextRestart) {
			switch rng.IntN(4) {
			case 0, 1:
				s.RestartCrawler(t)
				restarts["crawler"]++
			case 2:
				s.RestartPersister(t)
				restarts["persister"]++
			default:
				s.stop("scheduler")
				s.StartScheduler()
				restarts["scheduler"]++
			}
			nextRestart = now.Add(restartEvery)
		}
		if now.After(nextSample) {
			reschedule() // a run moves next_run_at to its 30s interval; keep the profile query due too
			sampleHealth(now.Sub(start))
			nextSample = now.Add(30 * time.Second)
		}
		time.Sleep(time.Second)
	}

	// Settle: no more schedules; let running jobs finish.
	if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET enabled = false`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Minute)
	for count(t, s, `SELECT count(*) FROM jobs WHERE status IN ('queued','running','finalizing','cancelling')`) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("running jobs did not finish in 10 minutes")
		}
		time.Sleep(time.Second)
	}
	sampleHealth(time.Since(start))
	// One full read of every profile reconciles what the scheduled runs
	// could not see (comments on posts outside the refresh window).
	lim := s.Limits()
	var ids []string
	for _, p := range profiles {
		ids = append(ids, s.Crawl(t, p.Username, full, lim, nil))
	}
	s.Wait(t, 5*time.Minute, ids...)
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	for _, p := range profiles {
		truth, _ := s.World.Truth(p.Username, since)
		checkTruth(t, s, truth)
	}
	if t.Failed() {
		t.Log("\n" + report.String())
		return
	}
	// The final reads were tracked jobs too (no finalizer row for them):
	// only the scheduled jobs' set entries must be gone.
	waitUntil := time.Now().Add(time.Minute)
	for {
		left, _ := s.Redis.SMembers(ctx, jobs.FinalizeSet).Result()
		scheduled := 0
		for _, id := range left {
			if !strings.HasPrefix(id, "job-") {
				scheduled++
			}
		}
		if scheduled == 0 {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatalf("%d scheduled jobs left in the finalize set", scheduled)
		}
		time.Sleep(time.Second)
	}
	g := runtime.NumGoroutine()
	if baseGoroutines > 0 && g > baseGoroutines*3/2+50 {
		t.Fatalf("goroutines grew from %d to %d", baseGoroutines, g)
	}
	jobsRun := count(t, s, `SELECT count(*) FROM jobs`)
	posts := count(t, s, `SELECT count(*) FROM posts WHERE kind <> 'highlight'`)
	comments := count(t, s, `SELECT count(*) FROM comments`)
	fmt.Fprintf(&report, "\n- jobs: %d; posts stored %d, comments %d\n- mutations: %v\n- restarts: %v\n"+
		"- goroutines: %d after 2 minutes, %d max, %d at the end; heap in use at most %.0f MB\n",
		jobsRun, posts, comments, mutations, restarts, baseGoroutines, maxGoroutines, g, float64(maxHeap)/1e6)
	t.Log("\n" + report.String())
	if out := os.Getenv("IGS_SOAK_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
