// Package soak drives igs's soak against fakeig (REQ-008 fix 12): it
// registers every fakeig profile through igs's API, keeps them on a short
// schedule while fakeig's world changes and crawler pods and Valkey are
// restarted, then reads every profile in full and compares igs's store
// with fakeig's ground truth. cmd/igs-soak runs it in the namespace
// igs-soak (deploy/soak); an e2e test runs it for a minute.
package soak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
)

// Options configure a soak.
type Options struct {
	DB     *pgxpool.Pool // igs's store, schema ig
	API    string        // igs-api's base URL
	FakeIG string        // fakeig's base URL (fakeig -admin)
	// Duration is the driving phase: schedules, world changes, chaos.
	Duration time.Duration
	// Interval is every profile's scheduled interval during the soak.
	Interval    time.Duration
	MutateEvery time.Duration
	SampleEvery time.Duration
	// KillEvery kills a crawler pod this often; ValkeyAt restarts Valkey
	// once, this far in. Both need Chaos; 0 is never.
	KillEvery time.Duration
	ValkeyAt  time.Duration
	Chaos     Chaos
	// Settle bounds the wait for running jobs once schedules stop;
	// FinalWait the final full reads.
	Settle    time.Duration
	FinalWait time.Duration
	// MaxAge is how far back a full read goes (ig.limits.max_age_days).
	MaxAge time.Duration
	Log    io.Writer
	HTTP   *http.Client
}

// Report is the soak's outcome.
type Report struct {
	Profiles   int
	Mutations  map[string]int
	Chaos      map[string]int
	ChaosFails []string
	// BadJobs are jobs that ended failed, partial or cancelled.
	BadJobs []string
	Jobs    map[string]int // by status, at the end
	// OldestRunning is the longest a job ran, as sampled.
	OldestRunning time.Duration
	Diffs         []string
	Posts         int64
	Comments      int64
	Took          time.Duration
}

// Pass reports whether the store ended equal to the truth and no job
// ended badly. Memory and goroutines are judged from Prometheus
// (docs/runbooks/igs-soak.md).
func (r Report) Pass() bool {
	return len(r.Diffs) == 0 && len(r.BadJobs) == 0 && len(r.ChaosFails) == 0
}

// Lines is the report, the soak's last log lines.
func (r Report) Lines() []string {
	verdict := "PASS"
	if !r.Pass() {
		verdict = "FAIL"
	}
	out := []string{
		fmt.Sprintf("igs soak: %s after %s, %d profiles", verdict, r.Took.Round(time.Second), r.Profiles),
		fmt.Sprintf("  world changes: %s", counts(r.Mutations)),
		fmt.Sprintf("  chaos: %s", counts(r.Chaos)),
		fmt.Sprintf("  jobs: %s; longest running %s", counts(r.Jobs), r.OldestRunning.Round(time.Second)),
		fmt.Sprintf("  stored: %d posts, %d comments", r.Posts, r.Comments),
		fmt.Sprintf("  store against fakeig's truth: %d differences", len(r.Diffs)),
	}
	for i, d := range r.Diffs {
		if i == 20 {
			out = append(out, fmt.Sprintf("    ... and %d more", len(r.Diffs)-20))
			break
		}
		out = append(out, "    "+d)
	}
	out = append(out, fmt.Sprintf("  jobs that ended badly: %d", len(r.BadJobs)))
	for i, j := range r.BadJobs {
		if i == 10 {
			out = append(out, fmt.Sprintf("    ... and %d more", len(r.BadJobs)-10))
			break
		}
		out = append(out, "    "+j)
	}
	for _, f := range r.ChaosFails {
		out = append(out, "  chaos failed: "+f)
	}
	return append(out, "igs soak: "+verdict)
}

func counts(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, m[k])
	}
	return strings.Join(parts, ", ")
}

type driver struct {
	o   Options
	key string
	r   Report
}

func (d *driver) logf(format string, args ...any) {
	if d.o.Log != nil {
		fmt.Fprintf(d.o.Log, "%s "+format+"\n", append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
	}
}

func (d *driver) call(ctx context.Context, method, url, key string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.o.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode/100 == 2 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

// Run drives the soak and returns its report; an error means the soak
// could not run (igs or fakeig unreachable), not that it failed.
func Run(ctx context.Context, o Options) (Report, error) {
	start := time.Now()
	r, err := run(ctx, o)
	r.Took = time.Since(start)
	return r, err
}

func run(ctx context.Context, o Options) (Report, error) {
	if o.HTTP == nil {
		o.HTTP = &http.Client{Timeout: time.Minute}
	}
	o.MutateEvery = max(o.MutateEvery, 100*time.Millisecond)
	o.SampleEvery = max(o.SampleEvery, time.Second)
	d := &driver{o: o, r: Report{Mutations: map[string]int{}, Chaos: map[string]int{}, Jobs: map[string]int{}}}

	key, _, err := httpapi.CreateClient(ctx, o.DB, "igs-soak", []string{api.ScopeProfilesWrite, httpapi.ScopeJobsWrite,
		httpapi.ScopeDataRead}, 0)
	if err != nil {
		return d.r, fmt.Errorf("soak: API client: %w", err)
	}
	d.key = key
	var names []string
	if _, err := d.call(ctx, http.MethodGet, o.FakeIG+fakeig.AdminPrefix+"profiles", "", nil, &names); err != nil || len(names) == 0 {
		return d.r, fmt.Errorf("soak: fakeig's profiles (is it run with -admin?): %v", err)
	}
	d.r.Profiles = len(names)
	for _, n := range names {
		code, err := d.call(ctx, http.MethodPost, o.API+"/v1/profiles", key, map[string]any{"profile": n}, nil)
		if err != nil || (code != http.StatusCreated && code != http.StatusOK) {
			return d.r, fmt.Errorf("soak: register %s: %d %v", n, code, err)
		}
	}
	d.logf("registered %d profiles; driving for %s at a %s interval", len(names), o.Duration, o.Interval)

	if err := d.drive(ctx); err != nil {
		return d.r, err
	}
	if err := d.settle(ctx); err != nil {
		return d.r, err
	}
	if err := d.finalReads(ctx, names); err != nil {
		return d.r, err
	}
	return d.r, d.check(ctx)
}

// drive keeps the profiles on the soak's schedule while fakeig changes
// and the chaos runs, sampling the jobs as it goes.
func (d *driver) drive(ctx context.Context) error {
	o := d.o
	start := time.Now()
	nextMutate, nextSample := start, start
	nextKill := start.Add(o.KillEvery)
	valkeyDone := o.ValkeyAt <= 0 || o.Chaos == nil
	tick := time.NewTicker(min(o.MutateEvery, time.Second))
	defer tick.Stop()
	for time.Since(start) < o.Duration {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		now := time.Now()
		if !now.Before(nextMutate) {
			var changes map[string]int
			if _, err := d.call(ctx, http.MethodPost, o.FakeIG+fakeig.AdminPrefix+"mutate?n=1", "", nil, &changes); err != nil {
				d.logf("world change failed: %v", err)
			}
			for k, v := range changes {
				d.r.Mutations[k] += v
			}
			nextMutate = now.Add(o.MutateEvery)
		}
		if o.Chaos != nil && o.KillEvery > 0 && !now.Before(nextKill) {
			name, err := o.Chaos.KillCrawler(ctx)
			d.chaos("crawler pod killed", name, err)
			nextKill = now.Add(o.KillEvery)
		}
		if !valkeyDone && now.Sub(start) >= o.ValkeyAt {
			d.chaos("valkey restarted", "valkey", o.Chaos.RestartValkey(ctx))
			valkeyDone = true
		}
		if !now.Before(nextSample) {
			if err := d.sample(ctx, now.Sub(start)); err != nil {
				return err
			}
			nextSample = now.Add(o.SampleEvery)
		}
	}
	return nil
}

func (d *driver) chaos(what, target string, err error) {
	if err != nil {
		d.r.ChaosFails = append(d.r.ChaosFails, fmt.Sprintf("%s: %v", what, err))
		d.logf("chaos: %s failed: %v", what, err)
		return
	}
	d.r.Chaos[what]++
	d.logf("chaos: %s (%s)", what, target)
}

// sample keeps every profile on the soak's interval (the tier job resets
// it) and logs the jobs; a job that ended badly is recorded.
func (d *driver) sample(ctx context.Context, elapsed time.Duration) error {
	secs := int(d.o.Interval.Seconds())
	if _, err := d.o.DB.Exec(ctx, `UPDATE profile_schedule SET interval_seconds = $1,
		next_run_at = least(next_run_at, now() + random() * make_interval(secs => $1)) WHERE enabled AND interval_seconds <> $1`,
		secs); err != nil {
		return fmt.Errorf("soak: schedule: %w", err)
	}
	if err := d.jobs(ctx); err != nil {
		return err
	}
	var running int
	var oldest time.Duration
	if err := d.o.DB.QueryRow(ctx, `SELECT count(*), coalesce(max(now() - created_at), '0') FROM jobs
		WHERE status IN ('queued', 'running', 'finalizing', 'cancelling')`).Scan(&running, &oldest); err != nil {
		return fmt.Errorf("soak: jobs: %w", err)
	}
	d.r.OldestRunning = max(d.r.OldestRunning, oldest)
	d.logf("t=%s jobs %s; %d running, oldest %s; world changes %s", elapsed.Round(time.Second), counts(d.r.Jobs), running,
		oldest.Round(time.Second), counts(d.r.Mutations))
	return nil
}

// jobs counts the jobs by status and records those that ended badly.
func (d *driver) jobs(ctx context.Context) error {
	rows, err := d.o.DB.Query(ctx, `SELECT job_id::text, kind, status, coalesce(error::text, '') FROM jobs`)
	if err != nil {
		return fmt.Errorf("soak: jobs: %w", err)
	}
	defer rows.Close()
	byStatus := map[string]int{}
	var bad []string
	for rows.Next() {
		var id, kind, st, jerr string
		if err := rows.Scan(&id, &kind, &st, &jerr); err != nil {
			return err
		}
		byStatus[st]++
		if st == "failed" || st == "partial" || st == "cancelled" {
			bad = append(bad, fmt.Sprintf("%s job %s ended %s %s", kind, id, st, jerr))
		}
	}
	d.r.Jobs, d.r.BadJobs = byStatus, bad
	return rows.Err()
}

// settle stops the schedules and waits for running jobs to end.
func (d *driver) settle(ctx context.Context) error {
	if _, err := d.o.DB.Exec(ctx, `UPDATE profile_schedule SET enabled = false`); err != nil {
		return fmt.Errorf("soak: stop schedules: %w", err)
	}
	deadline := time.Now().Add(d.o.Settle)
	for {
		var running int
		if err := d.o.DB.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued', 'running', 'finalizing', 'cancelling')`).
			Scan(&running); err != nil {
			return err
		}
		if running == 0 {
			d.logf("schedules stopped; no job running")
			return nil
		}
		if time.Now().After(deadline) {
			d.r.BadJobs = append(d.r.BadJobs, fmt.Sprintf("%d jobs still running %s after the schedules stopped", running, d.o.Settle))
			return nil
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

// finalReads reads every profile in full, as an API client would, so the
// store can be compared with the world (scheduled runs do not see
// comments on posts past the refresh window).
func (d *driver) finalReads(ctx context.Context, names []string) error {
	ids := make([]string, 0, len(names))
	for _, n := range names {
		var j struct {
			ID string `json:"job_id"`
		}
		code, err := d.call(ctx, http.MethodPost, d.o.API+"/v1/jobs", d.key, map[string]any{"profile": n, "mode": "full"}, &j)
		if err != nil || code/100 != 2 || j.ID == "" {
			return fmt.Errorf("soak: full read of %s: %d %v", n, code, err)
		}
		ids = append(ids, j.ID)
	}
	d.logf("reading %d profiles in full", len(ids))
	deadline := time.Now().Add(d.o.FinalWait)
	for {
		var open int
		if err := d.o.DB.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE job_id::text = ANY($1)
			AND status NOT IN ('completed', 'partial', 'failed', 'cancelled')`, ids).Scan(&open); err != nil {
			return err
		}
		if open == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("soak: %d full reads still running after %s", open, d.o.FinalWait)
		}
		if err := sleep(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

// check compares the store with fakeig's truth.
func (d *driver) check(ctx context.Context) error {
	if err := d.jobs(ctx); err != nil {
		return err
	}
	since := time.Now().Add(-d.o.MaxAge).UTC().Format(time.RFC3339)
	var truth []fakeig.ProfileTruth
	if _, err := d.call(ctx, http.MethodGet, d.o.FakeIG+fakeig.AdminPrefix+"truth?since="+url.QueryEscape(since), "", nil,
		&truth); err != nil || len(truth) == 0 {
		return fmt.Errorf("soak: fakeig's truth: %v", errors.Join(err, errEmpty(len(truth))))
	}
	for _, t := range truth {
		diffs, err := Compare(ctx, d.o.DB, t)
		if err != nil {
			return err
		}
		d.r.Diffs = append(d.r.Diffs, diffs...)
	}
	_ = d.o.DB.QueryRow(ctx, `SELECT (SELECT count(*) FROM posts WHERE kind <> 'highlight'), (SELECT count(*) FROM comments)`).
		Scan(&d.r.Posts, &d.r.Comments)
	return nil
}

// Ready reports whether igs's API and fakeig's admin answer.
func Ready(ctx context.Context, apiURL, fakeURL string) error {
	c := &http.Client{Timeout: 10 * time.Second}
	for _, u := range []string{apiURL + "/v1/profiles", fakeURL + fakeig.AdminPrefix + "profiles"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		resp, err := c.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		// The API answers 401 without a key: it is up.
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%s: %d", u, resp.StatusCode)
		}
	}
	return nil
}

func errEmpty(n int) error {
	if n == 0 {
		return errors.New("no profiles")
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
