package health

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func find(t *testing.T, checks []Check, name string) Check {
	t.Helper()
	for _, c := range checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no check %q in %+v", name, checks)
	return Check{}
}

// An empty store says there is nothing to collect. With profiles on the
// schedule, one overdue by its interval, a fifth of the last hour's jobs
// failed, no profile read for 3 hours and three of four exits resting, the
// report says so line by line, names what to do and counts as BAD; once the
// schedule is on time and a profile was read a minute ago, it is fine
// again.
func TestGather(t *testing.T) {
	db := testenv.PostgresIG(t)
	rdb := testenv.Redis(t)
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.IG.Proxy.Tiers = []config.ProxyTier{{Name: "isp", Kind: "static_list",
		Endpoints: []string{"http://a:1", "http://a:2", "http://a:3", "http://a:4"}}}
	src := Sources{DB: db, Redis: rdb, Cfg: cfg, Prefix: "t."}

	empty := Gather(ctx, src)
	if c := find(t, empty, "profiles on the schedule"); c.Status != Warn || !strings.HasPrefix(c.Value, "0") {
		t.Fatalf("empty store: %+v", c)
	}
	if c := find(t, empty, "last data stored"); c.Status != OK {
		t.Fatalf("no profile to collect is not missing data: %+v", c)
	}

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO profile_schedule (profile_ref, tier, interval_seconds, next_run_at) VALUES
		('u:a', 'long_tail', 10800, now() - interval '4 hours'),
		('u:b', 'hot', 600, now() + interval '5 minutes'),
		('u:c', 'long_tail', 10800, now() + interval '1 hour')`)
	exec(`UPDATE profile_schedule SET paused_until = now() + interval '1 day' WHERE profile_ref = 'u:c'`)
	exec(`INSERT INTO profiles (user_pk, username, author_key, last_profile_read_at) VALUES (1, 'a', '\\x01', now() - interval '4 hours')`)
	for i, st := range []string{"completed", "completed", "completed", "failed", "failed", "running"} {
		exec(`INSERT INTO jobs (job_id, kind, lane, mode, profile_ref, params, status)
			VALUES (gen_random_uuid(), 'scheduled', 1, 'incremental', $1, '{}', $2)`, "u:j"+string(rune('a'+i)), st)
	}
	rests := ratelimit.NewExitRests(rdb, "t.")
	for _, x := range []string{"isp#1", "isp#2", "isp#3"} {
		if err := rests.Publish(ctx, x, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	checks := Gather(ctx, src)
	want := map[string]struct {
		status Status
		value  string
	}{
		"profiles on the schedule": {OK, "2 (hot 1, long tail 1); 1 paused"},
		"schedule":                 {Bad, "1 profiles overdue by their own interval"},
		"jobs, last hour":          {Warn, "3 completed, 0 partial, 2 failed; 1 running, 0 queued"},
		"last data stored":         {Bad, "profile read"},
		"proxy exits resting":      {Warn, "isp 3 of 4"},
	}
	for name, w := range want {
		if c := find(t, checks, name); c.Status != w.status || !strings.Contains(c.Value, w.value) {
			t.Errorf("%s: %v %q, want %v containing %q", name, c.Status, c.Value, w.status, w.value)
		}
	}
	var out bytes.Buffer
	if !Print(&out, checks, time.Date(2026, 11, 7, 5, 0, 0, 0, time.UTC)) {
		t.Error("BAD checks did not make the report bad")
	}
	if s := out.String(); !strings.Contains(s, "igs health at 2026-11-07 08:00 Iraq time") || !strings.Contains(s, "BAD   schedule") ||
		!strings.Contains(s, "runbook: No data") {
		t.Errorf("report:\n%s", s)
	}

	// On time, a profile read a minute ago, the rests over: fine.
	exec(`UPDATE profile_schedule SET next_run_at = now() + interval '1 hour'`)
	exec(`UPDATE profiles SET last_profile_read_at = now() - interval '1 minute'`)
	src.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	checks = Gather(ctx, src)
	for _, name := range []string{"schedule", "proxy exits resting"} {
		if c := find(t, checks, name); c.Status != OK {
			t.Errorf("%s: %+v", name, c)
		}
	}
	src.Now = nil
	if c := find(t, Gather(ctx, src), "last data stored"); c.Status != OK {
		t.Errorf("last data: %+v", c)
	}
}
