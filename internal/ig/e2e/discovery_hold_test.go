package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
)

// Discovery ranks below regular collection (REQ-008 fix 6): the vetter
// starts no vet while a scheduled profile is a full interval overdue, while
// a scheduled run is still going a full interval after it started, or while
// most exits of a proxy tier rest. Once collection keeps up again, vets
// start. No cap on registrations or vets is involved.
func TestDiscoveryWaitsForRegularCollection(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Exits: 4, Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags"}, Threshold: 4, MinPriority: 2, VetInFlight: 10,
			Interval: time.Hour, FlushEvery: time.Second, HoldWhenBehind: true, HoldRestingShare: 0.5}
	}})
	ctx := context.Background()
	rests := ratelimit.NewExitRests(s.Redis, s.Cfg.Kafka.TopicPrefix)
	v := &discovery.Vetter{DB: s.DB, Dispatch: s.Dispatch, Cfg: s.Cfg, Rests: rests}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := s.DB.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO discovery_candidates (username, tags, priority) VALUES ('cand_01', 3, 10)`)
	// A profile on the 3-hourly schedule, four hours overdue.
	exec(`INSERT INTO profile_schedule (profile_ref, interval_seconds, next_run_at) VALUES ('u:late_01', 10800, now() - interval '4 hours')`)

	tick := func(wantHeld string) {
		t.Helper()
		if err := v.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		vets := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discovery'`)
		for _, r := range []string{discovery.HoldScheduleBehind, discovery.HoldExitsResting, discovery.HoldRestsUnknown} {
			want := 0.0
			if r == wantHeld {
				want = 1
			}
			if got := gauge("igs_discovery_held", map[string]string{"reason": r}); got != want {
				t.Errorf("igs_discovery_held{reason=%s} %v, want %v", r, got, want)
			}
		}
		if wantHeld != "" && vets != 0 {
			t.Fatalf("held for %s, yet %d vets started", wantHeld, vets)
		}
		if wantHeld == "" && vets != 1 {
			t.Fatalf("not held, yet %d vets started, want 1", vets)
		}
	}

	// The planner is a full interval behind.
	tick(discovery.HoldScheduleBehind)
	// A paused profile is not late.
	exec(`UPDATE profile_schedule SET paused_until = now() + interval '1 hour' WHERE profile_ref = 'u:late_01'`)
	tick("")
	exec(`DELETE FROM jobs WHERE kind = 'discovery'`)
	exec(`UPDATE discovery_candidates SET status = 'new', vet_job_id = NULL WHERE username = 'cand_01'`)

	// On time, but its last run has been going for four hours: its next
	// slot was skipped.
	exec(`UPDATE profile_schedule SET paused_until = NULL, next_run_at = now() + interval '1 hour' WHERE profile_ref = 'u:late_01'`)
	exec(`INSERT INTO jobs (job_id, kind, lane, mode, profile_ref, params, status, created_at)
		VALUES ('00000000-0000-0000-0000-00000000aa01', 'scheduled', 1, 'incremental', 'u:late_01', '{}', 'running', now() - interval '4 hours')`)
	exec(`UPDATE profile_schedule SET last_job_id = '00000000-0000-0000-0000-00000000aa01' WHERE profile_ref = 'u:late_01'`)
	tick(discovery.HoldScheduleBehind)
	exec(`UPDATE jobs SET status = 'completed', finished_at = now() WHERE job_id = '00000000-0000-0000-0000-00000000aa01'`)

	// The schedule keeps up, but three of the four exits rest.
	until := time.Now().Add(time.Hour)
	for _, exit := range []string{"isp#1", "isp#2"} {
		if err := rests.Publish(ctx, exit, until); err != nil {
			t.Fatal(err)
		}
	}
	soon := time.Now().Add(2 * time.Second)
	if err := rests.Publish(ctx, "isp#3", soon); err != nil {
		t.Fatal(err)
	}
	tick(discovery.HoldExitsResting)

	// isp#3's rest ends: half the exits rest, which is not most of them.
	time.Sleep(time.Until(soon) + 500*time.Millisecond)
	tick("")
	var st string
	if err := s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = 'cand_01'`).Scan(&st); err != nil || st != "vetting" {
		t.Errorf("candidate %q %v, want vetting", st, err)
	}
}

// gauge reads a gauge's child matching labels.
func gauge(name string, labels map[string]string) float64 {
	mfs, _ := metrics.Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matches(m, labels) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}
