package discovery

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Why the vetter starts no vets (igs_discovery_held{reason}).
const (
	// HoldScheduleBehind: a scheduled profile is a full interval overdue, or
	// its run is still going a full interval after it was started.
	HoldScheduleBehind = "schedule_behind"
	// HoldExitsResting: more than ig.discovery.hold_resting_share of a
	// static proxy tier's exits rest.
	HoldExitsResting = "exits_resting"
	// HoldRestsUnknown: the shared exit rests could not be read.
	HoldRestsUnknown = "rests_unknown"
)

var holdReasons = []string{HoldScheduleBehind, HoldExitsResting, HoldRestsUnknown}

// RestLoader reads the exit rests the crawlers share
// (ratelimit.ExitRests, ig.share_exit_rests).
type RestLoader interface {
	Load(ctx context.Context) (map[string]time.Time, error)
}

// hold reports why new vets wait, or "" when they may start. Discovery
// ranks below regular collection (the owner, 6 November 2026): it starts
// vets only while the schedule is on time and no proxy tier has most of
// its exits resting. Vets already running carry on; deciding, rescoring
// and registering read nothing from Instagram and go on too.
func (v *Vetter) hold(ctx context.Context) (string, error) {
	d := v.Cfg.IG.Discovery
	if d.HoldWhenBehind {
		behind, err := v.scheduleBehind(ctx)
		if err != nil {
			return "", err
		}
		if behind {
			return HoldScheduleBehind, nil
		}
	}
	if d.HoldRestingShare > 0 && v.Rests != nil {
		rests, err := v.Rests.Load(ctx)
		if err != nil {
			// Unknown is not known to be fine: wait (fail closed).
			slog.Warn("discovery: exit rests unreadable; vets wait", "err", err)
			return HoldRestsUnknown, nil
		}
		if tier := v.restingTier(rests, time.Now()); tier != "" {
			return HoldExitsResting, nil
		}
	}
	return "", nil
}

// scheduleBehind reports whether regular collection is behind: an enabled
// profile, not paused, is overdue by its own interval (the planner does
// not keep up), or a scheduled run is still queued or running a full
// interval after it was started (the crawlers do not: the profile's next
// slots are skipped).
func (v *Vetter) scheduleBehind(ctx context.Context) (bool, error) {
	floor := int(v.Cfg.IG.Schedule.MinInterval.Seconds())
	var behind bool
	err := v.DB.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM profile_schedule
			WHERE enabled AND next_run_at < now() - $1::int * interval '1 second'
				AND next_run_at < now() - make_interval(secs => interval_seconds)
				AND (paused_until IS NULL OR paused_until < now()))
		OR EXISTS (
			SELECT 1 FROM jobs j JOIN profile_schedule s ON s.profile_ref = j.profile_ref AND s.last_job_id = j.job_id
			WHERE j.status IN ('queued', 'running', 'finalizing') AND j.kind = 'scheduled'
				AND j.created_at < now() - make_interval(secs => s.interval_seconds))`, floor).Scan(&behind)
	return behind, err
}

// restingTier returns a static proxy tier with more than
// hold_resting_share of its exits resting at now, or "". Gateway tiers
// are left out: their exits are sessions, with no fixed number.
func (v *Vetter) restingTier(rests map[string]time.Time, now time.Time) string {
	share := v.Cfg.IG.Discovery.HoldRestingShare
	for _, t := range v.Cfg.IG.Proxy.Tiers {
		if t.Kind != "static_list" || len(t.Endpoints) == 0 {
			continue
		}
		resting := 0
		for exit, until := range rests {
			tier, n, ok := strings.Cut(exit, "#")
			if !ok || tier != t.Name || !until.After(now) {
				continue
			}
			// Static exits are tier#<1..n> (proxy.Endpoint.Exit).
			if i, err := strconv.Atoi(n); err == nil && i >= 1 && i <= len(t.Endpoints) {
				resting++
			}
		}
		if float64(resting) > share*float64(len(t.Endpoints)) {
			return t.Name
		}
	}
	return ""
}

// setHeld records the hold reason ("" when vets may start) in
// igs_discovery_held and logs a change.
func (v *Vetter) setHeld(reason string) {
	for _, r := range holdReasons {
		val := 0.0
		if r == reason {
			val = 1
		}
		metrics.DiscoveryHeld.WithLabelValues(r).Set(val)
	}
	if reason != v.held {
		if reason == "" {
			slog.Info("discovery: vets resume", "was", v.held)
		} else {
			slog.Info("discovery: vets wait for regular collection", "reason", reason)
		}
		v.held = reason
	}
}
