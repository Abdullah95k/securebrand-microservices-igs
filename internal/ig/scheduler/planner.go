// Package scheduler runs igs's control loops (leader only): the planner
// that starts each registered profile's scheduled run (D2: every 3 hours,
// the profile itself daily) with the due comment refreshes of its recent
// posts, the finalizer that turns finished jobs terminal and publishes
// ig.job.v1, the reaper for stalled jobs, and partition maintenance.
package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Every runs fn every interval until ctx is done, logging errors.
func Every(ctx context.Context, name string, interval time.Duration, fn func(context.Context) error) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("igs scheduler: loop failed", "loop", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Planner starts the scheduled runs of registered profiles. DB is a pool on
// schema ig.
type Planner struct {
	DB       *pgxpool.Pool
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
}

type due struct {
	ref      string
	userPK   *int64
	options  []byte
	maxAge   *int
	maxPosts *int64
	lastJob  *uuid.UUID
	refresh  bool
	interval int
}

// Tick takes the due profiles (a failing profile backs off: its interval
// doubles per consecutive failure, up to 30 days) and starts their runs.
func (p *Planner) Tick(ctx context.Context) error {
	g := p.Cfg.IG.Schedule
	rows, err := p.DB.Query(ctx, `
		WITH d AS (
			SELECT profile_ref FROM profile_schedule
			WHERE enabled AND next_run_at <= now() AND (paused_until IS NULL OR paused_until < now())
			ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE profile_schedule s SET
			next_run_at = now() + make_interval(secs => s.interval_seconds
				* least(power(2, s.consecutive_failures), 2592000.0 / s.interval_seconds)
				* (1 + (random() * 2 - 1) * $2)),
			last_run_at = now()
		FROM d WHERE s.profile_ref = d.profile_ref
		RETURNING s.profile_ref, s.user_pk, s.options, s.max_age_days, s.max_posts, s.last_job_id,
			coalesce(s.next_profile_read_at <= now(), true), s.interval_seconds`,
		max(p.Cfg.Scheduler.PlannerBatch, 1), g.Jitter)
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
		var d due
		return d, r.Scan(&d.ref, &d.userPK, &d.options, &d.maxAge, &d.maxPosts, &d.lastJob, &d.refresh, &d.interval)
	})
	if err != nil {
		return err
	}
	for _, d := range list {
		if d.lastJob != nil {
			var st string
			if err := p.DB.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, *d.lastJob).Scan(&st); err == nil &&
				(st == "running" || st == "finalizing" || st == "cancelling") {
				continue // the previous run is still going: skip this slot
			}
		}
		var o dispatch.CrawlOptions
		if len(d.options) > 0 {
			_ = json.Unmarshal(d.options, &o)
		}
		if d.maxAge != nil && *d.maxAge > 0 {
			o.MaxAgeDays = d.maxAge
		}
		if d.maxPosts != nil {
			o.MaxPosts = d.maxPosts
		}
		lim, err := dispatch.Limits(p.Cfg, o)
		if err != nil {
			slog.Warn("planner: stored options rejected; using the defaults", "profile", d.ref, "err", err)
			lim, _ = dispatch.Limits(p.Cfg, dispatch.CrawlOptions{})
		}
		var posts []dispatch.PostRoot
		var refreshed []int64
		if d.userPK != nil && lim.IncludeComments && g.PostRefreshPerRun > 0 {
			if posts, refreshed, err = p.dueRefreshes(ctx, *d.userPK); err != nil {
				slog.Warn("planner: post refreshes", "profile", d.ref, "err", err)
				posts, refreshed = nil, nil
			}
		}
		j, _, err := p.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindScheduled, Lane: igstaskv1.Lane_LANE_P1,
			Mode: "auto", ProfileRef: d.ref, Limits: lim, RefreshProfile: d.refresh, Posts: posts})
		if err != nil {
			slog.Warn("planner: dispatch failed", "profile", d.ref, "err", err)
			continue // the refreshes stay due
		}
		if _, err := p.DB.Exec(ctx, `UPDATE profile_schedule SET last_job_id = $2,
				next_profile_read_at = CASE WHEN $3 THEN now() + $4::int * interval '1 second' ELSE next_profile_read_at END
			WHERE profile_ref = $1`, d.ref, j.ID, d.refresh, int(g.ProfileRefresh.Seconds())); err != nil {
			return err
		}
		if len(refreshed) > 0 {
			// The next refresh waits twice as long as the last: comments come
			// fast while a post is new and slow down after.
			if _, err := p.DB.Exec(ctx, `UPDATE post_refresh SET refreshes = refreshes + 1, last_refresh_at = now(),
					next_refresh_at = now() + make_interval(secs => $2::int * power(2, least(refreshes, 10)))
				WHERE media_pk = ANY($1)`, refreshed, d.interval); err != nil {
				return err
			}
		}
	}
	var lag float64
	_ = p.DB.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM now() - min(next_run_at)), 0) FROM profile_schedule
		WHERE enabled AND next_run_at <= now()`).Scan(&lag)
	metrics.ScheduleLag.WithLabelValues("ig").Set(math.Max(lag, 0))
	return nil
}

// dueRefreshes returns the profile's posts due a comment refresh (newest
// first, at most ig.schedule.post_refresh_per_run), as post tasks.
func (p *Planner) dueRefreshes(ctx context.Context, userPK int64) ([]dispatch.PostRoot, []int64, error) {
	g := p.Cfg.IG.Schedule
	rows, err := p.DB.Query(ctx, `SELECT r.media_pk, i.shortcode, r.taken_at, r.last_comment_count, p.permalink, p.comment_count
		FROM post_refresh r JOIN post_index i ON i.media_pk = r.media_pk
		LEFT JOIN posts p ON p.taken_at = r.taken_at AND p.media_pk = r.media_pk
		WHERE r.user_pk = $1 AND r.next_refresh_at <= now() AND r.taken_at > now() - $2::int * interval '1 second'
		ORDER BY r.taken_at DESC LIMIT $3`, userPK, int(g.PostRefreshWindow.Seconds()), g.PostRefreshPerRun)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []dispatch.PostRoot
	var pks []int64
	for rows.Next() {
		var pk int64
		var code string
		var taken time.Time
		var last, count *int64
		var link *string
		if err := rows.Scan(&pk, &code, &taken, &last, &link, &count); err != nil {
			return nil, nil, err
		}
		sc := &igstaskv1.PostScope{MediaPk: pk, Shortcode: code, OwnerPk: userPK, TakenAt: timestamppb.New(taken), DetailKnown: true}
		if link != nil {
			sc.Permalink = *link
		}
		if count != nil {
			sc.ReportedCommentCount = *count
		}
		out = append(out, dispatch.PostRoot{Scope: sc})
		pks = append(pks, pk)
	}
	return out, pks, rows.Err()
}
