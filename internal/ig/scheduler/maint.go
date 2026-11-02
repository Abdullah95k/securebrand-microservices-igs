package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// Maintenance keeps schema ig's partitions ahead of time and applies the
// retention settings.
type Maintenance struct {
	DB  *pgxpool.Pool // schema ig
	Cfg *config.Config
}

// Tick creates missing partitions and drops expired ones and rows.
func (m *Maintenance) Tick(ctx context.Context) error {
	now := time.Now().UTC()
	n, err := store.IGLayout.EnsurePartitions(ctx, m.DB, now, store.PartitionWindow{
		MonthsBack: m.Cfg.IG.Limits.MaxAgeCapDays/30 + 1, MonthsAhead: max(m.Cfg.Scheduler.PartitionMonthsAhead, 1), DaysBack: 1, DaysAhead: 14})
	if err != nil {
		return err
	}
	if n > 0 {
		slog.Info("igs partitions created", "count", n)
	}
	ret := m.Cfg.Privacy.Retention
	for _, d := range []struct {
		table string
		days  int
	}{{"comments", ret.CommentsDays}, {"post_metrics", ret.MetricsDays}} {
		if d.days <= 0 {
			continue
		}
		dropped, err := store.DropPartitionsBefore(ctx, m.DB, d.table, now.AddDate(0, 0, -d.days))
		if err != nil {
			return err
		}
		if len(dropped) > 0 {
			slog.Info("igs retention: partitions dropped", "table", d.table, "partitions", dropped)
		}
	}
	if ret.JobsDays > 0 {
		if _, err := m.DB.Exec(ctx, `DELETE FROM jobs WHERE job_id IN (SELECT job_id FROM jobs
			WHERE finished_at < now() - make_interval(days => $1) LIMIT 5000)`, ret.JobsDays); err != nil {
			return err
		}
	}
	return nil
}

// Reaper ends jobs that stopped making progress: partial when tasks are
// still pending without activity, failed when their accounting state is
// gone.
type Reaper struct {
	DB    *pgxpool.Pool // schema ig
	Jobs  *jobs.Tracker
	After time.Duration
}

// Tick reaps stalled jobs.
func (r *Reaper) Tick(ctx context.Context) error {
	rows, err := r.DB.Query(ctx, `SELECT job_id FROM jobs WHERE status IN ('running','cancelling')
		AND created_at < now() - $1::int * interval '1 second' LIMIT 500`, int(r.After.Seconds()))
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return err
	}
	for _, id := range ids {
		st, err := r.Jobs.Status(ctx, id.String())
		if err != nil {
			continue
		}
		var final, reason string
		switch {
		case st.Spawned == 0:
			final, reason = "failed", "job state lost"
		case st.Pending > 0 && time.Since(st.LastActivity) > r.After:
			final, reason = "partial", "stalled with pending tasks"
		default:
			continue
		}
		eb, _ := json.Marshal(map[string]any{"message": reason, "pending_tasks": st.Pending})
		cb, _ := json.Marshal(st.Counters)
		if _, err := r.DB.Exec(ctx, `UPDATE jobs SET status = $2, error = $3, counters = $4, finished_at = now()
			WHERE job_id = $1 AND status IN ('running','cancelling')`, id, final, eb, cb); err != nil {
			return err
		}
		slog.Warn("igs reaper: job ended", "job", id, "status", final, "reason", reason)
	}
	return nil
}
