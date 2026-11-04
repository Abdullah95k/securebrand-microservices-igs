package scheduler

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Tierer places the profiles that are not pinned (adaptive) by their
// posting rate (ig.schedule.tiers): warm at warm_posts_per_day or more over
// auto_window, else long tail. Each is checked every auto_every, and as
// soon as its user pk is known (after its backfill). Pinned and custom
// profiles stay where they were put. Leader only.
type Tierer struct {
	DB  *pgxpool.Pool
	Cfg *config.Config
}

// TierBatch is how many profiles one tick places at most.
const TierBatch = 5000

// Tick places the adaptive profiles due a check and updates the tier
// gauges.
func (t *Tierer) Tick(ctx context.Context) error {
	sc := t.Cfg.IG.Schedule
	tc := sc.Tiers
	if tc.Auto {
		days := tc.AutoWindow.Hours() / 24
		tag, err := t.DB.Exec(ctx, `
			WITH d AS (
				SELECT profile_ref, user_pk FROM profile_schedule
				WHERE adaptive AND user_pk IS NOT NULL
					AND (tier_changed_at IS NULL OR tier_changed_at < now() - $1::int * interval '1 second')
				ORDER BY tier_changed_at NULLS FIRST LIMIT $2 FOR UPDATE SKIP LOCKED
			), r AS (
				SELECT d.profile_ref, (SELECT count(*) FROM post_index i
					WHERE i.owner_pk = d.user_pk AND i.taken_at > now() - $3::int * interval '1 second')::float8 / $4 AS ppd
				FROM d
			), p AS (
				SELECT profile_ref, ppd, CASE WHEN ppd >= $5 THEN $6 ELSE $7 END AS tier,
					CASE WHEN ppd >= $5 THEN $8::int ELSE $9::int END AS secs
				FROM r
			)
			UPDATE profile_schedule s SET posts_per_day = p.ppd, tier = p.tier, interval_seconds = p.secs, tier_changed_at = now(),
				next_run_at = least(s.next_run_at, now() + p.secs * interval '1 second')
			FROM p WHERE s.profile_ref = p.profile_ref`,
			int(tc.AutoEvery.Seconds()), TierBatch, int(tc.AutoWindow.Seconds()), days, tc.WarmPostsPerDay,
			dispatch.TierWarm, dispatch.TierLongTail, int(sc.TierInterval(dispatch.TierWarm).Seconds()),
			int(sc.TierInterval(dispatch.TierLongTail).Seconds()))
		if err != nil {
			return err
		}
		if n := tag.RowsAffected(); n > 0 {
			slog.Debug("tiers: profiles placed", "count", n)
		}
	}
	rows, err := t.DB.Query(ctx, `SELECT tier, count(*) FROM profile_schedule WHERE enabled GROUP BY tier`)
	if err != nil {
		return err
	}
	defer rows.Close()
	counts := map[string]float64{}
	for rows.Next() {
		var tier string
		var n int64
		if err := rows.Scan(&tier, &n); err != nil {
			return err
		}
		counts[tier] = float64(n)
	}
	for _, tier := range []string{dispatch.TierHot, dispatch.TierWarm, dispatch.TierLongTail, dispatch.TierCustom} {
		metrics.ProfilesByTier.WithLabelValues(tier).Set(counts[tier])
	}
	return rows.Err()
}
