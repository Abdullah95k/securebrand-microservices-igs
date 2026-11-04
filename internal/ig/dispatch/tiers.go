package dispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// Schedule tiers (architecture §8).
const (
	TierHot      = "hot"
	TierWarm     = "warm"
	TierLongTail = "long_tail"
	// TierCustom is a profile given its own interval.
	TierCustom = "custom"
	// TierAuto asks for placement by posting rate (adaptive).
	TierAuto = "auto"
)

// MaxInterval is the longest interval a profile may have.
const MaxInterval = 30 * 24 * time.Hour

// Placement is where a profile sits on the schedule.
type Placement struct {
	Tier     string
	Interval time.Duration
	// Adaptive profiles are moved between warm and long tail by their
	// posting rate.
	Adaptive bool
}

// Place resolves a requested tier ("", auto, hot, warm, long_tail) or
// interval to a placement. An interval without a tier (or with custom) is
// a custom placement; neither is auto: long tail until the posting rate
// says otherwise.
func Place(cfg *config.Config, tier string, interval time.Duration) (Placement, error) {
	sc := cfg.IG.Schedule
	switch tier {
	case "", TierAuto, TierCustom:
		if interval > 0 {
			if interval < sc.MinInterval || interval > MaxInterval {
				return Placement{}, fmt.Errorf("the interval must be between %s and %s", sc.MinInterval, MaxInterval)
			}
			return Placement{Tier: TierCustom, Interval: interval}, nil
		}
		if tier == TierCustom {
			return Placement{}, fmt.Errorf("tier custom needs an interval")
		}
		return Placement{Tier: TierLongTail, Interval: sc.TierInterval(TierLongTail), Adaptive: sc.Tiers.Auto}, nil
	case TierHot, TierWarm, TierLongTail:
		if interval > 0 {
			return Placement{}, fmt.Errorf("give a tier or an interval, not both")
		}
		return Placement{Tier: tier, Interval: sc.TierInterval(tier)}, nil
	}
	return Placement{}, fmt.Errorf("tier %q must be hot, warm, long_tail or auto", tier)
}

// SetPlacement moves the profiles to pl: a pin, a custom interval, or back
// to placement by posting rate (adaptive, re-placed at the next check).
// A shorter interval brings the next run forward.
func SetPlacement(ctx context.Context, db *pgxpool.Pool, refs []string, pl Placement) error {
	_, err := db.Exec(ctx, `UPDATE profile_schedule SET tier = $2, interval_seconds = $3::int, adaptive = $4,
			tier_changed_at = CASE WHEN $4 THEN NULL ELSE now() END,
			next_run_at = least(next_run_at, now() + $3::int * interval '1 second')
		WHERE profile_ref = ANY($1)`, refs, pl.Tier, int(pl.Interval.Seconds()), pl.Adaptive)
	return err
}
