package dispatch

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	mrand "math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// Registration puts one profile on the schedule for a client: the API's
// POST /v1/profiles and its bulk form, igsctl profiles import, and
// discovery.
type Registration struct {
	ClientID uuid.UUID
	// Ref is a normalized profile ref (task.NormalizeRef).
	Ref string
	// Interval between scheduled runs; 0 takes ig.schedule.interval.
	Interval time.Duration
	Enabled  bool
	Options  CrawlOptions
	// Backfill runs a full crawl now; nil runs one only for a profile that
	// has had no job yet, so registering a list again starts nothing twice.
	Backfill *bool
	// KeepSchedule leaves an existing schedule as it is: the profile only
	// gains the client's registration (discovery).
	KeepSchedule bool
	// BackfillOwner, when set, owns the backfill job; Params are recorded
	// on it.
	BackfillOwner *uuid.UUID
	Params        map[string]any
}

// Registered is what a registration did.
type Registered struct {
	// Created: the profile was not on the schedule before.
	Created  bool
	Backfill *Job
}

// Register schedules r.Ref and starts its backfill when due.
func (d *Dispatcher) Register(ctx context.Context, cfg *config.Config, r Registration) (Registered, error) {
	var res Registered
	lim, err := Limits(cfg, r.Options)
	if err != nil {
		return res, err
	}
	iv := r.Interval
	if iv <= 0 {
		iv = cfg.IG.Schedule.Interval
	}
	var upk *int64
	if k, found := d.Resolve(ctx, r.Ref); found {
		pk := k.UserPK()
		upk = &pk
	}
	opts, _ := json.Marshal(r.Options)
	next := time.Now().Add(iv + jitter(iv, cfg.IG.Schedule.Jitter))
	onConflict := `DO UPDATE SET user_pk = COALESCE(t.user_pk, EXCLUDED.user_pk), enabled = EXCLUDED.enabled,
				interval_seconds = EXCLUDED.interval_seconds, options = EXCLUDED.options,
				max_age_days = EXCLUDED.max_age_days, max_posts = EXCLUDED.max_posts`
	if r.KeepSchedule {
		onConflict = `DO UPDATE SET user_pk = COALESCE(t.user_pk, EXCLUDED.user_pk)`
	}
	var lastJob *uuid.UUID
	err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO profile_schedule AS t (profile_ref, user_pk, enabled, interval_seconds, next_run_at,
				options, max_age_days, max_posts, next_profile_read_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + $9::int * interval '1 second')
			ON CONFLICT (profile_ref) `+onConflict+`
			RETURNING xmax = 0, last_job_id`,
			r.Ref, upk, r.Enabled, int(iv.Seconds()), next, opts, r.Options.MaxAgeDays, r.Options.MaxPosts,
			int(cfg.IG.Schedule.ProfileRefresh.Seconds())).Scan(&res.Created, &lastJob); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO profile_registrations (api_client_id, profile_ref) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			r.ClientID, r.Ref)
		return err
	})
	if err != nil {
		return res, err
	}
	backfill := lastJob == nil
	if r.Backfill != nil {
		backfill = *r.Backfill
	}
	if !backfill {
		return res, nil
	}
	j, _, err := d.Create(ctx, Request{ClientID: r.BackfillOwner, Kind: KindBackfill, Lane: igstaskv1.Lane_LANE_P2, Mode: "full",
		ProfileRef: r.Ref, Limits: lim, RefreshProfile: true, Params: r.Params})
	if err != nil {
		return res, err
	}
	res.Backfill = j
	_, err = d.DB.Exec(ctx, `UPDATE profile_schedule SET last_job_id = $2, last_run_at = now() WHERE profile_ref = $1`, r.Ref, j.ID)
	return res, err
}

func jitter(iv time.Duration, share float64) time.Duration {
	if share <= 0 {
		return 0
	}
	return time.Duration(mrand.Int64N(int64(float64(iv)*share) + 1))
}

// SystemClient returns the API client name stands for, created revoked
// (no request authenticates as it) on first use: the owner of
// registrations made by igs itself (discovery) or by igsctl.
func SystemClient(ctx context.Context, db *pgxpool.Pool, name string) (uuid.UUID, error) {
	var id uuid.UUID
	err := db.QueryRow(ctx, `SELECT id FROM api_clients WHERE key_prefix = $1`, name).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return id, err
	}
	id = uuid.New()
	hash := make([]byte, 32)
	_, _ = rand.Read(hash)
	if _, err := db.Exec(ctx, `INSERT INTO api_clients (id, name, key_prefix, key_hash, scopes, revoked_at)
		VALUES ($1, $2, $2, $3, '{}', now()) ON CONFLICT (key_prefix) DO NOTHING`, id, name, hash); err != nil {
		return id, err
	}
	err = db.QueryRow(ctx, `SELECT id FROM api_clients WHERE key_prefix = $1`, name).Scan(&id)
	return id, err
}
