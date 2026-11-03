package api

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

// Profile is the API view of a stored profile.
type Profile struct {
	UserPK            int64           `json:"user_pk"`
	Username          *string         `json:"username,omitempty"`
	FullName          *string         `json:"full_name,omitempty"`
	Biography         *string         `json:"biography,omitempty"`
	BioLinks          json.RawMessage `json:"bio_links,omitempty"`
	Verified          *bool           `json:"verified,omitempty"`
	Private           *bool           `json:"private,omitempty"`
	FollowerCount     *int64          `json:"follower_count,omitempty"`
	FollowingCount    *int64          `json:"following_count,omitempty"`
	MediaCount        *int64          `json:"media_count,omitempty"`
	ProfilePicURL     *string         `json:"profile_pic_url,omitempty"`
	Category          *string         `json:"category,omitempty"`
	IsBusiness        *bool           `json:"is_business,omitempty"`
	HighlightCount    *int32          `json:"highlight_count,omitempty"`
	Status            string          `json:"status"`
	FirstSeenAt       time.Time       `json:"first_seen_at"`
	LastScrapedAt     *time.Time      `json:"last_scraped_at,omitempty"`
	LastProfileReadAt *time.Time      `json:"last_profile_read_at,omitempty"`
	Schedule          *Schedule       `json:"schedule,omitempty"`
}

// Schedule is a registered profile's scheduled crawl.
type Schedule struct {
	ProfileRef        string          `json:"profile_ref"`
	UserPK            *int64          `json:"user_pk,omitempty"`
	Enabled           bool            `json:"enabled"`
	IntervalSeconds   int             `json:"interval_seconds"`
	NextRunAt         time.Time       `json:"next_run_at"`
	NextProfileReadAt *time.Time      `json:"next_profile_read_at,omitempty"`
	LastRunAt         *time.Time      `json:"last_run_at,omitempty"`
	LastJobID         *uuid.UUID      `json:"last_job_id,omitempty"`
	Failures          int             `json:"consecutive_failures"`
	Options           json.RawMessage `json:"options,omitempty"`
}

func (s *Server) profileByPK(ctx context.Context, pk int64) (*Profile, error) {
	var p Profile
	err := s.ReadDB.QueryRow(ctx, `SELECT user_pk, username, full_name, biography, bio_links, verified, private, follower_count,
		following_count, media_count, profile_pic_url, category, is_business, highlight_count, status, first_seen_at,
		last_scraped_at, last_profile_read_at FROM profiles WHERE user_pk = $1 AND status <> 'suppressed'`, pk).
		Scan(&p.UserPK, &p.Username, &p.FullName, &p.Biography, &p.BioLinks, &p.Verified, &p.Private, &p.FollowerCount,
			&p.FollowingCount, &p.MediaCount, &p.ProfilePicURL, &p.Category, &p.IsBusiness, &p.HighlightCount, &p.Status,
			&p.FirstSeenAt, &p.LastScrapedAt, &p.LastProfileReadAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

const selectSchedule = `SELECT profile_ref, user_pk, enabled, interval_seconds, next_run_at, next_profile_read_at,
	last_run_at, last_job_id, consecutive_failures, options FROM profile_schedule`

func scanSchedule(row pgx.CollectableRow) (*Schedule, error) {
	var sc Schedule
	err := row.Scan(&sc.ProfileRef, &sc.UserPK, &sc.Enabled, &sc.IntervalSeconds, &sc.NextRunAt, &sc.NextProfileReadAt,
		&sc.LastRunAt, &sc.LastJobID, &sc.Failures, &sc.Options)
	return &sc, err
}

// resolveProfile maps the {profile} path parameter (a username, @name,
// URL or id:<pk>) to a stored user pk and its normalised reference.
func (s *Server) resolveProfile(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	ref, err := task.NormalizeRef(chi.URLParam(r, "profile"))
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid profile", err.Error())
		return 0, "", false
	}
	k, ok := s.Dispatch.Resolve(r.Context(), ref)
	if !ok {
		httpapi.Problem(w, http.StatusNotFound, "unknown profile", "the profile has not been read yet")
		return 0, ref, false
	}
	return k.UserPK(), ref, true
}

type scheduleReq struct {
	Enabled       *bool `json:"enabled,omitempty"`
	IntervalHours *int  `json:"interval_hours,omitempty"`
}

type registerReq struct {
	Profile  string       `json:"profile"`
	Schedule *scheduleReq `json:"schedule,omitempty"`
	// Backfill runs a full crawl now. By default only for a profile that
	// has had no job yet, so registering a list again starts nothing twice.
	Backfill *bool `json:"backfill,omitempty"`
	CrawlOptions
}

func (s *Server) interval(sr *scheduleReq) (time.Duration, error) {
	iv := s.Cfg.IG.Schedule.Interval
	if sr != nil && sr.IntervalHours != nil {
		if *sr.IntervalHours < 1 || *sr.IntervalHours > 24*30 {
			return 0, errors.New("interval_hours must be between 1 and 720")
		}
		iv = time.Duration(*sr.IntervalHours) * time.Hour
	}
	return iv, nil
}

// registerProfile adds a profile to the schedule (D2: every 3 hours, the
// profile itself daily) and starts its backfill.
func (s *Server) registerProfile(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Profile)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid profile", err.Error())
		return
	}
	lim, err := Limits(s.Cfg, req.CrawlOptions)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	iv, err := s.interval(req.Schedule)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid schedule", err.Error())
		return
	}
	enabled := true
	if req.Schedule != nil && req.Schedule.Enabled != nil {
		enabled = *req.Schedule.Enabled
	}
	ctx := r.Context()
	k, found := s.Dispatch.Resolve(ctx, ref)
	var upk *int64
	if found {
		pk := k.UserPK()
		upk = &pk
	}
	opts, _ := json.Marshal(req.CrawlOptions)
	next := time.Now().Add(iv + jitter(iv, s.Cfg.IG.Schedule.Jitter))
	c := httpapi.ClientFrom(ctx)
	var created bool
	var lastJob *uuid.UUID
	err = pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO profile_schedule AS t (profile_ref, user_pk, enabled, interval_seconds, next_run_at,
				options, max_age_days, max_posts, next_profile_read_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + $9::int * interval '1 second')
			ON CONFLICT (profile_ref) DO UPDATE SET user_pk = COALESCE(t.user_pk, EXCLUDED.user_pk), enabled = EXCLUDED.enabled,
				interval_seconds = EXCLUDED.interval_seconds, options = EXCLUDED.options,
				max_age_days = EXCLUDED.max_age_days, max_posts = EXCLUDED.max_posts
			RETURNING xmax = 0, last_job_id`,
			ref, upk, enabled, int(iv.Seconds()), next, opts, req.MaxAgeDays, req.MaxPosts, int(s.Cfg.IG.Schedule.ProfileRefresh.Seconds())).
			Scan(&created, &lastJob); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO profile_registrations (api_client_id, profile_ref) VALUES ($1, $2) ON CONFLICT DO NOTHING`, c.ID, ref)
		return err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	backfill := lastJob == nil
	if req.Backfill != nil {
		backfill = *req.Backfill
	}
	out := map[string]any{"profile_ref": ref, "registered": true}
	if backfill {
		j, _, err := s.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindBackfill, Lane: igstaskv1.Lane_LANE_P2,
			Mode: "full", ProfileRef: ref, Limits: lim, RefreshProfile: true, Params: map[string]any{"profile": req.Profile, "registered_by": c.ID}})
		switch {
		case errors.Is(err, dispatch.ErrUnknownProfile):
			httpapi.Problem(w, http.StatusUnprocessableEntity, "unknown profile", err.Error())
			return
		case err != nil:
			httpapi.ServerError(w, err)
			return
		}
		_, _ = s.DB.Exec(ctx, `UPDATE profile_schedule SET last_job_id = $2, last_run_at = now() WHERE profile_ref = $1`, ref, j.ID)
		out["backfill_job"] = j
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	httpapi.WriteJSON(w, code, out)
}

func jitter(iv time.Duration, share float64) time.Duration {
	if share <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(float64(iv)*share) + 1))
}

func (s *Server) listProfiles(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	rows, err := s.ReadDB.Query(r.Context(), selectSchedule+` s WHERE $1::uuid IS NULL OR EXISTS (
		SELECT 1 FROM profile_registrations g WHERE g.profile_ref = s.profile_ref AND g.api_client_id = $1)
		ORDER BY profile_ref LIMIT $2`, o, httpapi.LimitParam(r))
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	scs, err := pgx.CollectRows(rows, scanSchedule)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if scs == nil {
		scs = []*Schedule{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"profiles": scs})
}

func (s *Server) getProfile(w http.ResponseWriter, r *http.Request) {
	pk, ref, ok := s.resolveProfile(w, r)
	if !ok {
		return
	}
	p, err := s.profileByPK(r.Context(), pk)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), selectSchedule+` WHERE profile_ref = $1 OR user_pk = $2 LIMIT 1`, ref, pk)
	if err == nil {
		if sc, err := pgx.CollectExactlyOneRow(rows, scanSchedule); err == nil {
			p.Schedule = sc
		}
	}
	httpapi.WriteJSON(w, http.StatusOK, p)
}

// registeredRef is the caller's registration of {profile}, or 404.
func (s *Server) registeredRef(w http.ResponseWriter, r *http.Request) (string, bool) {
	ref, err := task.NormalizeRef(chi.URLParam(r, "profile"))
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid profile", err.Error())
		return "", false
	}
	var exists bool
	o := owner(r)
	if err := s.DB.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM profile_schedule s WHERE s.profile_ref = $1 AND ($2::uuid IS NULL
		OR EXISTS (SELECT 1 FROM profile_registrations g WHERE g.profile_ref = s.profile_ref AND g.api_client_id = $2)))`, ref, o).Scan(&exists); err != nil {
		httpapi.ServerError(w, err)
		return "", false
	}
	if !exists {
		httpapi.Problem(w, http.StatusNotFound, "not registered", "the profile is not registered by this client")
		return "", false
	}
	return ref, true
}

type updateReq struct {
	Schedule *scheduleReq  `json:"schedule,omitempty"`
	Options  *CrawlOptions `json:"options,omitempty"`
}

func (s *Server) updateProfile(w http.ResponseWriter, r *http.Request) {
	ref, ok := s.registeredRef(w, r)
	if !ok {
		return
	}
	var req updateReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ctx := r.Context()
	if req.Schedule != nil {
		if req.Schedule.IntervalHours != nil {
			iv, err := s.interval(req.Schedule)
			if err != nil {
				httpapi.Problem(w, http.StatusBadRequest, "invalid schedule", err.Error())
				return
			}
			if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET interval_seconds = $2::int,
				next_run_at = LEAST(next_run_at, now() + $2::int * interval '1 second') WHERE profile_ref = $1`, ref, int(iv.Seconds())); err != nil {
				httpapi.ServerError(w, err)
				return
			}
		}
		if req.Schedule.Enabled != nil {
			if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET enabled = $2 WHERE profile_ref = $1`, ref, *req.Schedule.Enabled); err != nil {
				httpapi.ServerError(w, err)
				return
			}
		}
	}
	if req.Options != nil {
		if _, err := Limits(s.Cfg, *req.Options); err != nil {
			httpapi.Problem(w, http.StatusBadRequest, "invalid options", err.Error())
			return
		}
		opts, _ := json.Marshal(req.Options)
		if _, err := s.DB.Exec(ctx, `UPDATE profile_schedule SET options = $2, max_age_days = $3, max_posts = $4 WHERE profile_ref = $1`,
			ref, opts, req.Options.MaxAgeDays, req.Options.MaxPosts); err != nil {
			httpapi.ServerError(w, err)
			return
		}
	}
	rows, err := s.DB.Query(ctx, selectSchedule+` WHERE profile_ref = $1`, ref)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	sc, err := pgx.CollectExactlyOneRow(rows, scanSchedule)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, sc)
}

// deleteProfile removes the caller's registration; the schedule ends with
// the last registration (an admin removes the schedule outright).
func (s *Server) deleteProfile(w http.ResponseWriter, r *http.Request) {
	ref, ok := s.registeredRef(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	c := httpapi.ClientFrom(ctx)
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		if c.IsAdmin() {
			// A profile an admin removes is not discovered again.
			if u, ok := strings.CutPrefix(ref, "u:"); ok {
				if _, err := tx.Exec(ctx, `UPDATE discovery_candidates SET status = 'rejected', decided_at = now(), vet = NULL
					WHERE username = $1`, u); err != nil {
					return err
				}
			}
			_, err := tx.Exec(ctx, `DELETE FROM profile_schedule WHERE profile_ref = $1`, ref)
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM profile_registrations WHERE profile_ref = $1 AND api_client_id = $2`, ref, c.ID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM profile_schedule s WHERE profile_ref = $1
			AND NOT EXISTS (SELECT 1 FROM profile_registrations g WHERE g.profile_ref = s.profile_ref)`, ref)
		return err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
