package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

// Profile is the API view of a stored profile.
type Profile struct {
	UserPK    int64           `json:"user_pk"`
	Username  *string         `json:"username,omitempty"`
	FullName  *string         `json:"full_name,omitempty"`
	Biography *string         `json:"biography,omitempty"`
	BioLinks  json.RawMessage `json:"bio_links,omitempty"`
	// ExternalURL is the website when Instagram serves it as such (REQ-008
	// fix 18); otherwise it is the first bio link.
	ExternalURL       *string    `json:"external_url,omitempty"`
	Verified          *bool      `json:"verified,omitempty"`
	Private           *bool      `json:"private,omitempty"`
	FollowerCount     *int64     `json:"follower_count,omitempty"`
	FollowingCount    *int64     `json:"following_count,omitempty"`
	MediaCount        *int64     `json:"media_count,omitempty"`
	ProfilePicURL     *string    `json:"profile_pic_url,omitempty"`
	Category          *string    `json:"category,omitempty"`
	IsBusiness        *bool      `json:"is_business,omitempty"`
	HighlightCount    *int32     `json:"highlight_count,omitempty"`
	Status            string     `json:"status"`
	FirstSeenAt       time.Time  `json:"first_seen_at"`
	LastScrapedAt     *time.Time `json:"last_scraped_at,omitempty"`
	LastProfileReadAt *time.Time `json:"last_profile_read_at,omitempty"`
	Schedule          *Schedule  `json:"schedule,omitempty"`
}

// Schedule is a registered profile's scheduled crawl.
type Schedule struct {
	ProfileRef      string `json:"profile_ref"`
	UserPK          *int64 `json:"user_pk,omitempty"`
	Enabled         bool   `json:"enabled"`
	IntervalSeconds int    `json:"interval_seconds"`
	Tier            string `json:"tier"`
	// Adaptive: placed by its posting rate (not pinned).
	Adaptive          bool            `json:"adaptive"`
	NextRunAt         time.Time       `json:"next_run_at"`
	NextProfileReadAt *time.Time      `json:"next_profile_read_at,omitempty"`
	LastRunAt         *time.Time      `json:"last_run_at,omitempty"`
	LastJobID         *uuid.UUID      `json:"last_job_id,omitempty"`
	Failures          int             `json:"consecutive_failures"`
	Options           json.RawMessage `json:"options,omitempty"`
}

func (s *Server) profileByPK(ctx context.Context, pk int64) (*Profile, error) {
	var p Profile
	err := s.ReadDB.QueryRow(ctx, `SELECT user_pk, username, full_name, biography, bio_links, external_url, verified, private,
		follower_count, following_count, media_count, profile_pic_url, category, is_business, highlight_count, status, first_seen_at,
		last_scraped_at, last_profile_read_at FROM profiles WHERE user_pk = $1 AND status <> 'suppressed'`, pk).
		Scan(&p.UserPK, &p.Username, &p.FullName, &p.Biography, &p.BioLinks, &p.ExternalURL, &p.Verified, &p.Private, &p.FollowerCount,
			&p.FollowingCount, &p.MediaCount, &p.ProfilePicURL, &p.Category, &p.IsBusiness, &p.HighlightCount, &p.Status,
			&p.FirstSeenAt, &p.LastScrapedAt, &p.LastProfileReadAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

const selectSchedule = `SELECT profile_ref, user_pk, enabled, interval_seconds, tier, adaptive, next_run_at, next_profile_read_at,
	last_run_at, last_job_id, consecutive_failures, options FROM profile_schedule`

func scanSchedule(row pgx.CollectableRow) (*Schedule, error) {
	var sc Schedule
	err := row.Scan(&sc.ProfileRef, &sc.UserPK, &sc.Enabled, &sc.IntervalSeconds, &sc.Tier, &sc.Adaptive, &sc.NextRunAt, &sc.NextProfileReadAt,
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
	Enabled *bool `json:"enabled,omitempty"`
	// Tier pins the profile to hot, warm or long_tail; auto lets its
	// posting rate place it. An interval (hours or minutes) instead is a
	// custom placement.
	Tier            *string `json:"tier,omitempty"`
	IntervalHours   *int    `json:"interval_hours,omitempty"`
	IntervalMinutes *int    `json:"interval_minutes,omitempty"`
}

type registerReq struct {
	Profile  string       `json:"profile"`
	Schedule *scheduleReq `json:"schedule,omitempty"`
	// Backfill runs a full crawl now. By default only for a profile that
	// has had no job yet, so registering a list again starts nothing twice.
	Backfill *bool `json:"backfill,omitempty"`
	CrawlOptions
}

// placement is the tier and interval sr asks for ("" and 0: auto), or nil
// when it asks for neither (a PATCH that leaves them).
func (s *Server) placement(sr *scheduleReq) (*dispatch.Placement, string, time.Duration, error) {
	if sr == nil || (sr.Tier == nil && sr.IntervalHours == nil && sr.IntervalMinutes == nil) {
		return nil, "", 0, nil
	}
	if sr.IntervalHours != nil && sr.IntervalMinutes != nil {
		return nil, "", 0, errors.New("give interval_hours or interval_minutes, not both")
	}
	var iv time.Duration
	switch {
	case sr.IntervalHours != nil:
		iv = time.Duration(*sr.IntervalHours) * time.Hour
		if iv <= 0 {
			return nil, "", 0, errors.New("interval_hours must be positive")
		}
	case sr.IntervalMinutes != nil:
		iv = time.Duration(*sr.IntervalMinutes) * time.Minute
		if iv <= 0 {
			return nil, "", 0, errors.New("interval_minutes must be positive")
		}
	}
	tier := ""
	if sr.Tier != nil {
		tier = *sr.Tier
	}
	pl, err := dispatch.Place(s.Cfg, tier, iv)
	return &pl, tier, iv, err
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
	reg, err := s.registration(r, req.Schedule, req.Backfill, req.CrawlOptions)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	reg.Ref, reg.Params = ref, map[string]any{"profile": req.Profile, "registered_by": reg.ClientID}
	res, err := s.Dispatch.Register(r.Context(), s.Cfg, reg)
	switch {
	case errors.Is(err, dispatch.ErrUnknownProfile):
		httpapi.Problem(w, http.StatusUnprocessableEntity, "unknown profile", err.Error())
		return
	case err != nil:
		httpapi.ServerError(w, err)
		return
	}
	out := map[string]any{"profile_ref": ref, "registered": true}
	if res.Backfill != nil {
		out["backfill_job"] = res.Backfill
	}
	code := http.StatusOK
	if res.Created {
		code = http.StatusCreated
	}
	httpapi.WriteJSON(w, code, out)
}

// registration is the caller's registration settings, validated.
func (s *Server) registration(r *http.Request, sc *scheduleReq, backfill *bool, o CrawlOptions) (dispatch.Registration, error) {
	if _, err := Limits(s.Cfg, o); err != nil {
		return dispatch.Registration{}, err
	}
	_, tier, iv, err := s.placement(sc)
	if err != nil {
		return dispatch.Registration{}, err
	}
	enabled := true
	if sc != nil && sc.Enabled != nil {
		enabled = *sc.Enabled
	}
	return dispatch.Registration{ClientID: httpapi.ClientFrom(r.Context()).ID, Tier: tier, Interval: iv, Enabled: enabled, Options: o,
		Backfill: backfill}, nil
}

// MaxBulkProfiles is the most profiles one bulk registration takes.
const MaxBulkProfiles = 1000

type bulkRegisterReq struct {
	// Profiles are usernames, @names or profile links.
	Profiles []string     `json:"profiles"`
	Schedule *scheduleReq `json:"schedule,omitempty"`
	Backfill *bool        `json:"backfill,omitempty"`
	CrawlOptions
}

// BulkResult says what a bulk registration did.
type BulkResult struct {
	Registered int `json:"registered"` // new on the schedule
	Updated    int `json:"updated"`    // already scheduled: settings updated, the caller registered
	Backfills  int `json:"backfills"`  // backfill jobs started
	// Invalid entries are neither usernames nor profile links; Failed ones
	// could not be registered (with why).
	Invalid []string          `json:"invalid"`
	Failed  map[string]string `json:"failed"`
}

// registerProfiles is registerProfile for a list (up to MaxBulkProfiles):
// the same schedule and options for each; duplicates count once.
func (s *Server) registerProfiles(w http.ResponseWriter, r *http.Request) {
	var req bulkRegisterReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	if len(req.Profiles) == 0 || len(req.Profiles) > MaxBulkProfiles {
		httpapi.Problem(w, http.StatusBadRequest, "invalid profiles", fmt.Sprintf("send 1 to %d profiles", MaxBulkProfiles))
		return
	}
	reg, err := s.registration(r, req.Schedule, req.Backfill, req.CrawlOptions)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	res := RegisterAll(r.Context(), s.Dispatch, s.Cfg, reg, req.Profiles)
	httpapi.WriteJSON(w, http.StatusOK, res)
}

// RegisterAll registers each of entries with reg's settings (the bulk API
// and igsctl profiles import).
func RegisterAll(ctx context.Context, d *dispatch.Dispatcher, cfg *config.Config, reg dispatch.Registration, entries []string) BulkResult {
	res := BulkResult{Invalid: []string{}, Failed: map[string]string{}}
	seen := map[string]bool{}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		ref, err := task.NormalizeRef(strings.TrimPrefix(e, "@"))
		if err != nil {
			res.Invalid = append(res.Invalid, e)
			continue
		}
		if seen[ref] {
			continue
		}
		seen[ref] = true
		r := reg
		r.Ref, r.Params = ref, map[string]any{"profile": e, "registered_by": reg.ClientID, "bulk": true}
		out, err := d.Register(ctx, cfg, r)
		if err != nil {
			res.Failed[e] = err.Error()
			continue
		}
		if out.Created {
			res.Registered++
		} else {
			res.Updated++
		}
		if out.Backfill != nil {
			res.Backfills++
		}
	}
	return res
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
		pl, _, _, err := s.placement(req.Schedule)
		if err != nil {
			httpapi.Problem(w, http.StatusBadRequest, "invalid schedule", err.Error())
			return
		}
		if pl != nil {
			if err := dispatch.SetPlacement(ctx, s.DB, []string{ref}, *pl); err != nil {
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
				if _, err := tx.Exec(ctx, `UPDATE discovery_candidates SET status = 'rejected', decided_at = now()
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
