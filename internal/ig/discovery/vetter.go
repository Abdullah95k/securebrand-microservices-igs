package discovery

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Candidate statuses.
const (
	StatusNew        = "new"
	StatusVetting    = "vetting"
	StatusRegistered = "registered"
	StatusBelow      = "below"
	StatusPrivate    = "private"
	StatusGone       = "gone"
	StatusFailed     = "failed"
	StatusRejected   = "rejected"
	// StatusCapped: at the threshold, waiting for room under
	// ig.discovery.max_new_per_day.
	StatusCapped = "capped"
)

// maxVetAttempts: a candidate whose vet job ended three times without a
// result is marked failed.
const maxVetAttempts = 3

// Vetter is profile discovery's leader loop (igs-scheduler): every
// ig.discovery.interval it decides the candidates whose vet came back,
// queues for another vet those seen again long after their decision, and
// starts vets for the most seen candidates, up to vet_in_flight at once.
type Vetter struct {
	DB       *pgxpool.Pool // schema ig
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
}

// Tick runs one round.
func (v *Vetter) Tick(ctx context.Context) error {
	if err := v.decide(ctx); err != nil {
		return fmt.Errorf("discovery: decide: %w", err)
	}
	d := v.Cfg.IG.Discovery
	if d.RevetAfter > 0 {
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'new'
			WHERE status IN ('below','private','gone') AND decided_at < now() - $1::int * interval '1 second'
				AND last_seen_at > decided_at`, int(d.RevetAfter.Seconds())); err != nil {
			return fmt.Errorf("discovery: revet: %w", err)
		}
	}
	if err := v.rescore(ctx); err != nil {
		return fmt.Errorf("discovery: rescore: %w", err)
	}
	if err := v.promote(ctx); err != nil {
		return fmt.Errorf("discovery: promote capped: %w", err)
	}
	if err := v.start(ctx); err != nil {
		return fmt.Errorf("discovery: start vets: %w", err)
	}
	return v.gauges(ctx)
}

// rescore scores again, from their stored read, the candidates below the
// threshold whose Iraqi audience grew since their decision (more registered
// accounts link to them): no new read of Instagram is needed. Those now at
// the threshold are registered.
func (v *Vetter) rescore(ctx context.Context) error {
	rows, err := v.DB.Query(ctx, `SELECT username, coalesce(user_pk, 0), vet, tags, coauthors, mentions, comments, iraq_context, sources
		FROM discovery_candidates WHERE status = 'below' AND vet IS NOT NULL AND sources > sources_at_decision LIMIT 1000`)
	if err != nil {
		return err
	}
	type row struct {
		username string
		pk       int64
		vet      []byte
		s        Sightings
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.username, &x.pk, &x.vet, &x.s.Tags, &x.s.Coauthors, &x.s.Mentions, &x.s.Comments, &x.s.IraqContext, &x.s.Sources)
	})
	if err != nil {
		return err
	}
	d := v.Cfg.IG.Discovery
	for _, x := range list {
		var vet Vet
		if err := json.Unmarshal(x.vet, &vet); err != nil {
			continue
		}
		pk := x.pk
		if vet.UserPK != 0 {
			pk = vet.UserPK
		}
		if gone, err := v.forgetSuppressed(ctx, x.username, pk); err != nil || gone {
			if err != nil {
				return err
			}
			continue
		}
		sig := Score(vet, x.s)
		score := sig.Sum()
		sb, _ := json.Marshal(sig)
		status := StatusBelow
		if vet.Found && !vet.Private && score >= d.Threshold && vet.Followers >= d.MinFollowers {
			ok, err := v.room(ctx)
			if err != nil {
				return err
			}
			status = StatusCapped
			if ok {
				status = StatusRegistered
				if err := v.registerKept(ctx, x.username, pk, score, sb, x.s, x.vet); err != nil {
					return err
				}
				slog.Info("discovery: candidate registered on a grown audience", plog.Account(x.pk, "u:"+x.username), "score", score, "sources", x.s.Sources)
			}
			metrics.DiscoveryVets.WithLabelValues("rescored_" + status).Inc()
		}
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = $2, score = $3, signals = $4, sources_at_decision = $5,
				decided_at = CASE WHEN $2 = 'below' THEN decided_at ELSE now() END
			WHERE username = $1`, x.username, status, score, sb, x.s.Sources); err != nil {
			return err
		}
	}
	return nil
}

// decide scores the candidates whose vet came back and registers those at
// the threshold; a vet job that ended without a result sends its candidate
// back to the queue.
func (v *Vetter) decide(ctx context.Context) error {
	rows, err := v.DB.Query(ctx, `SELECT c.username, coalesce(c.user_pk, 0), c.vet, c.tags, c.coauthors, c.mentions, c.comments,
			c.iraq_context, c.sources, c.attempts
		FROM discovery_candidates c LEFT JOIN jobs j ON j.job_id = c.vet_job_id
		WHERE c.status = 'vetting' AND (c.vet IS NOT NULL OR j.job_id IS NULL OR j.status IN ('completed','partial','failed','cancelled'))
		LIMIT 1000`)
	if err != nil {
		return err
	}
	type row struct {
		username string
		pk       int64
		vet      []byte
		s        Sightings
		attempts int
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.username, &x.pk, &x.vet, &x.s.Tags, &x.s.Coauthors, &x.s.Mentions, &x.s.Comments, &x.s.IraqContext, &x.s.Sources,
			&x.attempts)
	})
	if err != nil {
		return err
	}
	d := v.Cfg.IG.Discovery
	for _, x := range list {
		if x.vet == nil {
			status := StatusNew
			if x.attempts >= maxVetAttempts {
				status = StatusFailed
				metrics.DiscoveryVets.WithLabelValues(StatusFailed).Inc()
			}
			if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = $2, decided_at = CASE WHEN $2 = 'failed' THEN now() END
				WHERE username = $1`, x.username, status); err != nil {
				return err
			}
			continue
		}
		var vet Vet
		if err := json.Unmarshal(x.vet, &vet); err != nil {
			slog.Warn("discovery: unreadable vet", plog.Account(x.pk, "u:"+x.username), "err", err)
			vet = Vet{}
		}
		pk := x.pk
		if vet.UserPK != 0 {
			pk = vet.UserPK
		}
		if gone, err := v.forgetSuppressed(ctx, x.username, pk); err != nil || gone {
			if err != nil {
				return err
			}
			continue
		}
		sig := Score(vet, x.s)
		score := sig.Sum()
		status := StatusBelow
		switch {
		case !vet.Found:
			status = StatusGone
		case vet.Private:
			status = StatusPrivate
		case score >= d.Threshold && vet.Followers >= d.MinFollowers:
			ok, err := v.room(ctx)
			if err != nil {
				return err
			}
			status = StatusCapped
			if ok {
				status = StatusRegistered
			}
		}
		if status == StatusRegistered {
			if err := Register(ctx, v.DB, v.Dispatch, v.Cfg, x.username, pk); err != nil {
				return fmt.Errorf("register %s: %w", plog.AccountName(pk, "u:"+x.username), err)
			}
		}
		metrics.DiscoveryVets.WithLabelValues(status).Inc()
		sb, _ := json.Marshal(sig)
		seen, _ := json.Marshal(x.s)
		var upk *int64
		if pk != 0 {
			upk = &pk
		}
		// Data is kept forever: the read stays on the candidate (the latest)
		// and in discovery_vets (every one).
		if _, err := v.DB.Exec(ctx, `INSERT INTO discovery_vets (username, vetted_at, user_pk, status, score, signals, sightings, vet)
			VALUES ($1, now(), $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`, x.username, upk, status, score, sb, seen, x.vet); err != nil {
			return err
		}
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = $2, score = $3, signals = $4,
				user_pk = COALESCE(NULLIF($5::bigint, 0), user_pk), sources_at_decision = $6, decided_at = now()
			WHERE username = $1`, x.username, status, score, sb, pk, x.s.Sources); err != nil {
			return err
		}
		slog.Info("discovery: candidate decided", plog.Account(pk, "u:"+x.username), "status", status, "score", score)
	}
	return nil
}

// start dispatches vets for the most seen new candidates, up to
// vet_in_flight vetting at once. A candidate already on the schedule is
// marked registered without a vet.
func (v *Vetter) start(ctx context.Context) error {
	d := v.Cfg.IG.Discovery
	var inflight int
	if err := v.DB.QueryRow(ctx, `SELECT count(*) FROM discovery_candidates WHERE status = 'vetting'`).Scan(&inflight); err != nil {
		return err
	}
	free := d.VetInFlight - inflight
	if free <= 0 {
		return nil
	}
	rows, err := v.DB.Query(ctx, `SELECT username, coalesce(user_pk, 0) FROM discovery_candidates
		WHERE status = 'new' AND priority >= $1 ORDER BY priority DESC LIMIT $2`, d.MinPriority, free)
	if err != nil {
		return err
	}
	type cand struct {
		username string
		pk       int64
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (cand, error) {
		var c cand
		return c, r.Scan(&c.username, &c.pk)
	})
	if err != nil {
		return err
	}
	lim, err := dispatch.Limits(v.Cfg, dispatch.CrawlOptions{})
	if err != nil {
		return err
	}
	for _, c := range list {
		if gone, err := v.forgetSuppressed(ctx, c.username, c.pk); err != nil || gone {
			if err != nil {
				return err
			}
			continue
		}
		var scheduled bool
		if err := v.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM profile_schedule WHERE profile_ref = 'u:' || $1
				OR ($2::bigint <> 0 AND user_pk = $2))`, c.username, c.pk).Scan(&scheduled); err != nil {
			return err
		}
		if scheduled {
			metrics.DiscoveryVets.WithLabelValues("already_registered").Inc()
			if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'registered', decided_at = now() WHERE username = $1`,
				c.username); err != nil {
				return err
			}
			continue
		}
		j, _, err := v.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindDiscovery, Lane: igstaskv1.Lane_LANE_P2, Mode: "full",
			ProfileRef: "u:" + c.username, Limits: lim, Vet: true, Params: map[string]any{"candidate": c.username}})
		if err != nil {
			slog.Warn("discovery: vet not started", plog.Account(c.pk, "u:"+c.username), "err", err)
			continue
		}
		// The previous read is in discovery_vets; the candidate's vet column
		// clears so the new read can be told apart.
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'vetting', vet_job_id = $2, vet = NULL,
				attempts = attempts + 1 WHERE username = $1`, c.username, j.ID); err != nil {
			return err
		}
	}
	return nil
}

// room reports whether discovery may register one more profile today under
// ig.discovery.max_new_per_day (counted from today's registered decisions
// in discovery_vets, by UTC day).
func (v *Vetter) room(ctx context.Context) (bool, error) {
	limit := v.Cfg.IG.Discovery.MaxNewPerDay
	if limit <= 0 {
		return true, nil
	}
	var n int
	err := v.DB.QueryRow(ctx, `SELECT count(*) FROM discovery_vets WHERE status = 'registered'
		AND vetted_at >= date_trunc('day', now(), 'UTC')`).Scan(&n)
	return n < limit, err
}

// registerKept registers a candidate decided on a stored read (a rescore,
// or a capped one let through) and keeps the decision in discovery_vets.
func (v *Vetter) registerKept(ctx context.Context, username string, pk int64, score float64, signals []byte, s Sightings, vet []byte) error {
	if err := Register(ctx, v.DB, v.Dispatch, v.Cfg, username, pk); err != nil {
		return fmt.Errorf("register %s: %w", plog.AccountName(pk, "u:"+username), err)
	}
	seen, _ := json.Marshal(s)
	var upk *int64
	if pk != 0 {
		upk = &pk
	}
	_, err := v.DB.Exec(ctx, `INSERT INTO discovery_vets (username, vetted_at, user_pk, status, score, signals, sightings, vet)
		VALUES ($1, now(), $2, 'registered', $3, $4, $5, $6) ON CONFLICT DO NOTHING`, username, upk, score, signals, seen, vet)
	return err
}

// promote registers capped candidates, highest score first, while today's
// cap has room.
func (v *Vetter) promote(ctx context.Context) error {
	if v.Cfg.IG.Discovery.MaxNewPerDay <= 0 {
		// No cap (any more): every capped candidate goes through.
		_, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'below', sources_at_decision = -1 WHERE status = 'capped'`)
		return err
	}
	rows, err := v.DB.Query(ctx, `SELECT username, coalesce(user_pk, 0), score, signals, vet, tags, coauthors, mentions, comments,
			iraq_context, sources
		FROM discovery_candidates WHERE status = 'capped' ORDER BY score DESC, priority DESC LIMIT 100`)
	if err != nil {
		return err
	}
	type row struct {
		username     string
		pk           int64
		score        float64
		signals, vet []byte
		s            Sightings
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.username, &x.pk, &x.score, &x.signals, &x.vet, &x.s.Tags, &x.s.Coauthors, &x.s.Mentions, &x.s.Comments,
			&x.s.IraqContext, &x.s.Sources)
	})
	if err != nil {
		return err
	}
	for _, x := range list {
		ok, err := v.room(ctx)
		if err != nil || !ok {
			return err
		}
		if err := v.registerKept(ctx, x.username, x.pk, x.score, x.signals, x.s, x.vet); err != nil {
			return err
		}
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'registered', decided_at = now() WHERE username = $1`,
			x.username); err != nil {
			return err
		}
		metrics.DiscoveryVets.WithLabelValues("capped_registered").Inc()
	}
	return nil
}

// forgetSuppressed deletes the candidate and its vets when its account was
// erased (as a profile or as a commenter): it is never vetted, registered
// or kept.
func (v *Vetter) forgetSuppressed(ctx context.Context, username string, pk int64) (bool, error) {
	if pk == 0 {
		return false, nil
	}
	var suppressed bool
	err := v.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM suppression WHERE (subject_type = 'profile' AND subject_key = $1)
		OR (subject_type = 'author' AND subject_key = $2))`,
		strconv.FormatInt(pk, 10), hex.EncodeToString(emit.AuthorKey([]byte(v.Cfg.IG.Privacy.AuthorHMACKey), pk))).Scan(&suppressed)
	if err != nil || !suppressed {
		return false, err
	}
	if _, err = v.DB.Exec(ctx, `DELETE FROM discovery_vets WHERE username = $1 OR user_pk = $2`, username, pk); err != nil {
		return true, err
	}
	if _, err = v.DB.Exec(ctx, `DELETE FROM discovery_sources WHERE username = $1`, username); err != nil {
		return true, err
	}
	_, err = v.DB.Exec(ctx, `DELETE FROM discovery_candidates WHERE username = $1`, username)
	return true, err
}

// gauges counts the candidates by status (new: those due a vet).
func (v *Vetter) gauges(ctx context.Context) error {
	rows, err := v.DB.Query(ctx, `SELECT status, count(*) FROM discovery_candidates
		WHERE status <> 'new' OR priority >= $1 GROUP BY status`, v.Cfg.IG.Discovery.MinPriority)
	if err != nil {
		return err
	}
	counts := map[string]float64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			rows.Close()
			return err
		}
		counts[s] = float64(n)
	}
	rows.Close()
	for _, s := range []string{StatusNew, StatusVetting, StatusRegistered, StatusCapped, StatusBelow, StatusPrivate, StatusGone, StatusFailed,
		StatusRejected} {
		metrics.DiscoveryQueue.WithLabelValues(s).Set(counts[s])
	}
	return rows.Err()
}

// ClientName is the API client discovered profiles are registered under.
// It holds no usable key.
const ClientName = "discovery"

// Client returns the discovery API client's id, creating it on first use.
func Client(ctx context.Context, db *pgxpool.Pool) (uuid.UUID, error) {
	return dispatch.SystemClient(ctx, db, ClientName)
}

// Register puts username on the schedule under the discovery client and
// starts its backfill (the vetter, and the API's accept).
func Register(ctx context.Context, db *pgxpool.Pool, d *dispatch.Dispatcher, cfg *config.Config, username string, pk int64) error {
	client, err := Client(ctx, db)
	if err != nil {
		return err
	}
	ref := "u:" + username
	_, err = d.Register(ctx, cfg, dispatch.Registration{ClientID: client, Ref: ref, Enabled: true, KeepSchedule: true,
		BackfillOwner: &client, Params: map[string]any{"profile": username, "discovered": true}})
	if err == nil && pk != 0 {
		// A discovered account is known by its pk already.
		_, err = db.Exec(ctx, `UPDATE profile_schedule SET user_pk = $2 WHERE profile_ref = $1 AND user_pk IS NULL`, ref, pk)
	}
	return err
}
