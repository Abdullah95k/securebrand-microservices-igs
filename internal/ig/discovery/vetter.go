package discovery

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

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
	// StatusSmall: in scope but under ig.discovery.min_followers, kept to
	// be vetted again (follower_rechecks); StatusRecheck: due for that.
	StatusSmall   = "small"
	StatusRecheck = "recheck"
)

// maxVetAttempts: a candidate whose vet job ended three times without a
// result is marked failed.
const maxVetAttempts = 3

// Vetter is profile discovery's leader loop (igs-scheduler): every
// ig.discovery.interval it decides the candidates whose vet came back,
// queues for another vet those seen again long after their decision, and
// starts vets for the most seen candidates, up to vet_in_flight at once,
// while regular collection keeps up (hold.go).
type Vetter struct {
	DB       *pgxpool.Pool // schema ig
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
	// Rests, when set, are the exit rests the crawlers share: vets wait
	// while most exits of a tier rest (ig.discovery.hold_resting_share).
	Rests RestLoader
	// Candidates, when set, publishes each decided candidate in the
	// contract's form (discovery.candidate.v1).
	Candidates CandidatePublisher
	// held is the last hold reason, for logging its changes.
	held string
	// reclassified: the candidates decided below only for the follower
	// floor were moved to small (once a process).
	reclassified bool
	// pagesListed is when the discovery pages were last listed; pageTurn
	// picks the next page's kind in turn.
	pagesListed time.Time
	pageTurn    int
}

// Tick runs one round.
func (v *Vetter) Tick(ctx context.Context) error {
	if err := v.decide(ctx); err != nil {
		return fmt.Errorf("discovery: decide: %w", err)
	}
	d := v.Cfg.IG.Discovery
	if d.RevetAfter > 0 {
		if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'new'
			WHERE (status IN ('below','private','gone') OR (status = 'small' AND $2)) AND decided_at < now() - $1::int * interval '1 second'
				AND last_seen_at > decided_at`, int(d.RevetAfter.Seconds()), len(d.FollowerRechecks) == 0); err != nil {
			return fmt.Errorf("discovery: revet: %w", err)
		}
	}
	if err := v.dueRechecks(ctx); err != nil {
		return fmt.Errorf("discovery: rechecks: %w", err)
	}
	if err := v.rescore(ctx); err != nil {
		return fmt.Errorf("discovery: rescore: %w", err)
	}
	if err := v.promote(ctx); err != nil {
		return fmt.Errorf("discovery: promote capped: %w", err)
	}
	reason, err := v.hold(ctx)
	if err != nil {
		return fmt.Errorf("discovery: schedule check: %w", err)
	}
	v.setHeld(reason)
	if reason == "" {
		if err := v.start(ctx); err != nil {
			return fmt.Errorf("discovery: start vets: %w", err)
		}
		if err := v.pages(ctx); err != nil {
			return fmt.Errorf("discovery: pages: %w", err)
		}
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
		relevant, _, err := v.relevant(ctx, vet, sig, score)
		if err != nil {
			return err
		}
		sb, _ := json.Marshal(sig)
		status := StatusBelow
		if vet.Found && !vet.Private && relevant && vet.Followers < d.MinFollowers {
			status = StatusSmall
		}
		if vet.Found && !vet.Private && relevant && vet.Followers >= d.MinFollowers {
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

// pages dispatches discovery's hashtag and location pages (D-055,
// ig.discovery.pages), one at a time: at most per_day a day, evenly
// spread, kinds in turn, the page due longest first. It runs only while
// vets may start (the holds), and each load takes discovery's exit share.
func (v *Vetter) pages(ctx context.Context) error {
	p := v.Cfg.IG.Discovery.Pages
	if p.PerDay <= 0 || len(p.Kinds) == 0 {
		return nil
	}
	if time.Since(v.pagesListed) > time.Hour {
		if err := v.listPages(ctx); err != nil {
			return err
		}
		v.pagesListed = time.Now()
	}
	var last *time.Time
	var today int
	if err := v.DB.QueryRow(ctx, `SELECT max(dispatched_at), count(*) FILTER (WHERE dispatched_at > now() - interval '1 day')
		FROM discovery_pages`).Scan(&last, &today); err != nil {
		return err
	}
	if today >= p.PerDay || (last != nil && time.Since(*last) < 24*time.Hour/time.Duration(p.PerDay)) {
		return nil
	}
	kind := p.Kinds[v.pageTurn%len(p.Kinds)]
	v.pageTurn++
	var k, key string
	err := v.DB.QueryRow(ctx, `SELECT kind, key FROM discovery_pages WHERE due_at <= now() AND kind = ANY($1)
		ORDER BY (kind = $2) DESC, due_at LIMIT 1`, p.Kinds, kind).Scan(&k, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	lim, err := dispatch.Limits(v.Cfg, dispatch.CrawlOptions{})
	if err != nil {
		return err
	}
	if _, _, err := v.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindDiscovery, Lane: igstaskv1.Lane_LANE_P2, Limits: lim,
		Explore: &dispatch.Explore{Kind: k, Key: key}, Params: map[string]any{"page": k}}); err != nil {
		slog.Warn("discovery: page not started", "kind", k, "err", err)
		return nil
	}
	// Not due again until its load reports (the crawler sets the next).
	_, err = v.DB.Exec(ctx, `UPDATE discovery_pages SET dispatched_at = now(), due_at = now() + $3::int * interval '1 second'
		WHERE kind = $1 AND key = $2`, k, key, int(max(p.Revisit, time.Hour).Seconds()))
	return err
}

// listPages adds the pages discovery may load: Iraq's place names from the
// keyword table (each group's first enabled one-word term in English and
// in Arabic) and the configured hashtags, and the locations in Iraq most
// used by the posts of the last 90 days. Pages already listed keep their
// turn.
func (v *Vetter) listPages(ctx context.Context) error {
	p := v.Cfg.IG.Discovery.Pages
	var kinds, keys []string
	if slices.Contains(p.Kinds, "hashtag") {
		rows, err := v.DB.Query(ctx, `SELECT DISTINCT ON (grp, language) lower(term) FROM discovery_keywords
			WHERE enabled AND language IN ('en', 'ar') AND term !~ '[[:space:]''-]' ORDER BY grp, language, id`)
		if err != nil {
			return err
		}
		tags, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, h := range append(tags, p.Hashtags...) {
			if h = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(h), "#")); h != "" {
				kinds, keys = append(kinds, "hashtag"), append(keys, h)
			}
		}
	}
	if slices.Contains(p.Kinds, "location") && p.Locations > 0 {
		rows, err := v.DB.Query(ctx, `SELECT location->>'pk', coalesce(location->>'name', ''), (location->>'lat')::float8, (location->>'lng')::float8
			FROM posts WHERE location ? 'pk' AND taken_at > now() - interval '90 days'
			GROUP BY 1, 2, 3, 4 ORDER BY count(*) DESC LIMIT $1`, 10*p.Locations)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var pk, name string
			var lat, lng *float64
			if err := rows.Scan(&pk, &name, &lat, &lng); err != nil {
				rows.Close()
				return err
			}
			if n < p.Locations && pk != "" && pk != "0" && InIraq(name, lat, lng) {
				kinds, keys = append(kinds, "location"), append(keys, pk)
				n++
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	if len(keys) == 0 {
		return nil
	}
	_, err := v.DB.Exec(ctx, `INSERT INTO discovery_pages (kind, key) SELECT * FROM unnest($1::text[], $2::text[])
		ON CONFLICT DO NOTHING`, kinds, keys)
	return err
}

// recheckSlots is how many vets re-vets of small candidates may hold at
// once: share of vetInFlight, at least one when share > 0.
func recheckSlots(vetInFlight int, share float64) int {
	if share <= 0 || vetInFlight <= 0 {
		return 0
	}
	return max(1, int(math.Floor(float64(vetInFlight)*share)))
}

// dueRechecks queues for a re-vet the small candidates whose wait is over
// (ig.discovery.follower_rechecks): the n-th wait after their n-th small
// decision, the last repeating, or the first once they are seen again.
// Once a process, it first moves to small the candidates decided below
// only for the follower floor (the floor went live before this status).
func (v *Vetter) dueRechecks(ctx context.Context) error {
	d := v.Cfg.IG.Discovery
	if !v.reclassified {
		if d.MinFollowers > 0 {
			if _, err := v.DB.Exec(ctx, `UPDATE discovery_candidates SET status = 'small'
				WHERE status = 'below' AND score >= $1 AND vet IS NOT NULL AND coalesce((vet->>'found')::boolean, false)
					AND NOT coalesce((vet->>'private')::boolean, false) AND coalesce((vet->>'followers')::bigint, 0) < $2`,
				d.Threshold, d.MinFollowers); err != nil {
				return err
			}
		}
		v.reclassified = true
	}
	if len(d.FollowerRechecks) == 0 {
		return nil
	}
	waits := make([]int, len(d.FollowerRechecks))
	for i, w := range d.FollowerRechecks {
		waits[i] = int(w.Seconds())
	}
	_, err := v.DB.Exec(ctx, `UPDATE discovery_candidates c SET status = 'recheck'
		WHERE c.status = 'small' AND c.decided_at < now() - $1::int * interval '1 second'
			AND (c.last_seen_at > c.decided_at OR c.decided_at < now() - ($2::int[])[least(greatest(
				(SELECT count(*) FROM discovery_vets x WHERE x.username = c.username AND x.status = 'small'), 1), cardinality($2::int[]))::int]
				* interval '1 second')`, waits[0], waits)
	return err
}

// relevant reports whether a candidate is in scope: at the threshold, or,
// public and below it, with an Iraqi audience in its comments (D-055,
// ig.discovery.audience): share or more of min_commenters or more
// distinct commenters are Iraqi. The share goes with the signals
// (SignalIraqiCommenters) whenever commenters were read. The world-star
// cap does not apply to this test.
// The test's result ("" when it did not apply) is for
// igs_discovery_audience_checks_total.
func (v *Vetter) relevant(ctx context.Context, vet Vet, sig Signals, score float64) (bool, string, error) {
	if score >= v.Cfg.IG.Discovery.Threshold {
		return true, "", nil
	}
	a := v.Cfg.IG.Discovery.Audience
	if a.Share <= 0 || !vet.Found || vet.Private || vet.Audience == nil || vet.Followers < a.MinFollowers {
		return false, "", nil
	}
	if len(vet.Audience.Commenters) == 0 {
		return false, "none", nil
	}
	var pks []int64
	var refs, names []string
	for _, c := range vet.Audience.Commenters {
		if c.PK != 0 {
			pks = append(pks, c.PK)
		}
		if c.Username != "" {
			u := strings.ToLower(c.Username)
			names = append(names, u)
			refs = append(refs, "u:"+u)
		}
	}
	knownPK, knownName := map[int64]bool{}, map[string]bool{}
	rows, err := v.DB.Query(ctx, `SELECT coalesce(user_pk, 0), profile_ref FROM profile_schedule WHERE user_pk = ANY($1) OR profile_ref = ANY($2)
		UNION ALL SELECT coalesce(user_pk, 0), 'u:' || username FROM discovery_candidates WHERE status = 'registered' AND username = ANY($3)`,
		pks, refs, names)
	if err != nil {
		return false, "", err
	}
	for rows.Next() {
		var pk int64
		var ref string
		if err := rows.Scan(&pk, &ref); err != nil {
			rows.Close()
			return false, "", err
		}
		if pk != 0 {
			knownPK[pk] = true
		}
		knownName[strings.TrimPrefix(ref, "u:")] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	share, n := vet.Audience.IraqiShare(func(pk int64, u string) bool { return (pk != 0 && knownPK[pk]) || (u != "" && knownName[u]) })
	sig[SignalIraqiCommenters] = math.Round(share*100) / 100
	switch {
	case n < a.MinCommenters:
		return false, "too_few", nil
	case share < a.Share:
		return false, "below_share", nil
	}
	return true, "in_scope", nil
}

// decide scores the candidates whose vet came back and registers those at
// the threshold; a vet job that ended without a result sends its candidate
// back to the queue.
func (v *Vetter) decide(ctx context.Context) error {
	rows, err := v.DB.Query(ctx, `SELECT c.username, coalesce(c.user_pk, 0), c.vet, c.tags, c.coauthors, c.mentions, c.comments,
			c.iraq_context, c.sources, c.attempts, c.origins, c.first_seen_at
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
		origins  []string
		seen     time.Time
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.username, &x.pk, &x.vet, &x.s.Tags, &x.s.Coauthors, &x.s.Mentions, &x.s.Comments, &x.s.IraqContext, &x.s.Sources,
			&x.attempts, &x.origins, &x.seen)
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
			if status == StatusFailed {
				if err := v.publish(ctx, x.username, x.pk, nil, x.s, x.origins, status, 0, false, x.seen); err != nil {
					return err
				}
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
		relevant, check, err := v.relevant(ctx, vet, sig, score)
		if err != nil {
			return err
		}
		if check != "" {
			metrics.DiscoveryAudience.WithLabelValues(check).Inc()
		}
		status := StatusBelow
		switch {
		case !vet.Found:
			status = StatusGone
		case vet.Private:
			status = StatusPrivate
		case relevant && vet.Followers < d.MinFollowers:
			status = StatusSmall
		case relevant:
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
		if err := v.publish(ctx, x.username, pk, &vet, x.s, x.origins, status, score, relevant, x.seen); err != nil {
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
	type cand struct {
		username string
		pk       int64
		recheck  bool
	}
	pick := func(sql string, args ...any) ([]cand, error) {
		rows, err := v.DB.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, func(r pgx.CollectableRow) (cand, error) {
			var c cand
			return c, r.Scan(&c.username, &c.pk, &c.recheck)
		})
	}
	// Re-vets of small candidates first, within their share of the slots
	// (those already running count against it), oldest decision first.
	var list []cand
	if slots := recheckSlots(d.VetInFlight, d.RecheckShare); slots > 0 {
		var running int
		if err := v.DB.QueryRow(ctx, `SELECT count(*) FROM discovery_candidates c WHERE c.status = 'vetting'
				AND EXISTS (SELECT 1 FROM discovery_vets x WHERE x.username = c.username AND x.status = 'small')`).Scan(&running); err != nil {
			return err
		}
		if n := min(free, slots-running); n > 0 {
			var err error
			if list, err = pick(`SELECT username, coalesce(user_pk, 0), true FROM discovery_candidates
				WHERE status = 'recheck' ORDER BY decided_at LIMIT $1`, n); err != nil {
				return err
			}
		}
	}
	if n := free - len(list); n > 0 {
		news, err := pick(`SELECT username, coalesce(user_pk, 0), false FROM discovery_candidates
			WHERE status = 'new' AND priority >= $1 ORDER BY priority DESC LIMIT $2`, d.MinPriority, n)
		if err != nil {
			return err
		}
		list = append(list, news...)
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
		if c.recheck {
			metrics.DiscoveryRechecks.Inc()
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
	for _, s := range []string{StatusNew, StatusVetting, StatusRegistered, StatusCapped, StatusBelow, StatusSmall, StatusRecheck, StatusPrivate,
		StatusGone, StatusFailed, StatusRejected} {
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

// publish sends a decided candidate on discovery.candidate.v1, when the
// scheduler set a publisher. A failure leaves the decision stored and
// retries nothing: the candidate is published again when it is next
// decided.
func (v *Vetter) publish(ctx context.Context, username string, pk int64, vet *Vet, s Sightings, origins []string, status string,
	score float64, relevant bool, firstSeen time.Time) error {
	if v.Candidates == nil {
		return nil
	}
	key, c, ok := CandidateEvent(username, pk, vet, s, origins, status, score, relevant, firstSeen, time.Now())
	if !ok {
		return nil
	}
	if err := v.Candidates(ctx, key, c); err != nil {
		slog.Warn("discovery: candidate not published", plog.Account(pk, "u:"+username), "status", status, "err", err)
	}
	return nil
}
