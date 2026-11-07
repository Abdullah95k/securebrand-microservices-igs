package igdisc

import (
	"context"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// How an account was met near a keyword (igdisc_evidence.via, and
// Candidate.evidence.via later).
const (
	ViaCaption   = "caption"
	ViaComment   = "comment"
	ViaPostPlace = "post_place"
)

// OriginKeyword is the candidate origin Track 1 adds
// (ig.discovery_candidates.origins).
const OriginKeyword = "keyword"

// Config is Track 1's configuration (ig.igdisc in igs's config).
type Config struct {
	// Window: only posts and comments made this recently count, as
	// mentions (R8, 6 hours by default). Older content is ordinary network
	// discovery and adds no keyword evidence.
	Window time.Duration
	// FlushEvery is how often the recorder writes what it found.
	FlushEvery time.Duration
	// Priority is what each new piece of evidence adds to the candidate's
	// vet priority, so it is checked soon (2: as much as a sighting on a
	// post located in Iraq).
	Priority float64
	// Commenters: count comment authors. Only with privacy.pii_mode full,
	// as discovery itself requires.
	Commenters bool
}

// DefaultConfig is the plan's: a 6-hour window, writes every 10 seconds,
// priority 2 a piece of evidence.
func DefaultConfig() Config {
	return Config{Window: 6 * time.Hour, FlushEvery: 10 * time.Second, Priority: 2}
}

// Evidence is one account met near one keyword in one piece of content.
type Evidence struct {
	Username    string
	UserPK      int64
	Keyword     Keyword
	ContentID   string
	URL         string
	ContentTime time.Time
	Via         string
	SourcePK    int64
}

type evidenceKey struct{ username, keywordID, contentID string }

// Recorder is Track 1 (docs/master/igdisc-plan.md §3.1). A crawler hands
// it every post and every page of comments it emits, as it hands them to
// discovery's collector. It records, for each account met in a post or
// comment made within the window that names a keyword of the shared list,
// that keyword as evidence, and raises the account's vet priority so
// discovery checks it soon. It reads nothing from Instagram.
//
// Accounts already on the schedule, rejected ones and erased ones are
// skipped. What is held in memory when a crawler dies is lost, as with
// discovery's sightings; the same content is met again on the next read
// while it is still inside the window. A nil Recorder does nothing.
type Recorder struct {
	DB  *pgxpool.Pool // schema ig
	Cfg Config
	// Suppressed reports an erased account (by user pk): never recorded.
	Suppressed func(pk int64) bool
	// Now is the clock (time.Now when nil).
	Now func() time.Time

	index atomic.Pointer[Index]

	mu      sync.Mutex
	pending map[evidenceKey]Evidence
	kick    chan struct{}
}

// maxPending flushes early when this many pieces of evidence wait, and is
// the most a failed flush keeps for the next.
const maxPending = 20000

// NewRecorder returns a recorder matching the seed's keywords until
// UseIndex gives it the table's (WatchIndex).
func NewRecorder(db *pgxpool.Pool, cfg Config) (*Recorder, error) {
	def := DefaultConfig()
	if cfg.Window <= 0 {
		cfg.Window = def.Window
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = def.FlushEvery
	}
	if cfg.Priority <= 0 {
		cfg.Priority = def.Priority
	}
	seed, err := SeedIndex()
	if err != nil {
		return nil, err
	}
	r := &Recorder{DB: db, Cfg: cfg, pending: map[evidenceKey]Evidence{}, kick: make(chan struct{}, 1)}
	r.index.Store(seed)
	return r, nil
}

// UseIndex makes the recorder match x's keywords.
func (r *Recorder) UseIndex(x *Index) {
	if r != nil && x != nil {
		r.index.Store(x)
	}
}

func (r *Recorder) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// recent reports whether content made at t is inside the window.
func (r *Recorder) recent(t time.Time) bool {
	if t.IsZero() {
		return false
	}
	age := r.now().Sub(t)
	return age <= r.Cfg.Window && age >= -time.Hour // a little clock skew is fine
}

var usernameRe = regexp.MustCompile(`^[a-z0-9._]{1,30}$`)

func cleanUsername(u string) string {
	u = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(u), "@"))
	if !usernameRe.MatchString(u) {
		return ""
	}
	return u
}

// add holds one piece of evidence per keyword. Callers hold r.mu.
func (r *Recorder) add(username string, pk int64, kws []Keyword, e Evidence) {
	u := cleanUsername(username)
	if u == "" || (pk != 0 && pk == e.SourcePK) || (pk != 0 && r.Suppressed != nil && r.Suppressed(pk)) {
		return
	}
	e.Username, e.UserPK = u, pk
	for _, k := range kws {
		e.Keyword = k
		key := evidenceKey{u, k.ID, e.ContentID}
		if old, ok := r.pending[key]; ok && old.UserPK != 0 {
			continue
		}
		r.pending[key] = e
	}
}

func postURL(shortcode string) string {
	if shortcode == "" {
		return ""
	}
	return "https://www.instagram.com/p/" + shortcode + "/"
}

// Post records the accounts tagged, co-authoring or mentioned in a recent
// post whose caption or place names a keyword.
func (r *Recorder) Post(p *igv1.Post) {
	if r == nil || p == nil {
		return
	}
	at := p.GetTakenAt().AsTime()
	if p.GetTakenAt() == nil || !r.recent(at) {
		return
	}
	x := r.index.Load()
	capKW := x.Match(p.GetCaption())
	placeKW := x.Match(p.GetLocation().GetName())
	if len(capKW) == 0 && len(placeKW) == 0 {
		return
	}
	base := Evidence{ContentID: "ig:post:" + strconv.FormatInt(p.GetMediaPk(), 10), URL: postURL(p.GetShortcode()),
		ContentTime: at.UTC(), SourcePK: p.GetOwnerPk()}
	type met struct {
		username string
		pk       int64
	}
	var people []met
	for _, t := range p.GetUsertags() {
		people = append(people, met{t.GetUsername(), t.GetUserPk()})
	}
	for _, a := range p.GetCoauthors() {
		people = append(people, met{a.GetUsername(), a.GetUserPk()})
	}
	for _, m := range p.GetMentions() {
		people = append(people, met{m, 0})
	}
	if len(people) == 0 {
		return
	}
	r.mu.Lock()
	for _, m := range people {
		if len(capKW) > 0 {
			e := base
			e.Via = ViaCaption
			r.add(m.username, m.pk, capKW, e)
		}
		if len(placeKW) > 0 {
			e := base
			e.Via = ViaPostPlace
			r.add(m.username, m.pk, placeKW, e)
		}
	}
	full := len(r.pending) >= maxPending
	r.mu.Unlock()
	if full {
		r.wake()
	}
}

// Comments records the authors of recent comments that name a keyword on
// ownerPK's post. The post's owner's own comments do not count.
func (r *Recorder) Comments(ownerPK int64, cs []*igv1.Comment) {
	if r == nil || !r.Cfg.Commenters || len(cs) == 0 {
		return
	}
	// Match outside the lock: crawling tasks call this at once (up to the
	// pod's slots), and only adding to the batch needs to be serialised.
	type hit struct {
		username string
		pk       int64
		kws      []Keyword
		e        Evidence
	}
	x := r.index.Load()
	var hits []hit
	for _, cm := range cs {
		a := cm.GetAuthor()
		if a.GetUsername() == "" || cm.GetIsOp() || cm.GetRedacted() || cm.GetCreatedAt() == nil {
			continue
		}
		at := cm.GetCreatedAt().AsTime()
		if !r.recent(at) {
			continue
		}
		kws := x.Match(cm.GetText())
		if len(kws) == 0 {
			continue
		}
		src := ownerPK
		if src == 0 {
			src = cm.GetOwnerPk()
		}
		hits = append(hits, hit{a.GetUsername(), a.GetUserPk(), kws, Evidence{ContentID: "ig:comment:" + strconv.FormatInt(cm.GetCommentPk(), 10),
			ContentTime: at.UTC(), Via: ViaComment, SourcePK: src}})
	}
	if len(hits) == 0 {
		return
	}
	r.mu.Lock()
	for _, h := range hits {
		r.add(h.username, h.pk, h.kws, h.e)
	}
	full := len(r.pending) >= maxPending
	r.mu.Unlock()
	if full {
		r.wake()
	}
}

func (r *Recorder) wake() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run flushes every FlushEvery, and once more when ctx ends.
func (r *Recorder) Run(ctx context.Context) error {
	t := time.NewTicker(r.Cfg.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := r.Flush(fctx); err != nil {
				slog.Warn("igdisc: last flush failed", "err", err)
			}
			return nil
		case <-t.C:
		case <-r.kick:
		}
		if err := r.Flush(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("igdisc: flush failed", "err", err)
		}
	}
}

var evidenceTotal = func() *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "igs", Name: "igdisc_evidence_total",
		Help: "Instagram keyword discovery: new evidence recorded (an account met near a keyword in recent content), by how it was met (caption, comment, post_place) and the keyword's language."},
		[]string{"via", "language"})
	metrics.Registry.MustRegister(c)
	return c
}()

// Flush writes the evidence held since the last flush, in one transaction:
// each new piece in igdisc_evidence, unless its account is on the
// schedule or was rejected; then, for the accounts with new evidence, the
// candidate (ig.discovery_candidates), created if needed, with origin
// "keyword" and its vet priority raised by Cfg.Priority a piece. Evidence
// it could not write is kept for the next flush.
func (r *Recorder) Flush(ctx context.Context) error {
	r.mu.Lock()
	batch := r.pending
	r.pending = map[evidenceKey]Evidence{}
	r.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	n := len(batch)
	var (
		users, ids, terms, norms, langs, groups, contents, urls, vias = make([]string, 0, n), make([]string, 0, n),
			make([]string, 0, n), make([]string, 0, n), make([]string, 0, n), make([]string, 0, n), make([]string, 0, n),
			make([]string, 0, n), make([]string, 0, n)
		pks, sources = make([]int64, 0, n), make([]int64, 0, n)
		times        = make([]time.Time, 0, n)
	)
	for _, e := range batch {
		users = append(users, e.Username)
		pks = append(pks, e.UserPK)
		ids = append(ids, e.Keyword.ID)
		terms = append(terms, e.Keyword.Term)
		norms = append(norms, e.Keyword.Normalized)
		langs = append(langs, e.Keyword.Language)
		groups = append(groups, e.Keyword.Group)
		contents = append(contents, e.ContentID)
		urls = append(urls, e.URL)
		times = append(times, e.ContentTime)
		vias = append(vias, e.Via)
		sources = append(sources, e.SourcePK)
	}
	type added struct{ via, language string }
	var got []added
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH x AS (
				SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[],
					$8::text[], $9::text[], $10::timestamptz[], $11::text[], $12::bigint[])
					AS x(username, pk, keyword_id, keyword, normalized, language, grp, content_id, url, content_time, via, source_pk)
			), ins AS (
				INSERT INTO igdisc_evidence AS e (username, user_pk, keyword_id, keyword, normalized, language, grp, content_id, url,
					content_time, via, source_pk)
				SELECT x.username, NULLIF(x.pk, 0), x.keyword_id, x.keyword, x.normalized, x.language, x.grp, x.content_id, x.url,
					x.content_time, x.via, x.source_pk
				FROM x
				WHERE NOT EXISTS (SELECT 1 FROM profile_schedule s WHERE s.profile_ref = 'u:' || x.username
						OR (x.pk <> 0 AND s.user_pk = x.pk))
					AND NOT EXISTS (SELECT 1 FROM discovery_candidates c WHERE c.username = x.username
						AND c.status IN ('registered', 'rejected'))
				ON CONFLICT (username, keyword_id, content_id) DO UPDATE SET user_pk = COALESCE(e.user_pk, EXCLUDED.user_pk)
					WHERE e.user_pk IS NULL AND EXCLUDED.user_pk IS NOT NULL
				RETURNING username, user_pk, via, language, (xmax = 0) AS inserted
			), per AS (
				SELECT username, max(user_pk) AS pk, count(*) FILTER (WHERE inserted) AS n FROM ins GROUP BY username
			), cand AS (
				INSERT INTO discovery_candidates AS c (username, user_pk, priority, origins)
				SELECT username, pk, $13::float8 * n, '{keyword}' FROM per WHERE n > 0
				ON CONFLICT (username) DO UPDATE SET
					user_pk = COALESCE(c.user_pk, EXCLUDED.user_pk),
					priority = c.priority + EXCLUDED.priority,
					last_seen_at = now(),
					origins = CASE WHEN 'keyword' = ANY(c.origins) THEN c.origins ELSE c.origins || '{keyword}'::text[] END
			)
			SELECT via, language FROM ins WHERE inserted`,
			users, pks, ids, terms, norms, langs, groups, contents, urls, times, vias, sources, r.Cfg.Priority)
		if err != nil {
			return err
		}
		got, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (added, error) {
			var a added
			return a, row.Scan(&a.via, &a.language)
		})
		return err
	})
	if err != nil {
		r.mu.Lock()
		for k, e := range batch {
			if _, ok := r.pending[k]; !ok && len(r.pending) < maxPending {
				r.pending[k] = e
			}
		}
		r.mu.Unlock()
		return err
	}
	for _, a := range got {
		evidenceTotal.WithLabelValues(a.via, a.language).Inc()
	}
	return nil
}
