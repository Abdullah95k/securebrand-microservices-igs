package discovery

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Collector counts the accounts a crawler meets in the posts it reads and
// writes them to ig.discovery_candidates every FlushEvery. Counts held in
// memory when a crawler dies are lost: sightings are a priority, not a
// record. A nil Collector does nothing.
type Collector struct {
	DB *pgxpool.Pool
	// Suppressed reports an erased account (by user pk): it is never a
	// candidate.
	Suppressed func(pk int64) bool

	sources map[string]bool
	every   time.Duration

	mu      sync.Mutex
	pending map[string]*sighting
	kick    chan struct{}
}

type sighting struct {
	pk int64
	Sightings
}

// maxPending flushes early when this many accounts wait.
const maxPending = 20000

// NewCollector returns a collector for cfg's sources. Commenters are
// collected only when piiFull: with pseudonymized authors, igs keeps no
// commenter's name, and discovery does not either.
func NewCollector(db *pgxpool.Pool, cfg config.IGDiscovery, piiFull bool) *Collector {
	c := &Collector{DB: db, sources: map[string]bool{}, every: cfg.FlushEvery, pending: map[string]*sighting{}, kick: make(chan struct{}, 1)}
	for _, s := range cfg.Sources {
		if s != "commenters" || piiFull {
			c.sources[s] = true
		}
	}
	if c.every <= 0 {
		c.every = 10 * time.Second
	}
	return c
}

var usernameRe = regexp.MustCompile(`^[a-z0-9._]{1,30}$`)

// add counts one sighting of username. Callers hold c.mu.
func (c *Collector) add(username string, pk, ownerPK int64, iraq bool, source string, bump func(*Sightings)) {
	u := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(username), "@"))
	if !usernameRe.MatchString(u) || (pk != 0 && pk == ownerPK) || (pk != 0 && c.Suppressed != nil && c.Suppressed(pk)) {
		return
	}
	s := c.pending[u]
	if s == nil {
		s = &sighting{}
		c.pending[u] = s
	}
	if pk != 0 {
		s.pk = pk
	}
	bump(&s.Sightings)
	if iraq {
		s.IraqContext++
	}
	metrics.DiscoverySightings.WithLabelValues(source).Inc()
}

// Post counts the post's tagged accounts, co-authors and mentions, each
// once per post. A post located in Iraq counts for each as Iraqi context.
func (c *Collector) Post(p *igv1.Post) {
	if c == nil || p == nil {
		return
	}
	iraq := false
	if l := p.GetLocation(); l != nil {
		iraq = InIraq(l.GetName(), l.Lat, l.Lng)
	}
	c.mu.Lock()
	seen := map[string]bool{}
	once := func(kind, u string) bool {
		k := kind + ":" + strings.ToLower(u)
		if seen[k] {
			return false
		}
		seen[k] = true
		return true
	}
	if c.sources["tags"] {
		for _, t := range p.GetUsertags() {
			if once("tag", t.GetUsername()) {
				c.add(t.GetUsername(), t.GetUserPk(), p.GetOwnerPk(), iraq, "tags", func(s *Sightings) { s.Tags++ })
			}
		}
	}
	if c.sources["coauthors"] {
		for _, a := range p.GetCoauthors() {
			if once("coauthor", a.GetUsername()) {
				c.add(a.GetUsername(), a.GetUserPk(), p.GetOwnerPk(), iraq, "coauthors", func(s *Sightings) { s.Coauthors++ })
			}
		}
	}
	if c.sources["mentions"] {
		for _, m := range p.GetMentions() {
			if once("mention", m) {
				c.add(m, 0, p.GetOwnerPk(), iraq, "mentions", func(s *Sightings) { s.Mentions++ })
			}
		}
	}
	full := len(c.pending) >= maxPending
	c.mu.Unlock()
	if full {
		c.wake()
	}
}

// Comments counts the comments' authors (not the post's owner).
func (c *Collector) Comments(ownerPK int64, cs []*igv1.Comment) {
	if c == nil || !c.sources["commenters"] || len(cs) == 0 {
		return
	}
	c.mu.Lock()
	for _, cm := range cs {
		a := cm.GetAuthor()
		if a.GetUsername() == "" || cm.GetIsOp() {
			continue
		}
		c.add(a.GetUsername(), a.GetUserPk(), ownerPK, false, "commenters", func(s *Sightings) { s.Comments++ })
	}
	full := len(c.pending) >= maxPending
	c.mu.Unlock()
	if full {
		c.wake()
	}
}

func (c *Collector) wake() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Run flushes every FlushEvery, and once more when ctx ends.
func (c *Collector) Run(ctx context.Context) error {
	t := time.NewTicker(c.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := c.Flush(fctx); err != nil {
				slog.Warn("discovery: last flush failed", "err", err)
			}
			return nil
		case <-t.C:
		case <-c.kick:
		}
		if err := c.Flush(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("discovery: flush failed", "err", err)
		}
	}
}

// Flush writes the sightings gathered since the last flush. Those it could
// not write are kept for the next.
func (c *Collector) Flush(ctx context.Context) error {
	c.mu.Lock()
	batch := c.pending
	c.pending = map[string]*sighting{}
	c.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	n := len(batch)
	users := make([]string, 0, n)
	pks := make([]int64, 0, n)
	var tags, coauthors, mentions, comments, iraq []int32
	for u, s := range batch {
		users = append(users, u)
		pks = append(pks, s.pk)
		tags = append(tags, int32(s.Tags))
		coauthors = append(coauthors, int32(s.Coauthors))
		mentions = append(mentions, int32(s.Mentions))
		comments = append(comments, int32(s.Comments))
		iraq = append(iraq, int32(s.IraqContext))
	}
	_, err := c.DB.Exec(ctx, `
		INSERT INTO discovery_candidates AS c (username, user_pk, tags, coauthors, mentions, comments, iraq_context, priority)
		SELECT u, NULLIF(pk, 0), t, co, m, cm, iq, 3*co + 2*t + 2*m + cm + 2*iq
		FROM unnest($1::text[], $2::bigint[], $3::int[], $4::int[], $5::int[], $6::int[], $7::int[]) AS x(u, pk, t, co, m, cm, iq)
		ON CONFLICT (username) DO UPDATE SET
			user_pk = COALESCE(c.user_pk, EXCLUDED.user_pk),
			tags = c.tags + EXCLUDED.tags, coauthors = c.coauthors + EXCLUDED.coauthors,
			mentions = c.mentions + EXCLUDED.mentions, comments = c.comments + EXCLUDED.comments,
			iraq_context = c.iraq_context + EXCLUDED.iraq_context,
			priority = c.priority + EXCLUDED.priority, last_seen_at = now()`,
		users, pks, tags, coauthors, mentions, comments, iraq)
	if err != nil {
		c.mu.Lock()
		for u, s := range batch {
			if p := c.pending[u]; p != nil {
				p.Tags += s.Tags
				p.Coauthors += s.Coauthors
				p.Mentions += s.Mentions
				p.Comments += s.Comments
				p.IraqContext += s.IraqContext
			} else if len(c.pending) < maxPending {
				c.pending[u] = s
			}
		}
		c.mu.Unlock()
		return err
	}
	return nil
}
