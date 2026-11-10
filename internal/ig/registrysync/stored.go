package registrysync

import (
	"context"
	"log/slog"
	"math/bits"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

// storeSample is how many active accounts a run looks at to choose its
// canaries from. Each run draws a new random sample, so over time every
// account igs has takes its turn.
const storeSample = 200

// What a query kind needs from a canary account.
const (
	hasPosts      = 1 << iota // grid (and profile, which any account serves)
	hasComments               // a post of the last 30 days with comments
	hasReels                  // a stored reel
	hasHighlights             // at least one highlight
	hasAll        = hasPosts | hasComments | hasReels | hasHighlights
)

type storedAccount struct {
	username string
	has      uint
}

// The sample: active public accounts with a username and posts or
// highlights (the owner's test of an active account, 10 Nov 2026), and
// what each has. The comment test is the one storedPosts uses.
const storedSQL = `SELECT p.username,
		coalesce(p.media_count, 0) > 0 OR EXISTS (SELECT 1 FROM posts x WHERE x.owner_pk = p.user_pk AND x.kind <> 'highlight' AND x.status = 'visible'),
		EXISTS (SELECT 1 FROM posts x WHERE x.owner_pk = p.user_pk AND x.comment_count > 0 AND x.taken_at > now() - interval '30 days'),
		EXISTS (SELECT 1 FROM posts x WHERE x.owner_pk = p.user_pk AND x.kind = 'reel' AND x.status = 'visible'),
		coalesce(p.highlight_count, 0) > 0
	FROM (SELECT user_pk, username, media_count, highlight_count FROM profiles
		WHERE status = 'active' AND username IS NOT NULL AND username <> ''
			AND (coalesce(media_count, 0) > 0 OR coalesce(highlight_count, 0) > 0)
		ORDER BY random() LIMIT $1) p`

// storeCanaries chooses up to n canary accounts from the store. A failure
// only leaves the run to canary_pages.
func (s *Sync) storeCanaries(ctx context.Context, n int) []string {
	if n <= 0 {
		return nil
	}
	rows, err := s.DB.Query(ctx, storedSQL, storeSample)
	if err != nil {
		slog.Warn("registry-sync: store canaries", "err", err)
		return nil
	}
	defer rows.Close()
	var list []storedAccount
	for rows.Next() {
		var a storedAccount
		var posts, comments, reels, highlights bool
		if err := rows.Scan(&a.username, &posts, &comments, &reels, &highlights); err != nil {
			slog.Warn("registry-sync: store canaries", "err", err)
			return nil
		}
		for _, f := range []struct {
			ok  bool
			bit uint
		}{{posts, hasPosts}, {comments, hasComments}, {reels, hasReels}, {highlights, hasHighlights}} {
			if f.ok {
				a.has |= f.bit
			}
		}
		list = append(list, a)
	}
	if err := rows.Err(); err != nil {
		slog.Warn("registry-sync: store canaries", "err", err)
		return nil
	}
	return pickCanaries(list, n)
}

// pickCanaries orders up to n accounts for the checks, which read pages in
// order and stop once every query has a verdict: first the account with
// the most of what is still needed, until every need is met, then the
// accounts with the most of everything. Ties keep the sample's (random)
// order. An account with neither posts nor highlights is never chosen.
func pickCanaries(list []storedAccount, n int) []string {
	var out []string
	used := make([]bool, len(list))
	missing := uint(hasAll)
	for len(out) < n {
		best, bestNew, bestAll := -1, 0, 0
		for i, a := range list {
			if used[i] || a.has&(hasPosts|hasHighlights) == 0 {
				continue
			}
			nw, all := bits.OnesCount(a.has&missing), bits.OnesCount(a.has)
			if best < 0 || nw > bestNew || (nw == bestNew && all > bestAll) {
				best, bestNew, bestAll = i, nw, all
			}
		}
		if best < 0 {
			break
		}
		used[best] = true
		missing &^= list[best].has
		out = append(out, list[best].username)
	}
	return out
}

// canaryPages is the run's list of canary pages: the store's choice
// first, then canary_pages, without repeats, PageLoads at most.
func (s *Sync) canaryPages(ctx context.Context) []string {
	limit := s.Cfg.PageLoads()
	seen := map[string]bool{}
	var out []string
	add := func(p string) {
		k := strings.ToLower(p)
		if ref, err := task.NormalizeRef(p); err == nil {
			k = strings.ToLower(ref)
		}
		if len(out) < limit && !seen[k] {
			seen[k] = true
			out = append(out, p)
		}
	}
	for _, p := range s.storeCanaries(ctx, s.Cfg.StoreCanaries) {
		add(p)
	}
	for _, p := range s.Cfg.CanaryPages {
		add(p)
	}
	return out
}
