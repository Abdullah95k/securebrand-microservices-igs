package stage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/explore"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Page sizes the stages ask for ("first"). The owner's probe runs: the grid
// serves 50 posts a page when asked, the reels tab 50 reels; comments cap
// at about 24 (build-status, speed tests).
const (
	GridPageSize    = 50
	ReelsPageSize   = 50
	CommentPageSize = 50
)

// Phases of a profile stream, kept in its checkpoint (and in a
// continuation's cursor as "<phase>:<cursor>").
const (
	phaseGrid  = "g"
	phaseReels = "r"
	phaseEnd   = "end"
)

// ProfileStage reads a profile: the profile itself (the first segment of
// a full or daily run), its grid back to the age window or, on a
// scheduled run, to the newest stored post (spawning a post task for each
// post to read), then its reels tab for each reel's views.
type ProfileStage struct{ D *Deps }

func splitCursor(s string) (phase, cursor string) {
	if s == "" {
		return "", ""
	}
	p, c, _ := strings.Cut(s, ":")
	return p, c
}

func (s *ProfileStage) Run(ctx context.Context, t *igstaskv1.Task) Outcome {
	d := s.D
	c := map[string]int64{}
	sc := t.GetProfile()
	if sc == nil {
		return Outcome{Result: Fail, Reason: "profile task without profile scope"}
	}
	if sc.Vet || sc.ExploreKind != "" {
		// A vet's requests, and a discovery page's, take at most
		// discovery's share of each exit (ig.discovery.exit_share).
		ctx = session.WithShare(ctx, session.ShareDiscovery)
	}
	if sc.ExploreKind != "" {
		return s.explorePage(ctx, t, sc, c)
	}
	if d.Suppression.PageSuppressed(sc.UserPk) {
		return done(map[string]int64{"profiles_suppressed": 1})
	}
	if sc.Username == "" && sc.UserPk != 0 && d.DB != nil {
		_ = d.DB.QueryRow(ctx, `SELECT coalesce(username, '') FROM profiles WHERE user_pk = $1`, sc.UserPk).Scan(&sc.Username)
	}
	if sc.Username == "" {
		return Outcome{Result: Fail, Reason: "profile task without a username (" + sc.ProfileRef + ")"}
	}
	id, err := d.identity(ctx, "profile", t.Stalls)
	if err != nil {
		return noIdentity(t, "", err, c)
	}
	defer func() { d.Pool.Release(id) }() // id changes on a failover
	hops := 0
	page := strings.TrimRight(d.base(), "/") + "/" + sc.Username + "/"
	// boot bootstraps a fresh identity on the profile's page, failing over
	// to another exit when the page load is blocked.
	boot := func() *Outcome {
		for !id.Bootstrapped() {
			pl, err := d.load(ctx, id, page, t.Lane)
			c["page_loads"]++
			if pl != nil {
				c["wire_bytes"] += pl.Bytes
			}
			if err != nil && pl == nil {
				o := retry(t, t.Cursor, "30s", "page load: "+err.Error(), c)
				return &o
			}
			if pl.Class == detect.ContentUnavailable {
				o := s.unavailable(ctx, t, sc, c, "page 404")
				if sc.Vet {
					o = s.saveVet(ctx, t, sc, discovery.Vet{}, c)
				}
				return &o
			}
			if pl.Class != detect.OK || err != nil {
				o := classOutcome(t, pl.Class, pl.Reason, t.Cursor, c)
				if d.failover(ctx, "profile", &id, o, &hops) {
					continue
				}
				return &o
			}
		}
		return nil
	}
	if o := boot(); o != nil {
		return *o
	}
	if sc.Vet {
		for {
			out := s.vet(ctx, t, id, sc, c, page)
			if !d.failover(ctx, "profile", &id, out, &hops) {
				return out
			}
			if o := boot(); o != nil {
				return *o
			}
		}
	}
	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, "", "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Extra == nil {
		st.Extra = map[string]string{}
	}
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	if st.Extra["user_id"] == "" {
		st.Extra["user_id"] = sc.UserId
	}
	// What an earlier run of this segment learned (it may have crashed
	// after the profile phase, which does not run again).
	if sc.UserPk == 0 {
		sc.UserPk = atoi64(st.Extra["user_pk"])
	}
	// The newest post an earlier segment saw: the grid runs newest first,
	// so later segments only see older posts.
	if sc.NewestSeenAt != nil && sc.NewestSeenAt.Seconds > st.NewestSeenUnix {
		st.NewestSeenUnix = sc.NewestSeenAt.Seconds
	}
	phase, _ := splitCursor(st.Cursor)

	if phase == "" {
		// A scheduled run reads the profile only for the daily refresh: the
		// grid's first page (by username) gives the user id the later pages
		// and the reels tab need.
		if t.Seq == 0 && (sc.RefreshProfile || t.Mode == commonv1.CrawlMode_CRAWL_MODE_FULL || sc.UserPk == 0) {
			for {
				out, stop := s.profile(ctx, t, id, sc, &st, c, page)
				if !stop {
					break
				}
				if !d.failover(ctx, "profile", &id, out, &hops) {
					return out
				}
				if o := boot(); o != nil {
					return *o
				}
			}
		}
		st.Cursor = phaseGrid + ":"
		_ = d.saveCkpt(ctx, t, st, c)
	}

	started := time.Now()
	yield := func() bool {
		return kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter
	}
	for {
		phase, cur := splitCursor(st.Cursor)
		if phase == phaseEnd {
			break
		}
		if yield() {
			return s.yield(t, sc, st, c)
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		var out *Outcome
		switch phase {
		case phaseGrid:
			out = s.gridPage(ctx, t, id, sc, &st, c, cur, page)
		case phaseReels:
			out = s.reelsPage(ctx, t, id, sc, &st, c, cur, page)
		default:
			return Outcome{Result: Fail, Reason: "unknown phase " + phase, Counters: c}
		}
		if out != nil {
			if d.failover(ctx, "profile", &id, *out, &hops) {
				if o := boot(); o != nil {
					return *o
				}
				continue // the same page, on another exit
			}
			return *out
		}
		st.Pages++
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("profile: checkpoint save failed", "err", err)
		}
	}
	return done(c)
}

// profile reads and publishes the profile. stop is true when the task ends
// here (the profile is gone or private, or a retry is needed).
func (s *ProfileStage) profile(ctx context.Context, t *igstaskv1.Task, id *session.Identity, sc *igstaskv1.ProfileScope,
	st *checkpoint.State, c map[string]int64, page string) (Outcome, bool) {
	d := s.D
	resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "profile", Vars: map[string]any{"username": sc.Username},
		PageID: sc.UserPk, Fast: t.Lane == igstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: page})
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		return retry(t, "", "30s", err.Error(), c), true
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	res, rep := parse.Profile(resp.Parts)
	if resp.Class != detect.OK && (!res.NotFound || resp.Class != detect.SoftError) {
		return classOutcome(t, resp.Class, resp.Reason, "", c), true
	}
	d.GQL.Parsed(resp, rep.OK())
	for _, f := range rep.Optional {
		metrics.ParseMissingField.WithLabelValues("profile", f).Inc()
	}
	if !rep.OK() {
		metrics.ParseTotal.WithLabelValues("profile", "1", "drift").Inc()
		return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), "", c), true
	}
	metrics.ParseTotal.WithLabelValues("profile", "1", "ok").Inc()
	if res.NotFound {
		return s.unavailable(ctx, t, sc, c, "no user"), true
	}
	src := emit.Source{JobID: t.JobId, Query: "profile", DocID: resp.Query.DocID, ParserVersion: parse.ProfileVersion, Mode: t.Mode,
		RawRef: resp.RawRef}
	p := res.Profile
	p.RequestedRef, p.HighlightsHash = sc.ProfileRef, res.HighlightsHash
	if d.Suppression.PageSuppressed(p.UserPk) {
		return done(map[string]int64{"profiles_suppressed": 1}), true
	}
	b := d.Producer.NewBatch()
	// ig.limits.include_highlights also switches highlights off for tasks
	// already queued, which carry the value their job was created with
	// (pilot, 11 Nov 2026: Instagram rate-limited every highlights request,
	// and each one rested its exit).
	if t.Limits.GetIncludeHighlights() && d.Cfg.IG.Limits.IncludeHighlights && len(res.Highlights) > 0 &&
		!bytes.Equal(res.HighlightsHash, sc.HighlightsHash) {
		if cls, err := s.highlights(ctx, t, id, b, res, c, page); err != nil {
			slog.Debug("profile: highlights", "profile", p.UserPk, "err", err)
			c["highlights_failed"]++
			// Tries that got nowhere, not attempts (a park, every exit
			// resting, is no failed try), up to one short of the task's
			// own limit: the 30 s and 5 min retries outlast a rate limit's
			// rest, and the task never reaches the DLQ for its highlights.
			if t.Stalls < highlightStalls(d.Cfg.Crawler.MaxAttempts) {
				if cls.Blocking() {
					// A block fails over to another identity within the run
					// (Run), and only then waits for a retry.
					return classOutcome(t, cls, "highlights", "", c), true
				}
				return retry(t, "", "30s", "highlights: "+err.Error(), c), true
			}
			// Published without their items: the stored hash stays empty,
			// so the next run reads them again.
			p.HighlightsHash = nil
		}
	}
	if err := d.Emit.Profile(ctx, b, src, p); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if _, err := d.media(ctx, b, t, src, res.Media); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c), true
	}
	c["profiles"]++
	sc.UserPk, sc.HighlightsHash = p.UserPk, p.HighlightsHash
	st.Extra["user_id"], st.Extra["user_pk"] = res.UserID, itoa(p.UserPk)
	sc.UserId = res.UserID
	if p.IsPrivate {
		c["profiles_private"]++
		return done(c), true
	}
	return Outcome{}, false
}

// highlights reads each highlight's items and publishes the highlights. A
// highlight whose items cannot be read is still published without them;
// the error then says why, with the response class of a refused query.
func (s *ProfileStage) highlights(ctx context.Context, t *igstaskv1.Task, id *session.Identity, b *kafka.Batch, res parse.ProfileResult,
	c map[string]int64, page string) (detect.Class, error) {
	d := s.D
	ids := make([]any, 0, len(res.Highlights))
	for _, h := range res.Highlights {
		ids = append(ids, "highlight:"+itoa(h.HighlightId))
	}
	byID := map[int64]*igv1.Highlight{}
	resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "highlights", Vars: map[string]any{"reel_ids": ids, "initial_reel_id": ids[0]},
		PageID: res.Profile.UserPk, Lane: topics.LaneName(t.Lane), Referer: page})
	c["requests"]++
	var items []*igv1.Highlight
	var media []*igv1.Media
	if err == nil && resp.Class == detect.OK {
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		items, media, _ = parse.HighlightItems(resp.Parts, res.Profile.UserPk)
	}
	for _, h := range items {
		byID[h.HighlightId] = h
	}
	src := emit.Source{JobID: t.JobId, Query: "highlights", ParserVersion: parse.HighlightsVersion, Mode: t.Mode}
	if resp != nil {
		src.RawRef = resp.RawRef
	}
	for _, h := range res.Highlights {
		if full := byID[h.HighlightId]; full != nil {
			full.Cover = h.Cover
			if full.Title == "" {
				full.Title = h.Title
			}
			h = full
		}
		if err := d.Emit.Highlight(ctx, b, src, h); err != nil {
			return detect.OK, err
		}
		c["highlights"]++
	}
	if _, err := d.media(ctx, b, t, src, media); err != nil {
		return detect.OK, err
	}
	if err != nil {
		return detect.Transport, err
	}
	if resp.Class != detect.OK {
		return resp.Class, errors.New(resp.Class.String())
	}
	return detect.OK, nil
}

// unavailable publishes a profile that is gone (renamed, deleted, refused).
func (s *ProfileStage) unavailable(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope, c map[string]int64, why string) Outcome {
	c["profiles_unavailable"]++
	if sc.UserPk == 0 {
		// Never seen: nothing to mark.
		return done(c)
	}
	b := s.D.Producer.NewBatch()
	p := &igv1.Profile{UserPk: sc.UserPk, Username: sc.Username, Status: igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE, RequestedRef: sc.ProfileRef}
	if err := s.D.Emit.Profile(ctx, b, emit.Source{JobID: t.JobId, Query: "profile", Mode: t.Mode}, p); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	slog.Debug("profile unavailable", plog.Account(sc.UserPk, sc.ProfileRef), "why", why)
	return done(c)
}

// vet reads a discovery candidate (M11): its profile and, when public, the
// captions of its grid's first page. What it read goes to
// ig.discovery_candidates for the vetter to score; nothing is published,
// so a candidate that is not registered never reaches the analysis.
func (s *ProfileStage) vet(ctx context.Context, t *igstaskv1.Task, id *session.Identity, sc *igstaskv1.ProfileScope,
	c map[string]int64, page string) Outcome {
	d := s.D
	lane := topics.LaneName(t.Lane)
	resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "profile", Vars: map[string]any{"username": sc.Username}, Lane: lane, Referer: page})
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		return retry(t, "", "30s", err.Error(), c)
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	res, rep := parse.Profile(resp.Parts)
	if resp.Class != detect.OK && (!res.NotFound || resp.Class != detect.SoftError) {
		return classOutcome(t, resp.Class, resp.Reason, "", c)
	}
	if res.NotFound {
		return s.saveVet(ctx, t, sc, discovery.Vet{}, c)
	}
	if !rep.OK() || res.Profile == nil {
		return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), "", c)
	}
	p := res.Profile
	v := discovery.Vet{Found: true, Private: p.IsPrivate, UserPK: p.UserPk, Username: p.Username, FullName: p.FullName,
		Biography: p.Biography, Category: p.Category, Followers: p.GetFollowerCount(), Verified: p.IsVerified, Business: p.IsBusiness}
	for _, l := range p.BioLinks {
		v.Links = append(v.Links, l.GetUrl())
	}
	if !p.IsPrivate {
		gresp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "grid_first", Vars: map[string]any{"username": sc.Username, "first": GridPageSize},
			Lane: lane, Referer: page})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, "", "30s", err.Error(), c)
		}
		c["wire_bytes"] += gresp.BytesIn + gresp.BytesOut
		if gresp.Class != detect.OK {
			return classOutcome(t, gresp.Class, gresp.Reason, "", c)
		}
		g, _ := parse.Grid(gresp.Parts)
		for i, it := range g.Items {
			if i == 12 {
				break
			}
			if it.Caption != "" {
				v.Captions = append(v.Captions, truncate(it.Caption, 500))
			}
		}
		if s.wantsAudience(ctx, sc, v) {
			s.readAudience(ctx, id, &v, g.Items, c, lane, page)
		}
	}
	return s.saveVet(ctx, t, sc, v, c)
}

// wantsAudience reports whether a public candidate's vet also reads its
// commenters (D-055, ig.discovery.audience): only one with the follower
// count that its own words and sightings leave below the threshold.
func (s *ProfileStage) wantsAudience(ctx context.Context, sc *igstaskv1.ProfileScope, v discovery.Vet) bool {
	d := s.D.Cfg.IG.Discovery
	if d.Audience.Share <= 0 || v.Followers < d.Audience.MinFollowers || s.D.Cfg.IG.Privacy.PIIMode != "full" || s.D.DB == nil {
		return false
	}
	var sg discovery.Sightings
	err := s.D.DB.QueryRow(ctx, `SELECT tags, coauthors, mentions, comments, iraq_context, sources FROM discovery_candidates
		WHERE username = lower($1)`, sc.Username).Scan(&sg.Tags, &sg.Coauthors, &sg.Mentions, &sg.Comments, &sg.IraqContext, &sg.Sources)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Warn("vet: sightings", "err", err)
		return false
	}
	return discovery.Score(v, sg).Sum() < d.Threshold
}

// readAudience reads the newest top-level comments on the candidate's
// newest posts (pinned ones skipped) until audience.posts posts with
// comments or audience.max_comments comments were read, posts + 1 comment
// reads at most, each from the post's page as regular collection reads
// them. It never fails the vet: a refused or unreadable read (a wall, a
// rate limit, a transport error) ends the sample with what it has, and the
// vetter decides on that (fewer than min_commenters: no decision). The
// exit's rests still follow the answer, as for any request.
func (s *ProfileStage) readAudience(ctx context.Context, id *session.Identity, v *discovery.Vet,
	items []parse.GridItem, c map[string]int64, lane, page string) {
	d := s.D
	a := d.Cfg.IG.Discovery.Audience
	au := &discovery.Audience{}
	at := map[string]int{}
	reads := 0
	for _, it := range items {
		if au.Posts >= a.Posts || au.Comments >= a.MaxComments || reads > a.Posts {
			break
		}
		if it.Pinned || it.MediaPK == 0 {
			continue
		}
		vars := map[string]any{"media_pk": itoa(it.MediaPK), "first": min(CommentPageSize, a.MaxComments-au.Comments)}
		referer := page
		if it.Shortcode != "" {
			referer = permalink(d.base(), it.Shortcode, it.ProductType)
		}
		resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: vars, PageID: v.UserPK, Lane: lane, Referer: referer})
		reads++
		c["requests"]++
		c["audience_reads"]++
		if err != nil {
			c["transport_errors"]++
			metrics.DiscoveryAudienceReads.WithLabelValues("transport").Inc()
			break
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class != detect.OK {
			metrics.DiscoveryAudienceReads.WithLabelValues(resp.Class.String()).Inc()
			break
		}
		res, rep := parse.Comments(resp.Parts, parse.CommentContext{MediaPK: it.MediaPK, OwnerPK: v.UserPK})
		if !rep.OK() {
			metrics.DiscoveryAudienceReads.WithLabelValues("drift").Inc()
			break
		}
		metrics.DiscoveryAudienceReads.WithLabelValues("ok").Inc()
		n := 0
		for _, cm := range res.Comments {
			if au.Comments >= a.MaxComments {
				break
			}
			if cm.Author == nil || cm.IsOp || cm.Depth > 0 || cm.ParentCommentPk != nil {
				continue
			}
			key := strings.ToLower(cm.Author.Username)
			if cm.Author.UserPk != 0 {
				key = itoa(cm.Author.UserPk)
			}
			if key == "" {
				continue
			}
			n++
			au.Comments++
			iraqi := discovery.IraqiText(cm.Text)
			if i, ok := at[key]; ok {
				au.Commenters[i].IraqText = au.Commenters[i].IraqText || iraqi
				continue
			}
			at[key] = len(au.Commenters)
			au.Commenters = append(au.Commenters, discovery.Commenter{PK: cm.Author.UserPk, Username: strings.ToLower(cm.Author.Username), IraqText: iraqi})
		}
		if n > 0 {
			au.Posts++
		}
	}
	v.Audience = au
}

// explorePage loads a discovery page (D-055): a location's or a
// hashtag's, logged out. The accounts that posted what it shows become
// candidates seen in Iraq (iraq_context), with origin place or hashtag;
// nothing else of the page is kept or published. A login page, a wall, a
// rate limit or a checkpoint is recorded and ends the task: the page waits
// for its next turn, on whatever exit, and nothing pushes past the check.
func (s *ProfileStage) explorePage(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope, c map[string]int64) Outcome {
	d := s.D
	path := explore.PagePath(sc.ExploreKind, sc.ExploreKey)
	if path == "" {
		return Outcome{Result: Fail, Reason: "discovery page: unknown kind " + sc.ExploreKind, Counters: c}
	}
	id, err := d.identity(ctx, "profile", t.Stalls)
	if err != nil {
		return noIdentity(t, "", err, c)
	}
	defer func() { d.Pool.Release(id) }()
	pl, err := d.load(ctx, id, strings.TrimRight(d.base(), "/")+path, t.Lane)
	c["page_loads"]++
	c["discovery_pages"]++
	if pl != nil {
		c["wire_bytes"] += pl.Bytes
	}
	if pl == nil {
		// Not sent (a resting exit) or no answer: another turn, later.
		return retry(t, "", "60s", "discovery page: "+fmt.Sprint(err), c)
	}
	result := "other"
	var owners []explore.Owner
	switch {
	case strings.Contains(pl.URL, "/accounts/login"):
		result = "login"
	case pl.Class == detect.LoginWall:
		result = "wall"
	case pl.Class == detect.RateLimited || pl.Class == detect.Checkpoint:
		result = "limited"
	case pl.Class == detect.ContentUnavailable:
		result = "unavailable"
	case pl.Class == detect.OK:
		// A page with no tokens still shows its posts.
		result = "loaded"
		owners = explore.Owners(pl.HTML)
	}
	added, err := s.pageSightings(ctx, sc.ExploreKind, owners)
	if err != nil {
		return retry(t, "", "30s", "discovery page: "+err.Error(), c)
	}
	metrics.DiscoveryPages.WithLabelValues(sc.ExploreKind, result).Inc()
	c["candidates_seen"] += int64(len(owners))
	c["candidates_new"] += int64(added)
	if d.DB != nil {
		if _, err := d.DB.Exec(ctx, `UPDATE discovery_pages SET loaded_at = now(), loads = loads + 1, last_class = $3,
				last_accounts = $4, last_new = $5, empty_runs = CASE WHEN $5 = 0 THEN empty_runs + 1 ELSE 0 END,
				due_at = now() + $6::int * interval '1 second' * power(2, least(CASE WHEN $5 = 0 THEN empty_runs + 1 ELSE 0 END, 4))
			WHERE kind = $1 AND key = $2`, sc.ExploreKind, sc.ExploreKey, result, len(owners), added,
			int(max(d.Cfg.IG.Discovery.Pages.Revisit, time.Hour).Seconds())); err != nil {
			return retry(t, "", "30s", "discovery page: "+err.Error(), c)
		}
	}
	return done(c)
}

// pageSightings records a discovery page's accounts as candidates seen in
// Iraq, with origin place (a location's page) or hashtag, and returns how
// many were new to discovery. An erased account is never a candidate.
func (s *ProfileStage) pageSightings(ctx context.Context, kind string, owners []explore.Owner) (int, error) {
	if s.D.DB == nil || len(owners) == 0 {
		return 0, nil
	}
	origin := "hashtag"
	if kind == explore.KindLocation {
		origin = "place"
	}
	var users []string
	var pks []int64
	for _, o := range owners {
		u := strings.ToLower(o.Username)
		if !discovery.ValidUsername(u) || (o.PK != 0 && s.D.Suppression.PageSuppressed(o.PK)) {
			continue
		}
		users, pks = append(users, u), append(pks, o.PK)
	}
	if len(users) == 0 {
		return 0, nil
	}
	var added int
	err := s.D.DB.QueryRow(ctx, `WITH up AS (
			INSERT INTO discovery_candidates AS c (username, user_pk, iraq_context, priority, origins)
			SELECT x.u, NULLIF(x.pk, 0), 1, 2, ARRAY[$3::text] FROM unnest($1::text[], $2::bigint[]) AS x(u, pk)
			ON CONFLICT (username) DO UPDATE SET user_pk = COALESCE(c.user_pk, EXCLUDED.user_pk),
				iraq_context = c.iraq_context + 1, priority = c.priority + 2, last_seen_at = now(),
				origins = CASE WHEN $3 = ANY(c.origins) THEN c.origins ELSE c.origins || ARRAY[$3::text] END
			RETURNING (xmax = 0) AS inserted)
		SELECT count(*) FILTER (WHERE inserted) FROM up`, users, pks, origin).Scan(&added)
	if err != nil {
		return 0, err
	}
	metrics.DiscoverySightings.WithLabelValues(origin).Add(float64(len(users)))
	return added, nil
}

// saveVet records what a vet read for the vetter.
func (s *ProfileStage) saveVet(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope, v discovery.Vet, c map[string]int64) Outcome {
	if s.D.DB != nil {
		b, err := json.Marshal(v)
		if err != nil {
			return Outcome{Result: Fail, Reason: "vet: " + err.Error(), Counters: c}
		}
		if _, err := s.D.DB.Exec(ctx, `UPDATE discovery_candidates SET vet = $2, vetted_at = now() WHERE username = lower($1)`,
			sc.Username, b); err != nil {
			return retry(t, "", "30s", "vet: "+err.Error(), c)
		}
	}
	c["candidates_vetted"]++
	return done(c)
}

// gridPage reads one grid page and spawns a post task per post to read.
// It returns nil to go on, or the task's outcome.
func (s *ProfileStage) gridPage(ctx context.Context, t *igstaskv1.Task, id *session.Identity, sc *igstaskv1.ProfileScope,
	st *checkpoint.State, c map[string]int64, cur, page string) *Outcome {
	d := s.D
	req := graphql.Request{Query: "grid_first", Vars: map[string]any{"username": sc.Username, "first": GridPageSize},
		PageID: sc.UserPk, Fast: t.Lane == igstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: page}
	if cur != "" {
		req.Query, req.Vars = "grid", map[string]any{"user_id": st.Extra["user_id"], "cursor": cur, "first": GridPageSize}
	}
	resp, err := d.GQL.Do(ctx, id, req)
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		o := retry(t, st.Cursor, "30s", err.Error(), c)
		return &o
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	if resp.Class != detect.OK {
		if resp.Class == detect.LoginWall && wallTruncates(st, st.Cursor) {
			// The same wall at the same depth on fresh identities: the grid
			// ends here for logged-out readers.
			c["timelines_truncated"]++
			return s.endGrid(ctx, t, sc, st, c, "wall")
		}
		_ = d.saveCkpt(ctx, t, *st, c)
		o := classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		return &o
	}
	res, rep := parse.Grid(resp.Parts)
	d.GQL.Parsed(resp, rep.OK())
	if !rep.OK() {
		metrics.ParseTotal.WithLabelValues("grid", "1", "drift").Inc()
		o := classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		return &o
	}
	metrics.ParseTotal.WithLabelValues("grid", "1", "ok").Inc()
	st.Walls, st.WallCursor = 0, ""
	if st.Extra["user_id"] == "" {
		st.Extra["user_id"] = res.UserID
	}
	watermark := s.watermarkFor(ctx, t, sc)
	now := d.now()
	var maxAge time.Time
	if a := t.Limits.GetMaxAgeSeconds(); a > 0 {
		maxAge = now.Add(-time.Duration(a) * time.Second)
	}
	var children []*igstaskv1.Task
	stop := ""
	for _, it := range res.Items {
		taken := time.Time{}
		if it.TakenAt != nil {
			taken = it.TakenAt.AsTime()
		}
		if it.Pinned {
			// Pinned posts come first though older: they never end the
			// walk, and a known one is not read again on a scheduled run.
			if !watermark.IsZero() && !taken.After(watermark) {
				continue
			}
		} else {
			if !watermark.IsZero() && !taken.After(watermark) {
				stop = "watermark"
				break
			}
			if !maxAge.IsZero() && !taken.IsZero() && taken.Before(maxAge) {
				stop = "age"
				break
			}
			if u := taken.Unix(); u > st.NewestSeenUnix && !taken.IsZero() {
				st.NewestSeenUnix = u
			}
		}
		if mp := t.Limits.GetMaxPosts(); mp > 0 && sc.PostsSeen+st.PostsSeen >= mp {
			stop = "cap"
			break
		}
		owner := it.OwnerPK
		if owner == 0 {
			owner = sc.UserPk
		}
		children = append(children, task.NewPost(t, commonv1.CrawlMode_CRAWL_MODE_FULL, &igstaskv1.PostScope{
			MediaPk: it.MediaPK, Shortcode: it.Shortcode, OwnerPk: owner, TakenAt: it.TakenAt, Pinned: it.Pinned,
			Permalink: permalink(d.base(), it.Shortcode, it.ProductType), MediaType: it.MediaType, ProductType: it.ProductType,
		}))
		st.PostsSeen++
		if !taken.IsZero() {
			if o := st.Extra["oldest"]; o == "" || taken.Unix() < atoi64(o) {
				st.Extra["oldest"] = itoa(taken.Unix())
			}
		}
	}
	if err := d.spawn(ctx, children...); err != nil {
		o := retry(t, st.Cursor, "30s", err.Error(), c)
		return &o
	}
	c["posts"] += int64(len(children))
	c["pages_fetched"]++
	switch {
	case stop != "":
		if stop == "cap" {
			c["timelines_capped"]++
		}
		return s.endGrid(ctx, t, sc, st, c, stop)
	case !res.PageInfo.HasNext || res.PageInfo.EndCursor == "":
		return s.endGrid(ctx, t, sc, st, c, "end")
	}
	st.Cursor = phaseGrid + ":" + res.PageInfo.EndCursor
	return nil
}

// endGrid records the grid pass and moves on to the reels tab (or the end).
func (s *ProfileStage) endGrid(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope, st *checkpoint.State,
	c map[string]int64, why string) *Outcome {
	newest := time.Time{}
	if st.NewestSeenUnix > 0 {
		newest = time.Unix(st.NewestSeenUnix, 0).UTC()
	}
	oldest := time.Time{}
	if o := atoi64(st.Extra["oldest"]); o > 0 {
		oldest = time.Unix(o, 0).UTC()
	}
	state := "complete"
	if why == "wall" || why == "cap" {
		state = "truncated"
	}
	if err := s.D.saveWatermark(ctx, sc.UserPk, newest, oldest, state, why, t.Mode == commonv1.CrawlMode_CRAWL_MODE_FULL); err != nil {
		slog.Warn("profile: watermark", "profile", sc.UserPk, "err", err)
	}
	if t.Limits.GetIncludeReels() && st.Extra["user_id"] != "" {
		st.Cursor = phaseReels + ":"
		if st.Extra == nil {
			st.Extra = map[string]string{}
		}
		st.Extra["reels"] = s.reelsWindow(ctx, t, sc)
	} else {
		st.Cursor = phaseEnd
	}
	return nil
}

// reelsPage reads one page of the reels tab and publishes each reel's
// counts (its views exist only here) as a metrics sample.
func (s *ProfileStage) reelsPage(ctx context.Context, t *igstaskv1.Task, id *session.Identity, sc *igstaskv1.ProfileScope,
	st *checkpoint.State, c map[string]int64, cur, page string) *Outcome {
	d := s.D
	// A segment that continues the reels tab has a checkpoint of its own:
	// it decides the window again (the same way, as the pass hasn't ended).
	if st.Extra["reels"] == "" {
		st.Extra["reels"] = s.reelsWindow(ctx, t, sc)
	}
	vars := map[string]any{"user_id": st.Extra["user_id"], "first": ReelsPageSize}
	if cur != "" {
		vars["cursor"] = cur
	}
	resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "reels", Vars: vars, PageID: sc.UserPk, Lane: topics.LaneName(t.Lane), Referer: page + "reels/"})
	c["requests"]++
	if err != nil {
		c["transport_errors"]++
		o := retry(t, st.Cursor, "30s", err.Error(), c)
		return &o
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	if resp.Class != detect.OK {
		_ = d.saveCkpt(ctx, t, *st, c)
		o := classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
		return &o
	}
	res, rep := parse.Reels(resp.Parts)
	d.GQL.Parsed(resp, rep.OK())
	if !rep.OK() {
		o := classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		return &o
	}
	var maxAge time.Time
	if a := t.Limits.GetMaxAgeSeconds(); a > 0 {
		maxAge = d.now().Add(-time.Duration(a) * time.Second)
	}
	// A scheduled run reads only the reels window (ig.schedule.reels_window).
	full := st.Extra["reels"] == reelsFull
	if u := atoi64(strings.TrimPrefix(st.Extra["reels"], "since:")); !full && u > 0 {
		if since := time.Unix(u, 0).UTC(); since.After(maxAge) {
			maxAge = since
		}
	}
	src := emit.Source{JobID: t.JobId, Query: "reels", DocID: resp.Query.DocID, ParserVersion: parse.ReelsVersion, Mode: t.Mode,
		RawRef: resp.RawRef}
	b := d.Producer.NewBatch()
	old := false
	for _, r := range res.Reels {
		if !maxAge.IsZero() && r.TakenAt != nil && r.TakenAt.AsTime().Before(maxAge) {
			if !r.Pinned {
				old = true
				break
			}
			continue
		}
		m := &igv1.PostMetrics{MediaPk: r.MediaPK, OwnerPk: sc.UserPk, TakenAt: r.TakenAt, PlayCount: r.PlayCount,
			LikeCount: r.LikeCount, CommentCount: r.CommentCount, CountsHidden: r.CountsHidden}
		if err := d.Emit.Metrics(ctx, b, src, m); err != nil {
			o := retry(t, st.Cursor, "30s", err.Error(), c)
			return &o
		}
		c["reels"]++
	}
	if err := b.Wait(ctx); err != nil {
		o := retry(t, st.Cursor, "30s", "produce: "+err.Error(), c)
		return &o
	}
	if old || !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
		st.Cursor = phaseEnd
		if full {
			c["reels_full_passes"]++
		}
		if err := d.saveReelsPass(ctx, sc.UserPk, full); err != nil {
			slog.Warn("profile: reels cursor", plog.Account(sc.UserPk, sc.ProfileRef), "err", err)
		}
		return nil
	}
	st.Cursor = phaseReels + ":" + res.PageInfo.EndCursor
	return nil
}

// reelsFull marks a reels pass that reads back to limits.max_age_days.
const reelsFull = "full"

// reelsWindow decides how far back this run reads the reels tab: reelsFull,
// or "since:<unix seconds>". A scheduled (incremental) run reads
// ig.schedule.reels_window back, unless the profile had no full pass within
// ig.schedule.reels_full_every; backfills and full runs always read it all.
func (s *ProfileStage) reelsWindow(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope) string {
	g := s.D.Cfg.IG.Schedule
	if t.Mode != commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL || g.ReelsWindow <= 0 {
		return reelsFull
	}
	now := s.D.now()
	last, err := s.D.lastReelsFull(ctx, sc.UserPk)
	if err != nil {
		slog.Warn("profile: reels cursor", plog.Account(sc.UserPk, sc.ProfileRef), "err", err)
	}
	if err != nil || last.IsZero() || now.Sub(last) >= g.ReelsFullEvery {
		return reelsFull
	}
	return "since:" + strconv.FormatInt(now.Add(-g.ReelsWindow).Unix(), 10)
}

// watermarkFor is the newest stored post time on a scheduled (incremental)
// run: the scope's, else the profile's grid cursor in the store.
func (s *ProfileStage) watermarkFor(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.ProfileScope) time.Time {
	if t.Mode != commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		return time.Time{}
	}
	if sc.Watermark != nil {
		return sc.Watermark.AsTime()
	}
	if w, ok, err := s.D.watermark(ctx, sc.UserPk); err == nil && ok {
		sc.Watermark = timestamppb.New(w)
		return w
	}
	return time.Time{}
}

// yield continues the stream in a new segment.
func (s *ProfileStage) yield(t *igstaskv1.Task, sc *igstaskv1.ProfileScope, st checkpoint.State, c map[string]int64) Outcome {
	next := task.Continuation(t, st.Cursor)
	ns := next.GetProfile()
	ns.UserPk, ns.UserId, ns.Username = sc.UserPk, st.Extra["user_id"], sc.Username
	ns.PostsSeen = sc.PostsSeen + st.PostsSeen
	ns.Watermark, ns.HighlightsHash = sc.Watermark, sc.HighlightsHash
	if st.NewestSeenUnix > 0 && (ns.NewestSeenAt == nil || st.NewestSeenUnix > ns.NewestSeenAt.Seconds) {
		ns.NewestSeenAt = timestamppb.New(time.Unix(st.NewestSeenUnix, 0))
	}
	metrics.TaskYields.WithLabelValues("profile").Inc()
	return Outcome{Result: Continue, Next: next, Counters: c}
}

// wallTruncates counts a login wall at cursor; three walls at the same
// cursor (each on a fresh identity, since a wall retires the identity) end
// the stream there.
func wallTruncates(st *checkpoint.State, cursor string) bool {
	if st.WallCursor != cursor {
		st.Walls, st.WallCursor = 0, cursor
	}
	st.Walls++
	return st.Walls >= 3
}

func permalink(base, code, productType string) string {
	kind := "p"
	if productType == "clips" {
		kind = "reel"
	}
	return strings.TrimRight(base, "/") + "/" + kind + "/" + code + "/"
}

func atoi64(s string) int64 { return walk.Int64(gjsonString(s)) }

// highlightStalls is how many tries without progress a profile task makes
// for its highlights before it publishes them without their items (the
// next profile read tries again): one short of crawler.max_attempts, at
// least two.
func highlightStalls(maxAttempts int) uint32 {
	return uint32(max(2, maxAttempts-1))
}
