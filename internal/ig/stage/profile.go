package stage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
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
	if d.Suppression.PageSuppressed(sc.UserPk) {
		return done(map[string]int64{"profiles_suppressed": 1})
	}
	if sc.Username == "" && sc.UserPk != 0 && d.DB != nil {
		_ = d.DB.QueryRow(ctx, `SELECT coalesce(username, '') FROM profiles WHERE user_pk = $1`, sc.UserPk).Scan(&sc.Username)
	}
	if sc.Username == "" {
		return Outcome{Result: Fail, Reason: "profile task without a username (" + sc.ProfileRef + ")"}
	}
	id, err := d.identity(ctx, "profile", t.Attempt)
	if err != nil {
		return retry(t, "", "30s", "no identity: "+err.Error(), c)
	}
	defer d.Pool.Release(id)
	page := strings.TrimRight(d.base(), "/") + "/" + sc.Username + "/"
	if !id.Bootstrapped() {
		pl, err := d.load(ctx, id, page, t.Lane)
		c["page_loads"]++
		if pl != nil {
			c["wire_bytes"] += pl.Bytes
		}
		if err != nil && pl == nil {
			return retry(t, t.Cursor, "30s", "page load: "+err.Error(), c)
		}
		if pl.Class == detect.ContentUnavailable {
			return s.unavailable(ctx, t, sc, c, "page 404")
		}
		if pl.Class != detect.OK || err != nil {
			return classOutcome(t, pl.Class, pl.Reason, t.Cursor, c)
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
	// The newest post an earlier segment saw: the grid runs newest first,
	// so later segments only see older posts.
	if sc.NewestSeenAt != nil && sc.NewestSeenAt.Seconds > st.NewestSeenUnix {
		st.NewestSeenUnix = sc.NewestSeenAt.Seconds
	}
	phase, _ := splitCursor(st.Cursor)

	if phase == "" {
		if t.Seq == 0 && (sc.RefreshProfile || t.Mode == commonv1.CrawlMode_CRAWL_MODE_FULL || sc.UserPk == 0 || st.Extra["user_id"] == "") {
			out, stop := s.profile(ctx, t, id, sc, &st, c, page)
			if stop {
				return out
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
	src := emit.Source{JobID: t.JobId, Query: "profile", DocID: resp.Query.DocID, ParserVersion: parse.ProfileVersion, Mode: t.Mode}
	p := res.Profile
	p.RequestedRef, p.HighlightsHash = sc.ProfileRef, res.HighlightsHash
	if d.Suppression.PageSuppressed(p.UserPk) {
		return done(map[string]int64{"profiles_suppressed": 1}), true
	}
	b := d.Producer.NewBatch()
	if err := d.Emit.Profile(ctx, b, src, p); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if _, err := d.media(ctx, b, t, src, res.Media); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if t.Limits.GetIncludeHighlights() && len(res.Highlights) > 0 && !bytes.Equal(res.HighlightsHash, sc.HighlightsHash) {
		if err := s.highlights(ctx, t, id, b, res, c, page); err != nil {
			slog.Debug("profile: highlights", "profile", p.UserPk, "err", err)
			c["highlights_failed"]++
		}
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c), true
	}
	c["profiles"]++
	sc.UserPk, sc.HighlightsHash = p.UserPk, res.HighlightsHash
	st.Extra["user_id"] = res.UserID
	sc.UserId = res.UserID
	if p.IsPrivate {
		c["profiles_private"]++
		return done(c), true
	}
	return Outcome{}, false
}

// highlights reads each highlight's items and publishes the highlights. A
// highlight whose items cannot be read is still published without them.
func (s *ProfileStage) highlights(ctx context.Context, t *igstaskv1.Task, id *session.Identity, b *kafka.Batch, res parse.ProfileResult,
	c map[string]int64, page string) error {
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
	for _, h := range res.Highlights {
		if full := byID[h.HighlightId]; full != nil {
			full.Cover = h.Cover
			if full.Title == "" {
				full.Title = h.Title
			}
			h = full
		}
		if err := d.Emit.Highlight(ctx, b, src, h); err != nil {
			return err
		}
		c["highlights"]++
	}
	if _, err := d.media(ctx, b, t, src, media); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if resp.Class != detect.OK {
		return errors.New(resp.Class.String())
	}
	return nil
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
	slog.Debug("profile unavailable", "profile", sc.ProfileRef, "why", why)
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
			Permalink: permalink(d.base(), it.Shortcode, it.ProductType),
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
	src := emit.Source{JobID: t.JobId, Query: "reels", DocID: resp.Query.DocID, ParserVersion: parse.ReelsVersion, Mode: t.Mode}
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
		return nil
	}
	st.Cursor = phaseReels + ":" + res.PageInfo.EndCursor
	return nil
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
