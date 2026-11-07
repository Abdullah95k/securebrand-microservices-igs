package stage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// incrementalOlderStop: a scheduled (incremental) read of a post's
// comments, newest first, stops after this many comments in a row that are
// older than the newest one stored.
const incrementalOlderStop = 5

// PostStage reads a post: its page (the post's detail and media, and the
// first comments it embeds), then its top-level comments page by page.
// Logged out, Instagram serves no replies; a comment's reply count is kept.
type PostStage struct{ D *Deps }

func (s *PostStage) Run(ctx context.Context, t *igstaskv1.Task) Outcome {
	d := s.D
	c := map[string]int64{}
	sc := t.GetPost()
	if sc == nil || sc.MediaPk == 0 || sc.Shortcode == "" {
		return Outcome{Result: Fail, Reason: "post task without media pk and shortcode"}
	}
	if d.Suppression.PageSuppressed(sc.OwnerPk) {
		return done(map[string]int64{"posts_suppressed": 1})
	}
	if sc.Permalink == "" {
		sc.Permalink = permalink(d.base(), sc.Shortcode, "")
	}
	id, err := d.identity(ctx, "post", t.Stalls)
	if err != nil {
		return noIdentity(t, t.Cursor, err, c)
	}
	defer func() { d.Pool.Release(id) }() // id changes on a failover
	hops := 0

	st, _, err := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	if err != nil {
		return retry(t, t.Cursor, "30s", "checkpoint: "+err.Error(), c)
	}
	st.Resume(c)
	if st.Cursor == "" {
		st.Cursor = t.Cursor
	}
	first := t.Seq == 0 && st.Pages == 0 && st.Cursor == "" && st.Extra["page"] == ""
	createdUnix := int64(0)
	if sc.TakenAt != nil {
		createdUnix = sc.TakenAt.Seconds
	}

	var embedded parse.CommentsResult
	var pageRef string // the post page's raw reference: its embedded comments came with it
	var watermark time.Time
	// The post page bootstraps a fresh identity and, on the first segment,
	// is the post's detail. A blocked load, or a page gated for this
	// identity, fails over to another exit.
	for first || !id.Bootstrapped() {
		pl, err := d.load(ctx, id, sc.Permalink, t.Lane)
		c["page_loads"]++
		if pl != nil {
			c["wire_bytes"] += pl.Bytes
		}
		switch {
		case pl == nil:
			return retry(t, st.Cursor, "30s", "post page: "+err.Error(), c)
		case pl.Class == detect.ContentUnavailable:
			return s.unavailable(ctx, t, sc, c)
		case pl.Class != detect.OK || err != nil:
			o := classOutcome(t, pl.Class, pl.Reason, st.Cursor, c)
			if d.failover(ctx, "post", &id, o, &hops) {
				continue
			}
			return o
		}
		if first {
			res, rep := parse.PostPage(pl.HTML, sc.Shortcode, d.base())
			for _, f := range rep.Optional {
				metrics.ParseMissingField.WithLabelValues("post", f).Inc()
			}
			if res.Gated {
				if st.Extra == nil {
					st.Extra = map[string]string{}
				}
				if wallTruncates(&st, "page") {
					// Three fresh identities saw the same wall: the post is
					// age-gated or otherwise closed to logged-out readers.
					c["posts_gated"]++
					_, _ = d.Jobs.SetPost(ctx, t.JobId, sc.MediaPk, createdUnix, sc.ReportedCommentCount, jobs.PostGated)
					return s.walled(ctx, t, sc, c)
				}
				_ = d.saveCkpt(ctx, t, st, c)
				o := classOutcome(t, detect.LoginWall, "post page gated", st.Cursor, c)
				if d.failover(ctx, "post", &id, o, &hops) {
					continue
				}
				return o
			}
			if !rep.OK() {
				metrics.ParseTotal.WithLabelValues("post", "1", "drift").Inc()
				return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
			}
			metrics.ParseTotal.WithLabelValues("post", "1", "ok").Inc()
			pageRef = d.keepRaw(ctx, "post_page", pl.HTML, map[string]any{"shortcode": sc.Shortcode})
			out, stop := s.publish(ctx, t, sc, res, pageRef, c)
			if stop {
				return out
			}
			embedded = res.Comments
			if res.Post.CommentCount != nil {
				sc.ReportedCommentCount = *res.Post.CommentCount
			}
			sc.DetailKnown = true
		}
		break
	}
	// boot bootstraps the identity a failover brought, on the post page.
	boot := func() *Outcome {
		for !id.Bootstrapped() {
			pl, err := d.load(ctx, id, sc.Permalink, t.Lane)
			c["page_loads"]++
			if pl != nil {
				c["wire_bytes"] += pl.Bytes
			}
			switch {
			case pl == nil:
				o := retry(t, st.Cursor, "30s", "post page: "+err.Error(), c)
				return &o
			case pl.Class != detect.OK || err != nil:
				o := classOutcome(t, pl.Class, pl.Reason, st.Cursor, c)
				if d.failover(ctx, "post", &id, o, &hops) {
					continue
				}
				return &o
			}
		}
		return nil
	}

	if !t.Limits.GetIncludeComments() || (first && sc.ReportedCommentCount == 0 && len(embedded.Comments) == 0) {
		_, _ = d.Jobs.SetPost(ctx, t.JobId, sc.MediaPk, createdUnix, sc.ReportedCommentCount, jobs.PostSkipped)
		return done(c)
	}
	if t.Mode == commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL {
		stored, newest, ok := d.storedPost(ctx, sc.MediaPk)
		if sc.CommentsWatermark != nil {
			newest = sc.CommentsWatermark.AsTime()
		}
		switch {
		case first && ok && stored == sc.ReportedCommentCount:
			// No new comments since the last read.
			c["posts_unchanged"]++
			_, _ = d.Jobs.SetPost(ctx, t.JobId, sc.MediaPk, createdUnix, sc.ReportedCommentCount, jobs.PostSkipped)
			return done(c)
		case first && ok && sc.ReportedCommentCount < stored && d.Cfg.Scheduler.PostRefresh.DetectDeletions:
			// Fewer comments than at the last read: some were deleted. Read
			// them all, so the job's end can tell which (REQ-008 fix 14).
			c["posts_comments_dropped"]++
		case !newest.IsZero():
			watermark = newest.Add(-time.Minute)
		}
	}
	// A known post's comments read from the first page to the last are
	// compared with the stored ones when the job ends: those the read no
	// longer returned were deleted (jobs.Tracker passes; the finalizer).
	pass := false
	if d.Cfg.Scheduler.PostRefresh.DetectDeletions {
		if first && watermark.IsZero() {
			if _, _, known := d.storedPost(ctx, sc.MediaPk); known {
				pass = d.Jobs.BeginPass(ctx, t.JobId, t.LineageId, sc.MediaPk, time.Now()) == nil
			}
		} else if !first {
			pass, _ = d.Jobs.InPass(ctx, t.JobId, t.LineageId)
		}
	}
	seen := func(res parse.CommentsResult) {
		if !pass || len(res.Comments) == 0 {
			return
		}
		cs := make([]jobs.SeenComment, 0, len(res.Comments))
		for _, cm := range res.Comments {
			cs = append(cs, jobs.SeenComment{ID: cm.CommentPk, Replies: cm.GetChildCommentCount()})
		}
		if err := d.Jobs.AddSeen(ctx, t.JobId, sc.MediaPk, cs); err != nil {
			// A lost page makes the read look partial: no deletion is inferred.
			slog.Warn("post: seen comments not recorded", "err", err)
		}
	}
	if collected, _ := d.Jobs.SetPost(ctx, t.JobId, sc.MediaPk, createdUnix, sc.ReportedCommentCount, jobs.PostPending); collected && first {
		c["posts_already_collected"]++
		return done(c)
	}

	src := emit.Source{JobID: t.JobId, Query: "comments", ParserVersion: parse.CommentsVersion, Mode: t.Mode}
	if q, err := d.GQL.Registry.Get("comments"); err == nil {
		src.DocID = q.DocID
	}
	cc := parse.CommentContext{MediaPK: sc.MediaPk, OwnerPK: sc.OwnerPk, PostTakenAt: sc.TakenAt}
	maxC := t.Limits.GetMaxCommentsPerPost()
	olderRun := 0
	// complete: the read reached the last page (not a cap or the
	// incremental stop).
	finished, truncated, complete, capped := false, false, false, false
	next := func(res parse.CommentsResult, n, older int) {
		if !watermark.IsZero() {
			if n > 0 && older == n {
				olderRun += older
			} else {
				olderRun = older
			}
		}
		switch {
		case !res.PageInfo.HasNext || res.PageInfo.EndCursor == "":
			finished, complete = true, true
		case maxC > 0 && t.ItemsSeen+st.ItemsSeen >= maxC:
			finished, capped = true, true
			c["posts_capped"]++
		case !watermark.IsZero() && olderRun >= incrementalOlderStop:
			finished = true
		default:
			st.Cursor = res.PageInfo.EndCursor
		}
	}
	if first {
		// The page's embedded comments are the connection's first page.
		b := d.Producer.NewBatch()
		esrc := src
		esrc.Query, esrc.DocID, esrc.RawRef = "post_page", "", pageRef
		n, older, err := s.emitComments(ctx, b, t, esrc, embedded, watermark, maxC, &st, c)
		if err == nil {
			err = b.Wait(ctx)
		}
		if err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		seen(embedded)
		if st.Extra == nil {
			st.Extra = map[string]string{}
		}
		st.Extra["page"] = "1"
		if embedded.PageInfo.Found {
			next(embedded, n, older)
		} else if len(embedded.Comments) == 0 && sc.ReportedCommentCount > 0 {
			// The page embedded no comments: read the first page by query.
		} else {
			finished, complete = true, true
		}
		_ = d.saveCkpt(ctx, t, st, c)
	}

	started := time.Now()
	for !finished {
		if kafka.IsDraining(ctx) || st.Pages >= d.Cfg.Crawler.YieldPages || time.Since(started) > d.Cfg.Crawler.YieldAfter {
			break
		}
		if d.Jobs.Cancelled(ctx, t.JobId) {
			return done(map[string]int64{"cancelled": 1})
		}
		vars := map[string]any{"media_pk": itoa(sc.MediaPk), "first": CommentPageSize}
		if st.Cursor != "" {
			vars["cursor"] = st.Cursor
		}
		resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: vars, PageID: sc.OwnerPk,
			Fast: t.Lane == igstaskv1.Lane_LANE_P0, Lane: topics.LaneName(t.Lane), Referer: sc.Permalink})
		c["requests"]++
		if err != nil {
			c["transport_errors"]++
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		c["wire_bytes"] += resp.BytesIn + resp.BytesOut
		if resp.Class == detect.LoginWall {
			if wallTruncates(&st, st.Cursor) {
				truncated = true
				break
			}
			_ = d.saveCkpt(ctx, t, st, c)
		}
		if resp.Class != detect.OK {
			o := classOutcome(t, resp.Class, resp.Reason, st.Cursor, c)
			if d.failover(ctx, "post", &id, o, &hops) {
				if o := boot(); o != nil {
					return *o
				}
				continue // the same page, on another exit
			}
			return o
		}
		res, rep := parse.Comments(resp.Parts, cc)
		d.GQL.Parsed(resp, rep.OK())
		for _, f := range rep.Optional {
			metrics.ParseMissingField.WithLabelValues("comments", f).Inc()
		}
		if !rep.OK() {
			metrics.ParseTotal.WithLabelValues("comments", "1", "drift").Inc()
			return classOutcome(t, detect.SchemaDrift, fmt.Sprint(rep.Missing), st.Cursor, c)
		}
		metrics.ParseTotal.WithLabelValues("comments", "1", "ok").Inc()
		if res.NodeNull {
			if st.Pages == 0 && !sc.DetailKnown {
				return s.unavailable(ctx, t, sc, c)
			}
			finished = true
			break
		}
		b := d.Producer.NewBatch()
		src.RawRef = resp.RawRef
		n, older, err := s.emitComments(ctx, b, t, src, res, watermark, maxC, &st, c)
		if err == nil {
			err = b.Wait(ctx)
		}
		if err != nil {
			return retry(t, st.Cursor, "30s", err.Error(), c)
		}
		seen(res)
		st.Pages++
		st.Walls, st.WallCursor = 0, ""
		next(res, n, older)
		if err := d.saveCkpt(ctx, t, st, c); err != nil {
			slog.Warn("post: checkpoint save failed", "err", err)
		}
	}
	c["comment_pages"] += int64(st.Pages)
	if pass && finished && complete && !truncated {
		if err := d.Jobs.EndPass(ctx, t.JobId, t.LineageId, sc.MediaPk, sc.TakenAt.AsTime(), sc.ReportedCommentCount); err != nil {
			slog.Warn("post: full read not recorded", "err", err)
		}
	}
	if finished || truncated {
		// The post's comment coverage, written when the job ends.
		state := jobs.PostComplete
		switch {
		case truncated:
			state = jobs.PostTruncated
			c["posts_truncated"]++
		case capped:
			state = jobs.PostCapped
		case !watermark.IsZero():
			// Only the comments since the last read: as complete as that was.
			state = jobs.PostUpdated
		}
		_ = d.Jobs.UpdatePostState(ctx, t.JobId, sc.MediaPk, state)
		return done(c)
	}
	nt := task.Continuation(t, st.Cursor)
	nt.ItemsSeen = t.ItemsSeen + st.ItemsSeen
	ns := nt.GetPost()
	ns.DetailKnown, ns.ReportedCommentCount = true, sc.ReportedCommentCount
	if !watermark.IsZero() && ns.CommentsWatermark == nil {
		ns.CommentsWatermark = sc.CommentsWatermark
	}
	metrics.TaskYields.WithLabelValues("post").Inc()
	return Outcome{Result: Continue, Next: nt, Counters: c}
}

// publish emits the post and its media. stop is true when the task ends
// here (retry on a produce failure).
func (s *PostStage) publish(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.PostScope, res parse.PostResult, rawRef string, c map[string]int64) (Outcome, bool) {
	d := s.D
	p := res.Post
	if p.OwnerPk == 0 {
		p.OwnerPk = sc.OwnerPk
	}
	if d.Suppression.PageSuppressed(p.OwnerPk) {
		return done(map[string]int64{"posts_suppressed": 1}), true
	}
	// A task from the grid (a full read) knows whether the post is pinned; a
	// scheduled refresh does not, and leaves the stored flag.
	if t.Mode == commonv1.CrawlMode_CRAWL_MODE_FULL {
		pinned := sc.Pinned
		p.IsPinned = &pinned
	}
	if p.ProductType == "clips" && p.DiscoveredVia == igv1.DiscoveredVia_DISCOVERED_VIA_UNSPECIFIED {
		p.DiscoveredVia = igv1.DiscoveredVia_DISCOVERED_VIA_GRID
	}
	src := emit.Source{JobID: t.JobId, Query: "post_page", ParserVersion: parse.PostVersion, Mode: t.Mode, RawRef: rawRef}
	b := d.Producer.NewBatch()
	if err := d.Emit.Post(ctx, b, src, p); err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	n, err := d.media(ctx, b, t, src, res.Media)
	if err != nil {
		return retry(t, "", "30s", err.Error(), c), true
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", "produce: "+err.Error(), c), true
	}
	d.Discovery.Post(p)
	c["posts_detailed"]++
	c["media"] += int64(len(res.Media))
	c["media_queued"] += n
	return Outcome{}, false
}

// unavailable publishes a post that is gone (deleted, archived, or made
// private since it was listed).
func (s *PostStage) unavailable(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.PostScope, c map[string]int64) Outcome {
	d := s.D
	c["posts_unavailable"]++
	if sc.TakenAt == nil {
		sc.TakenAt = timestampFromPK(sc.MediaPk)
	}
	b := d.Producer.NewBatch()
	p := &igv1.Post{MediaPk: sc.MediaPk, Shortcode: sc.Shortcode, OwnerPk: sc.OwnerPk, TakenAt: sc.TakenAt,
		Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusReason: ReasonNotFound}
	if err := d.Emit.Post(ctx, b, emit.Source{JobID: t.JobId, Query: "post_page", Mode: t.Mode}, p); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	_, _ = d.Jobs.SetPost(ctx, t.JobId, sc.MediaPk, sc.TakenAt.Seconds, 0, jobs.PostComplete)
	return done(c)
}

// emitComments publishes one page of comments, up to the job's per-post
// cap. It returns how many were read and how many were older than
// watermark.
func (s *PostStage) emitComments(ctx context.Context, b *kafka.Batch, t *igstaskv1.Task, src emit.Source, res parse.CommentsResult,
	watermark time.Time, maxC int64, st *checkpoint.State, c map[string]int64) (int, int, error) {
	older := 0
	var emitted []*igv1.Comment
	// Discovery counts the commenters once emitted: an erased or
	// pseudonymized author has no username left by then.
	defer func() { s.D.Discovery.Comments(t.GetPost().GetOwnerPk(), emitted) }()
	for _, cm := range res.Comments {
		if maxC > 0 && t.ItemsSeen+st.ItemsSeen >= maxC {
			break
		}
		if cm.CreatedAt != nil {
			if u := cm.CreatedAt.AsTime().Unix(); u > st.NewestSeenUnix {
				st.NewestSeenUnix = u
			}
			if !watermark.IsZero() && cm.CreatedAt.AsTime().Before(watermark) {
				older++
			}
		}
		keep, err := s.D.Emit.Comment(ctx, b, src, cm)
		if err != nil {
			return 0, 0, err
		}
		emitted = append(emitted, cm)
		st.ItemsSeen++
		c["comments"]++
		if !keep {
			c["comments_redacted"]++
		}
	}
	return len(res.Comments), older, nil
}

// Why a post is unavailable (igv1.Post.status_reason, posts.status_reason).
const (
	// ReasonLoginWall: the post page is walled for logged-out readers.
	ReasonLoginWall = "login_wall"
	// ReasonNotFound: the post is gone (deleted, archived or made private).
	ReasonNotFound = "not_found"
)

// walled publishes a post whose page three fresh identities found walled,
// from what the grid gave: its id, shortcode, time and type, with status
// unavailable (login_wall) and its counts unset, never 0 (REQ-008 fix 15).
// A post already stored keeps what was read before; a scheduled refresh's
// task has no grid fields and publishes nothing.
func (s *PostStage) walled(ctx context.Context, t *igstaskv1.Task, sc *igstaskv1.PostScope, c map[string]int64) Outcome {
	d := s.D
	if sc.DetailKnown || sc.TakenAt == nil {
		return done(c)
	}
	if _, _, known := d.storedPost(ctx, sc.MediaPk); known {
		return done(c)
	}
	p := &igv1.Post{MediaPk: sc.MediaPk, Shortcode: sc.Shortcode, OwnerPk: sc.OwnerPk, TakenAt: sc.TakenAt,
		MediaType: sc.MediaType, ProductType: sc.ProductType, Permalink: sc.Permalink,
		DiscoveredVia: igv1.DiscoveredVia_DISCOVERED_VIA_GRID,
		Status:        igv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusReason: ReasonLoginWall}
	if t.Mode == commonv1.CrawlMode_CRAWL_MODE_FULL {
		pinned := sc.Pinned
		p.IsPinned = &pinned
	}
	b := d.Producer.NewBatch()
	if err := d.Emit.Post(ctx, b, emit.Source{JobID: t.JobId, Query: "grid", Mode: t.Mode}, p); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	if err := b.Wait(ctx); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	c["posts_walled_published"]++
	return done(c)
}
