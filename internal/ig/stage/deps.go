// Package stage implements igs's crawl stages (profile, post) and the
// worker that runs them from Kafka. The worker's task lifecycle is fbs's
// (duplicate suppression, cancellation, stream leases, retries with
// escalating delay, the dead letter queue and job accounting), on igs's
// own task envelope.
package stage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/suppress"
)

// Deps are the collaborators every stage uses.
type Deps struct {
	Cfg         *config.Config
	Pool        *session.Pool
	Proxies     *proxy.Pool
	Loader      *session.Loader
	GQL         *graphql.Client
	Emit        *emit.Emitter
	Producer    *kafka.Producer
	Jobs        *jobs.Tracker
	Ckpt        *checkpoint.Store
	DB          *pgxpool.Pool // schema ig: crawl_cursors; nil disables watermarks
	Suppression *suppress.Suppression
	Now         func() time.Time
}

func (d *Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now().UTC()
}

func (d *Deps) base() string { return d.Cfg.IG.BaseURL }

// Result of one stage run.
type Result int

const (
	// Done: the task finished.
	Done Result = iota
	// Continue: the task yielded; Next continues the same stream.
	Continue
	// Retry: transient failure; Next is the same task to retry later.
	Retry
	// Fail: permanent failure; the task goes to the DLQ.
	Fail
)

// Outcome is what a stage returns to the worker.
type Outcome struct {
	Result   Result
	Next     *igstaskv1.Task
	Delay    string // retry topic: "30s" | "5m"
	Reason   string
	Counters map[string]int64
	// Class is the response class a Retry is for (detect.OK when none).
	Class detect.Class
	// Park: a Retry that is no fault of the task (every exit rests). It
	// waits without counting a stall toward ig's max_attempts.
	Park bool
	// FollowUps are new streams to start once this task released its lease.
	FollowUps []*igstaskv1.Task
}

func done(c map[string]int64) Outcome { return Outcome{Result: Done, Counters: c} }

func retry(t *igstaskv1.Task, cursor, delay, reason string, c map[string]int64) Outcome {
	return Outcome{Result: Retry, Next: task.Retry(t, cursor), Delay: delay, Reason: reason, Counters: c}
}

// Stage runs one task kind.
type Stage interface {
	Run(ctx context.Context, t *igstaskv1.Task) Outcome
}

// identity acquires an identity for a query class, escalating tiers on
// retries that made no progress (cheapest first) and skipping tiers whose
// exits all cool down.
func (d *Deps) identity(ctx context.Context, class string, stalls uint32) (*session.Identity, error) {
	tiers := d.Proxies.TiersFor(class)
	from := min(int(stalls), len(tiers)-1)
	tier := tiers[from]
	for _, t := range tiers[from:] {
		if d.Proxies.Available(t) {
			tier = t
			break
		}
	}
	return d.Pool.Acquire(ctx, tier)
}

// noIdentity is the outcome of a task that got no identity. With every
// exit of its tiers resting (proxy.ErrTierOpen) it parks: it waits 5
// minutes, then tries again, without counting a stall, since nothing about
// the task failed. Only max_total_attempts bounds it (50 parks: about 4
// hours with no usable exit).
func noIdentity(t *igstaskv1.Task, cursor string, err error, c map[string]int64) Outcome {
	if errors.Is(err, proxy.ErrTierOpen) {
		o := retry(t, cursor, "5m", "every exit rests: "+err.Error(), c)
		o.Park = true
		return o
	}
	return retry(t, cursor, "30s", "no identity: "+err.Error(), c)
}

// failover replaces *id, just blocked as o says, with an identity on
// another exit of its tier, so the task carries on at once instead of
// waiting for a retry: at most ig.block_hops times a run. The blocked exit
// rests already when the block was a rate limit or a checkpoint (Instagram
// limits per IP); a login wall retired the identity alone. It returns
// false, leaving *id as it was, when o is no block, the hops are used up,
// or no other exit is free; the caller then returns o. The new identity
// may need bootstrapping.
func (d *Deps) failover(ctx context.Context, stage string, id **session.Identity, o Outcome, hops *int) bool {
	if o.Result != Retry || !o.Class.Blocking() || *hops >= d.Cfg.IG.BlockHops {
		return false
	}
	old := *id
	actx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	nid, err := d.Pool.AcquireElsewhere(actx, old.Endpoint.Tier, old.Endpoint.Exit())
	if err != nil {
		slog.Debug("failover: no other exit", "stage", stage, "class", o.Class.String(), "err", err)
		return false
	}
	*hops++
	d.Pool.Release(old)
	*id = nid
	metrics.IdentityHops.WithLabelValues(stage, o.Class.String()).Inc()
	if o.Counters != nil {
		o.Counters["identity_hops"]++
	}
	slog.Debug("failover", "stage", stage, "class", o.Class.String(), "from", old.Endpoint.Exit(), "to", nid.Endpoint.Exit())
	return true
}

// observeLoad reports a page load to the traffic budget and AIMD.
func (d *Deps) observeLoad(pl *session.PageLoad, err error, lane igstaskv1.Lane) {
	if d.GQL == nil || d.GQL.Outcomes == nil {
		return
	}
	switch {
	case pl != nil:
		d.GQL.Outcomes.Observe(pl.Class, topics.LaneName(lane), pl.Bytes)
	case err != nil:
		d.GQL.Outcomes.Observe(detect.Transport, topics.LaneName(lane), 0)
	}
}

// load fetches a page with id, bootstrapping it if it has no tokens yet.
// An exit that walls a fresh identity's first page load rests
// (ig.page_wall_rest): some ISP exits send every page load to the login
// page (the owner's details run), and further identities on them only
// waste requests.
func (d *Deps) load(ctx context.Context, id *session.Identity, url string, lane igstaskv1.Lane) (*session.PageLoad, error) {
	fresh := !id.Bootstrapped()
	pl, err := d.Loader.Load(ctx, id, url)
	d.observeLoad(pl, err, lane)
	if pl != nil && pl.Class == detect.LoginWall && fresh && id.Endpoint.URL != "" && d.Cfg.IG.PageWallRest > 0 {
		d.Pool.RestExit(id, d.Cfg.IG.PageWallRest, "page_wall")
	}
	if err != nil {
		return pl, err
	}
	if pl.Class == detect.OK && !id.Bootstrapped() {
		return pl, fmt.Errorf("%w: no tokens on %s", session.ErrBootstrap, url)
	}
	return pl, nil
}

// classOutcome maps a non-OK response class to an outcome for task t.
func classOutcome(t *igstaskv1.Task, cls detect.Class, reason, cursor string, c map[string]int64) Outcome {
	if c == nil {
		c = map[string]int64{}
	}
	switch cls {
	case detect.RateLimited, detect.Checkpoint, detect.LoginWall:
		c["blocked"]++
	case detect.Transport:
		c["transport_errors"]++
	case detect.SoftError:
		c["soft_errors"]++
	case detect.RegistryStale, detect.SchemaDrift:
		c["drift"]++
	}
	delay := "30s"
	if cls == detect.RegistryStale || cls == detect.SchemaDrift {
		// Needs a registry or parser update; wait longer.
		delay = "5m"
	}
	o := retry(t, cursor, delay, cls.String()+": "+reason, c)
	o.Class = cls
	return o
}

// spawn registers child tasks with the job and produces them.
func (d *Deps) spawn(ctx context.Context, children ...*igstaskv1.Task) error {
	if len(children) == 0 {
		return nil
	}
	ids := make([]string, len(children))
	recs := make([]*kgo.Record, 0, len(children))
	for i, c := range children {
		ids[i] = c.TaskId
		topic, ok := topics.TaskTopic(c.Kind, c.Lane)
		if !ok {
			return fmt.Errorf("stage: spawn: kind %v has no topic", c.Kind)
		}
		rec, err := d.Producer.Record(topic, task.ScopeKey(c), c)
		if err != nil {
			return err
		}
		recs = append(recs, rec)
	}
	if err := d.Jobs.Spawn(ctx, children[0].JobId, ids...); err != nil {
		return fmt.Errorf("stage: spawn: %w", err)
	}
	err := d.Producer.ProduceSync(ctx, recs...)
	slog.Debug("tasks spawned", "job", children[0].JobId, "tasks", ids, "err", err)
	return err
}

// media publishes media metadata, and download tasks when the job asks for
// files (D5: off by default).
func (d *Deps) media(ctx context.Context, b *kafka.Batch, t *igstaskv1.Task, src emit.Source, ms []*igv1.Media) (int64, error) {
	lim := t.Limits
	var queued int64
	hasHD := map[string]bool{}
	for _, m := range ms {
		if m.Variant == "hd" {
			hasHD[m.SourceMediaId] = true
		}
	}
	for _, m := range ms {
		if err := d.Emit.Media(ctx, b, src, m); err != nil {
			return queued, err
		}
		if !lim.GetIncludeMedia() || !wanted(m, lim.GetVideoPolicy(), hasHD[m.SourceMediaId]) {
			continue
		}
		mt := task.NewMedia(t.JobId, m)
		rec, err := d.Producer.Record(topics.TaskMedia, m.MediaId, mt)
		if err != nil {
			return queued, err
		}
		b.Add(ctx, rec)
		queued++
	}
	return queued, nil
}

func wanted(m *igv1.Media, vp igstaskv1.VideoPolicy, hasHD bool) bool {
	switch m.Variant {
	case "hd":
		return vp == igstaskv1.VideoPolicy_VIDEO_POLICY_HD
	case "sd":
		return vp == igstaskv1.VideoPolicy_VIDEO_POLICY_SD || vp == igstaskv1.VideoPolicy_VIDEO_POLICY_UNSPECIFIED ||
			(vp == igstaskv1.VideoPolicy_VIDEO_POLICY_HD && !hasHD)
	case "thumbnail":
		return vp != igstaskv1.VideoPolicy_VIDEO_POLICY_NONE
	}
	return true
}

// watermark returns the newest post time stored for a profile's grid.
func (d *Deps) watermark(ctx context.Context, userPK int64) (time.Time, bool, error) {
	if d.DB == nil || userPK == 0 {
		return time.Time{}, false, nil
	}
	var ts *time.Time
	err := d.DB.QueryRow(ctx, `SELECT newest_seen_at FROM crawl_cursors
		WHERE scope_type = 'profile_grid' AND scope_id = $1 AND ordering = 'default'`, userPK).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) || ts == nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return *ts, true, nil
}

// saveWatermark records the end of a grid pass.
func (d *Deps) saveWatermark(ctx context.Context, userPK int64, newest, oldest time.Time, state, reason string, full bool) error {
	if d.DB == nil || userPK == 0 {
		return nil
	}
	var nw, ol *time.Time
	if !newest.IsZero() {
		nw = &newest
	}
	if !oldest.IsZero() {
		ol = &oldest
	}
	_, err := d.DB.Exec(ctx, `
		INSERT INTO crawl_cursors (scope_type, scope_id, ordering, newest_seen_at, oldest_seen_at, state,
			truncated_reason, last_full_at, last_incremental_at, updated_at)
		VALUES ('profile_grid', $1, 'default', $2, $3, $4, NULLIF($5, ''),
			CASE WHEN $6 THEN now() END, CASE WHEN NOT $6 THEN now() END, now())
		ON CONFLICT (scope_type, scope_id, ordering) DO UPDATE SET
			newest_seen_at = GREATEST(crawl_cursors.newest_seen_at, EXCLUDED.newest_seen_at),
			oldest_seen_at = LEAST(crawl_cursors.oldest_seen_at, EXCLUDED.oldest_seen_at),
			state = EXCLUDED.state, truncated_reason = EXCLUDED.truncated_reason,
			last_full_at = COALESCE(EXCLUDED.last_full_at, crawl_cursors.last_full_at),
			last_incremental_at = COALESCE(EXCLUDED.last_incremental_at, crawl_cursors.last_incremental_at),
			updated_at = now()`, userPK, nw, ol, state, reason, full)
	return err
}

// storedCommentState is a post's comment count and newest comment time as
// last stored, for incremental comment reads.
func (d *Deps) storedPost(ctx context.Context, mediaPK int64) (count int64, newest time.Time, ok bool) {
	if d.DB == nil {
		return 0, time.Time{}, false
	}
	var c *int64
	var n *time.Time
	err := d.DB.QueryRow(ctx, `SELECT p.comment_count, (SELECT max(created_at) FROM comments c WHERE c.media_pk = p.media_pk)
		FROM posts p JOIN post_index i ON i.media_pk = p.media_pk AND i.taken_at = p.taken_at WHERE p.media_pk = $1`, mediaPK).Scan(&c, &n)
	if err != nil {
		return 0, time.Time{}, false
	}
	if c != nil {
		count = *c
	}
	if n != nil {
		newest = *n
	}
	return count, newest, true
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
