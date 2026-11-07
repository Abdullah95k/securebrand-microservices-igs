package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Finalizer turns jobs whose tasks all finished terminal, once the
// persister has written everything published until then (or Timeout
// passed), and publishes ig.job.v1.
type Finalizer struct {
	DB       *pgxpool.Pool // schema ig
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
	Cfg      *config.Config
	Timeout  time.Duration
	// PersisterGroup defaults to igs's persister group.
	PersisterGroup string

	held    map[string]bool
	pending []finalBatch
}

type finalBatch struct {
	jobs  []string
	ends  kadm.ListedOffsets
	since time.Time
}

// Tick takes the jobs ready to finalize and finalizes those the persister
// has caught up with.
func (f *Finalizer) Tick(ctx context.Context) error {
	if f.held == nil {
		f.held = map[string]bool{}
	}
	ids, err := f.Jobs.ReadyToFinalize(ctx, 500)
	if err != nil {
		return err
	}
	var fresh []string
	for _, id := range ids {
		if !f.held[id] {
			fresh = append(fresh, id)
		}
	}
	if len(fresh) > 0 {
		names := make([]string, len(topics.Entity))
		for i, t := range topics.Entity {
			names[i] = f.Producer.Topic(t)
		}
		ends, err := kafka.EndOffsets(ctx, f.Producer.Client(), names...)
		if err != nil {
			return err
		}
		for _, id := range fresh {
			f.held[id] = true
		}
		f.pending = append(f.pending, finalBatch{jobs: fresh, ends: ends, since: time.Now()})
	}
	group := f.PersisterGroup
	if group == "" {
		group = f.Producer.Topic(persist.Group)
	}
	keep := f.pending[:0]
	for _, b := range f.pending {
		caught, err := kafka.GroupCaughtUp(ctx, f.Producer.Client(), group, b.ends)
		if err != nil {
			slog.Warn("igs finalizer: offset check failed", "err", err)
		}
		timedOut := time.Since(b.since) > f.Timeout
		if !caught && !timedOut {
			keep = append(keep, b)
			continue
		}
		if timedOut && !caught {
			slog.Warn("igs finalizer: persister lag; finalizing anyway", "jobs", len(b.jobs))
		}
		for _, id := range b.jobs {
			delete(f.held, id)
			if err := f.Finalize(ctx, id); err != nil {
				slog.Error("igs finalizer: job failed to finalize", "job", id, "err", err)
				continue // still in the set: taken again on the next tick
			}
			if err := f.Jobs.Finalized(ctx, id); err != nil {
				slog.Warn("igs finalizer: could not clear a finalized job", "job", id, "err", err)
			}
		}
	}
	f.pending = keep
	return nil
}

// Finalize makes one job terminal: its status from its counters and post
// states, its completeness, the schedule's failure count, the comment
// refreshes of the profile's recent posts, and its ig.job.v1 event.
func (f *Finalizer) Finalize(ctx context.Context, id string) error {
	jid, err := uuid.Parse(id)
	if err != nil {
		return nil // not one of igs's jobs
	}
	var status, kind, ref string
	var lane int16
	var userPK *int64
	var created time.Time
	err = f.DB.QueryRow(ctx, `SELECT status, kind, lane, profile_ref, user_pk, created_at FROM jobs WHERE job_id = $1`, jid).
		Scan(&status, &kind, &lane, &ref, &userPK, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load job: %w", err)
	}
	if status != "running" && status != "cancelling" && status != "finalizing" {
		return nil // already terminal
	}
	if userPK == nil {
		// A profile first read by this job.
		var pk int64
		if name, ok := strings.CutPrefix(ref, "u:"); ok &&
			f.DB.QueryRow(ctx, `SELECT user_pk FROM profiles WHERE lower(username) = lower($1)`, name).Scan(&pk) == nil {
			userPK = &pk
		}
	}
	st, err := f.Jobs.Status(ctx, id)
	if err != nil {
		return err
	}
	posts, err := f.Jobs.Posts(ctx, id)
	if err != nil {
		return err
	}
	comp, err := f.completeness(ctx, posts, st.Counters)
	if err != nil {
		return err
	}
	deleted, err := f.deletions(ctx, id, status == "cancelling")
	if err != nil {
		return fmt.Errorf("deletions: %w", err)
	}
	if deleted > 0 {
		st.Counters["comments_deleted"] += deleted
	}
	c := st.Counters
	final := "completed"
	switch {
	case status == "cancelling":
		final = "cancelled"
	case c["profiles"] == 0 && c["posts"] == 0 && (c["profiles_unavailable"] > 0 || c["tasks_failed"] > 0):
		final = "failed"
	case c["tasks_failed"] > 0 || c["timelines_truncated"] > 0 || comp.PostsTruncated > 0:
		final = "partial"
	}
	cb, _ := json.Marshal(c)
	compb, _ := json.Marshal(comp)
	now := time.Now().UTC()
	err = pgx.BeginFunc(ctx, f.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $2, counters = $3, completeness = $4, finished_at = $5,
			user_pk = COALESCE(user_pk, $6) WHERE job_id = $1`, jid, final, cb, compb, now, userPK); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE profile_schedule SET user_pk = COALESCE(user_pk, $2),
			consecutive_failures = CASE WHEN $3 IN ('completed','partial') THEN 0 ELSE consecutive_failures + 1 END
			WHERE last_job_id = $1`, jid, userPK, final); err != nil {
			return err
		}
		if err := coverage(ctx, tx, posts); err != nil {
			return fmt.Errorf("coverage: %w", err)
		}
		if userPK == nil || final == "cancelled" || final == "failed" {
			return nil
		}
		// The profile's recent posts get comment refreshes; older ones leave.
		g := f.Cfg.IG.Schedule
		if _, err := tx.Exec(ctx, `INSERT INTO post_refresh (media_pk, user_pk, taken_at, next_refresh_at, last_comment_count)
			SELECT p.media_pk, p.owner_pk, p.taken_at, now() + $3::int * interval '1 second', p.comment_count FROM posts p
			WHERE p.owner_pk = $1 AND p.status = 'visible' AND p.kind <> 'highlight' AND p.taken_at > now() - $2::int * interval '1 second'
			ON CONFLICT (media_pk) DO UPDATE SET last_comment_count = EXCLUDED.last_comment_count`,
			*userPK, int(g.PostRefreshWindow.Seconds()), int(g.Interval.Seconds())); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM post_refresh WHERE user_pk = $1 AND taken_at <= now() - $2::int * interval '1 second'`,
			*userPK, int(g.PostRefreshWindow.Seconds()))
		return err
	})
	if err != nil {
		return err
	}
	ev := &commonv1.JobEvent{
		EventId: id + ":" + final, Platform: commonv1.Platform_PLATFORM_INSTAGRAM, JobId: id, Status: jobStatus(final),
		Kind: kind, SubjectRef: ref, CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(now), Counters: c,
		Completeness: &commonv1.Completeness{Posts: comp.Posts, PostsComplete: comp.PostsComplete, PostsTruncated: comp.PostsTruncated,
			MedianRatio: comp.MedianRatio, MinRatio: comp.MinRatio, CommentsReported: comp.CommentsReported,
			CommentsCollected: comp.CommentsCollected},
	}
	if userPK != nil {
		ev.SubjectId = *userPK
	}
	b := f.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Job, id, ev); err != nil {
		return err
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	metrics.Jobs.WithLabelValues(final, topics.LaneName(igstaskv1.Lane(lane))).Inc()
	_ = f.Jobs.Expire(ctx, id, 24*time.Hour)
	slog.Info("igs job finished", "job", id, "status", final, "posts", c["posts"], "comments", c["comments"],
		"reels", c["reels"], "complete", comp.PostsComplete, "truncated", comp.PostsTruncated)
	return nil
}

// Completeness is stored on the job. On Instagram a post's comment count
// includes replies, which logged-out readers do not get, so the ratios are
// information, never a failure (ADR 0005 §5).
type Completeness struct {
	Posts          int64 `json:"posts"`
	PostsComplete  int64 `json:"posts_complete"`
	PostsTruncated int64 `json:"posts_truncated"`
	PostsSkipped   int64 `json:"posts_skipped"`
	// Read up to the job's comment cap (max_comments_per_post).
	PostsCapped       int64   `json:"posts_capped"`
	PostsGated        int64   `json:"posts_gated"`
	MedianRatio       float64 `json:"median_ratio"`
	MinRatio          float64 `json:"min_ratio"`
	CommentsReported  int64   `json:"comments_reported"`
	CommentsCollected int64   `json:"comments_collected"`
}

func (f *Finalizer) completeness(ctx context.Context, posts []jobs.PostState, c map[string]int64) (Completeness, error) {
	var comp Completeness
	var pks []int64
	for _, p := range posts {
		comp.Posts++
		switch p.State {
		case jobs.PostComplete, jobs.PostUpdated:
			comp.PostsComplete++
		case jobs.PostTruncated:
			comp.PostsTruncated++
		case jobs.PostSkipped, jobs.PostGated:
			comp.PostsSkipped++
		case jobs.PostCapped:
			comp.PostsCapped++
		}
		pks = append(pks, p.PostID)
	}
	comp.PostsGated = c["posts_gated"] // recorded skipped
	if len(pks) == 0 {
		return comp, nil
	}
	rows, err := f.DB.Query(ctx, `SELECT coalesce(p.comment_count, 0),
			(SELECT count(*) FROM comments c WHERE c.taken_at = p.taken_at AND c.media_pk = p.media_pk AND c.depth = 0)
		FROM posts p JOIN post_index i ON i.media_pk = p.media_pk AND i.taken_at = p.taken_at WHERE i.media_pk = ANY($1)`, pks)
	if err != nil {
		return comp, err
	}
	defer rows.Close()
	var ratios []float64
	for rows.Next() {
		var reported, collected int64
		if err := rows.Scan(&reported, &collected); err != nil {
			return comp, err
		}
		comp.CommentsReported += reported
		comp.CommentsCollected += collected
		if reported > 0 {
			ratios = append(ratios, min(float64(collected)/float64(reported), 1))
		}
	}
	if err := rows.Err(); err != nil {
		return comp, err
	}
	if len(ratios) > 0 {
		sort.Float64s(ratios)
		comp.MinRatio, comp.MedianRatio = ratios[0], ratios[len(ratios)/2]
	}
	return comp, nil
}

// coverage writes each visible post's comment coverage (REQ-008 fix 15):
// posts.comments_state from how the job's read of its comments ended, and
// posts.comments_collected, its visible top-level comments stored now.
//   - complete: the read went from the first page to the last;
//   - truncated: a login wall stopped it;
//   - partial: a cap stopped it, or it did not finish (a failed task, a
//     cancelled job);
//   - a scheduled read of the comments since the last one keeps the state
//     that read left ("none", from before coverage was written, becomes
//     partial);
//   - a post with no comments is complete; a post skipped for another
//     reason (unchanged, or comments not asked for) keeps its state.
//
// Logged out, Instagram serves no replies, so collected is compared with
// the post's comment count only as information. Comments still on their
// way to the store are counted at the post's next read. Posts that are
// unavailable (walled or gone) are left as they are.
func coverage(ctx context.Context, tx pgx.Tx, posts []jobs.PostState) error {
	if len(posts) == 0 {
		return nil
	}
	ids := make([]int64, len(posts))
	states := make([]string, len(posts))
	reported := make([]int64, len(posts))
	for i, p := range posts {
		ids[i], states[i], reported[i] = p.PostID, p.State, p.Reported
	}
	_, err := tx.Exec(ctx, `UPDATE posts p SET
			comments_state = CASE
				WHEN s.state = 'complete' THEN 'complete'
				WHEN s.state = 'truncated' THEN 'truncated'
				WHEN s.state IN ('capped', 'pending', 'failed') THEN 'partial'
				WHEN s.state = 'updated' THEN CASE WHEN p.comments_state = 'none' THEN 'partial' ELSE p.comments_state END
				WHEN s.state = 'skipped' AND s.reported = 0 AND coalesce(p.comment_count, 0) = 0 THEN 'complete'
				ELSE p.comments_state END,
			comments_collected = (SELECT count(*) FROM comments c
				WHERE c.taken_at = p.taken_at AND c.media_pk = p.media_pk AND c.depth = 0 AND c.status = 'visible')
		FROM post_index i, unnest($1::bigint[], $2::text[], $3::bigint[]) AS s(media_pk, state, reported)
		WHERE i.media_pk = s.media_pk AND p.taken_at = i.taken_at AND p.media_pk = i.media_pk
			AND p.status = 'visible' AND s.state <> 'gated'`, ids, states, reported)
	return err
}

func jobStatus(s string) commonv1.JobStatus {
	switch s {
	case "completed":
		return commonv1.JobStatus_JOB_STATUS_COMPLETED
	case "partial":
		return commonv1.JobStatus_JOB_STATUS_PARTIAL
	case "failed":
		return commonv1.JobStatus_JOB_STATUS_FAILED
	case "cancelled":
		return commonv1.JobStatus_JOB_STATUS_CANCELLED
	}
	return commonv1.JobStatus_JOB_STATUS_UNSPECIFIED
}

// deletions compares the stored comments of each post read in full during
// the job with what that read returned (REQ-008 fix 14). A comment stored
// before the read began and not returned by it is published as deleted on
// ig.comment.v1 and marked deleted in the store; its text and history stay.
// It returns how many were marked; a cancelled job only drops its pass
// records.
func (f *Finalizer) deletions(ctx context.Context, job string, cancelled bool) (int64, error) {
	passes, posts, err := f.Jobs.Passes(ctx, job)
	if err != nil || len(posts) == 0 {
		return 0, err
	}
	var marked int64
	if f.Cfg.Scheduler.PostRefresh.DetectDeletions && !cancelled {
		var dels []*igv1.Comment
		for _, p := range passes {
			ds, err := f.missing(ctx, job, p)
			if err != nil {
				return 0, err
			}
			dels = append(dels, ds...)
		}
		if len(dels) > 0 {
			// One time for the whole check, so the store and the events agree.
			at := time.Now().UTC()
			em := emit.Emitter{Now: func() time.Time { return at }}
			src := emit.Source{JobID: job, Query: "comments", Mode: commonv1.CrawlMode_CRAWL_MODE_FULL}
			b := f.Producer.NewBatch()
			for _, d := range dels {
				if err := em.CommentDeleted(ctx, b, src, d); err != nil {
					return 0, err
				}
			}
			if err := b.Wait(ctx); err != nil {
				return 0, err
			}
			// The persister may apply the events first; either way each comment
			// is marked once, at the same time.
			for _, d := range dels {
				d.Obs = &commonv1.Observation{ObservedAt: timestamppb.New(at)}
			}
			if _, err := persist.MarkDeleted(ctx, f.DB, dels); err != nil {
				return 0, err
			}
			marked = int64(len(dels))
			// Before the pass records go: a retried finalize finds these
			// already marked and does not count them again.
			if err := f.Jobs.AddCounters(ctx, job, map[string]int64{"comments_deleted": marked}); err != nil {
				return 0, err
			}
		}
	}
	return marked, f.Jobs.ClearPasses(ctx, job, posts)
}

// missing returns the stored comments of p's post stored before the read
// began and not returned by it. Only a complete read counts: the comments
// it returned, each with its reply count, must add up to the post's comment
// count from the same read (within scheduler.post_refresh.
// deletion_count_slack), or Instagram may have answered partially and the
// post is skipped.
func (f *Finalizer) missing(ctx context.Context, job string, p jobs.Pass) ([]*igv1.Comment, error) {
	seen, err := f.Jobs.Seen(ctx, job, p.PostID)
	if err != nil {
		return nil, err
	}
	var covered int64
	for _, replies := range seen {
		covered += 1 + replies
	}
	slack := int64(math.Round(f.Cfg.Scheduler.PostRefresh.DeletionCountSlack * float64(p.Reported)))
	if p.Reported <= 0 || covered < p.Reported-slack || covered > p.Reported+slack {
		metrics.DeletionChecks.WithLabelValues("skipped_partial").Inc()
		slog.Debug("igs finalizer: partial comment read; not checked for deletions", "job", job, "post", p.PostID,
			"returned", covered, "reported", p.Reported)
		return nil, nil
	}
	metrics.DeletionChecks.WithLabelValues("checked").Inc()
	rows, err := f.DB.Query(ctx, `SELECT comment_pk, owner_pk, created_at FROM comments
		WHERE taken_at = $1 AND media_pk = $2 AND depth = 0 AND status = 'visible' AND NOT redacted
		  AND first_seen_at < $3`, p.PostCreated, p.PostID, p.Start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*igv1.Comment
	for rows.Next() {
		var id, owner int64
		var created *time.Time
		if err := rows.Scan(&id, &owner, &created); err != nil {
			return nil, err
		}
		if _, ok := seen[id]; !ok {
			c := &igv1.Comment{CommentPk: id, MediaPk: p.PostID, OwnerPk: owner, PostTakenAt: timestamppb.New(p.PostCreated)}
			if created != nil {
				c.CreatedAt = timestamppb.New(*created)
			}
			out = append(out, c)
		}
	}
	return out, rows.Err()
}
