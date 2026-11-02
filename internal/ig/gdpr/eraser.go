// Package gdpr carries out igs's erasure requests (ADR 0005 §1) on schema
// ig and announces each on ig.erasure.v1, so every downstream store deletes
// the subject's data too. Subjects are resolved locally; no Instagram
// request is made.
package gdpr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/suppress"
)

// Eraser processes pending erasure requests. DB is a pool on schema ig.
type Eraser struct {
	DB       *pgxpool.Pool
	S3       *minio.Client // nil: stored objects are left to the janitor's orphan sweep
	Producer *kafka.Producer
	// Suppression, when set, learns the subject at once (in-process
	// crawlers); others reload the table.
	Suppression *suppress.Suppression
}

type request struct {
	id        uuid.UUID
	subject   string
	authorKey []byte
	userPK    *int64
	created   time.Time
}

// Tick processes up to 10 pending requests, oldest first.
func (e *Eraser) Tick(ctx context.Context) error {
	rows, err := e.DB.Query(ctx, `SELECT id, subject_type, author_key, user_pk, created_at FROM erasure_requests
		WHERE status = 'pending' ORDER BY created_at LIMIT 10`)
	if err != nil {
		return err
	}
	reqs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (request, error) {
		var q request
		return q, r.Scan(&q.id, &q.subject, &q.authorKey, &q.userPK, &q.created)
	})
	if err != nil {
		return err
	}
	for _, r := range reqs {
		stats, err := e.process(ctx, r)
		if err != nil {
			slog.Error("igs erasure failed", "request", r.id, "err", err)
			_, _ = e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'failed', error = $2 WHERE id = $1`, r.id, err.Error())
			continue
		}
		sb, _ := json.Marshal(stats)
		if _, err := e.DB.Exec(ctx, `UPDATE erasure_requests SET status = 'done', stats = $2, completed_at = now() WHERE id = $1`, r.id, sb); err != nil {
			return err
		}
		slog.Info("igs erasure done", "request", r.id, "subject", r.subject, "stats", stats)
	}
	return nil
}

func (e *Eraser) process(ctx context.Context, r request) (map[string]int64, error) {
	stats := map[string]int64{}
	ev := &commonv1.ErasureEvent{EventId: r.id.String(), Platform: commonv1.Platform_PLATFORM_INSTAGRAM, RequestId: r.id.String(),
		RequestedAt: timestamppb.New(r.created)}
	switch r.subject {
	case "author":
		if len(r.authorKey) == 0 {
			return nil, fmt.Errorf("author erasure without author key")
		}
		ev.SubjectType, ev.AuthorKey = commonv1.SubjectType_SUBJECT_TYPE_AUTHOR, r.authorKey
		if err := e.eraseAuthor(ctx, r.authorKey, stats); err != nil {
			return nil, err
		}
		if e.Suppression != nil {
			e.Suppression.Add(r.authorKey, 0)
		}
	case "profile":
		if r.userPK == nil || *r.userPK == 0 {
			return nil, fmt.Errorf("profile erasure without user pk")
		}
		ev.SubjectType, ev.AccountId, ev.AuthorKey = commonv1.SubjectType_SUBJECT_TYPE_ACCOUNT, *r.userPK, r.authorKey
		if err := e.eraseProfile(ctx, *r.userPK, stats); err != nil {
			return nil, err
		}
		// The profile's owner is also a person whose comments elsewhere go.
		if len(r.authorKey) > 0 {
			if err := e.eraseAuthor(ctx, r.authorKey, stats); err != nil {
				return nil, err
			}
		}
		if e.Suppression != nil {
			e.Suppression.Add(r.authorKey, *r.userPK)
		}
	default:
		return nil, fmt.Errorf("unknown subject type %q", r.subject)
	}
	ev.CompletedAt = timestamppb.Now()
	b := e.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.Erasure, r.id.String(), ev); err != nil {
		return nil, err
	}
	return stats, b.Wait(ctx)
}

// eraseAuthor suppresses a person's key and redacts their comments: the
// keys and times stay, the text and identity go, and so do their media.
func (e *Eraser) eraseAuthor(ctx context.Context, key []byte, stats map[string]int64) error {
	var comments []int64
	err := pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1) ON CONFLICT DO NOTHING`,
			hex.EncodeToString(key)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `UPDATE comments SET text = NULL, author_pk = NULL, extra = NULL, redacted = true
			WHERE author_key = $1 RETURNING comment_pk`, key)
		if err != nil {
			return err
		}
		if comments, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM comment_authors WHERE author_key = $1`, key)
		stats["authors_deleted"] += tag.RowsAffected()
		return err
	})
	if err != nil {
		return err
	}
	stats["comments_redacted"] += int64(len(comments))
	n, err := e.deleteMedia(ctx, `comment_pk = ANY($1)`, comments)
	stats["media_deleted"] += n
	return err
}

// eraseProfile suppresses an account and deletes everything collected
// from it; its row stays, emptied and marked suppressed, so it is never
// read or written again.
func (e *Eraser) eraseProfile(ctx context.Context, pk int64, stats map[string]int64) error {
	if _, err := e.DB.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('profile', $1) ON CONFLICT DO NOTHING`,
		strconv.FormatInt(pk, 10)); err != nil {
		return err
	}
	n, err := e.deleteMedia(ctx, `user_pk = $1`, pk)
	if err != nil {
		return err
	}
	stats["media_deleted"] += n
	return pgx.BeginFunc(ctx, e.DB, func(tx pgx.Tx) error {
		for _, q := range []struct{ name, sql string }{
			{"comments", `DELETE FROM comments WHERE owner_pk = $1`},
			{"post_metrics", `DELETE FROM post_metrics WHERE owner_pk = $1`},
			{"post_owners", `DELETE FROM post_owners WHERE media_pk IN (SELECT media_pk FROM post_index WHERE owner_pk = $1)`},
			{"post_refresh", `DELETE FROM post_refresh WHERE user_pk = $1`},
			{"posts", `DELETE FROM posts WHERE owner_pk = $1`},
			{"post_index", `DELETE FROM post_index WHERE owner_pk = $1`},
			{"highlights", `DELETE FROM highlights WHERE user_pk = $1`},
			{"profile_snapshots", `DELETE FROM profile_snapshots WHERE user_pk = $1`},
			{"crawl_cursors", `DELETE FROM crawl_cursors WHERE scope_id = $1 AND scope_type IN ('profile_grid','profile_reels')`},
			{"schedule", `UPDATE profile_schedule SET enabled = false WHERE user_pk = $1`},
			{"profiles", `UPDATE profiles SET status = 'suppressed', username = NULL, full_name = NULL, biography = NULL,
				bio_links = NULL, follower_count = NULL, following_count = NULL, media_count = NULL, profile_pic_url = NULL,
				linked_facebook_page = NULL, category = NULL, highlights_hash = NULL, extra = NULL WHERE user_pk = $1`},
		} {
			tag, err := tx.Exec(ctx, q.sql, pk)
			if err != nil {
				return fmt.Errorf("%s: %w", q.name, err)
			}
			stats[q.name] = tag.RowsAffected()
		}
		return nil
	})
}

// deleteMedia removes the stored objects of the media matching where (one
// argument) and marks the rows deleted.
func (e *Eraser) deleteMedia(ctx context.Context, where string, arg any) (int64, error) {
	rows, err := e.DB.Query(ctx, `SELECT media_id::text, s3_bucket, s3_key FROM media WHERE `+where+` AND status <> 'deleted'`, arg)
	if err != nil {
		return 0, err
	}
	type obj struct {
		id          string
		bucket, key *string
	}
	objs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (obj, error) {
		var o obj
		return o, r.Scan(&o.id, &o.bucket, &o.key)
	})
	if err != nil || len(objs) == 0 {
		return 0, err
	}
	ids := make([]string, 0, len(objs))
	for _, o := range objs {
		if e.S3 != nil && o.bucket != nil && o.key != nil {
			if err := e.S3.RemoveObject(ctx, *o.bucket, *o.key, minio.RemoveObjectOptions{}); err != nil {
				return 0, err
			}
		}
		ids = append(ids, o.id)
	}
	tag, err := e.DB.Exec(ctx, `UPDATE media SET status = 'deleted', deleted_at = now(), source_url = NULL, alt_text = NULL
		WHERE media_id::text = ANY($1)`, ids)
	return tag.RowsAffected(), err
}
