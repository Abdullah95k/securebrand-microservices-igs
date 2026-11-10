package media

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	mediav1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/media/v1"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Janitor deletes stored media once every required consumer acknowledged
// it, and enforces the TTL as a safety net. DB is a pool on schema ig.
type Janitor struct {
	DB                   *pgxpool.Pool
	S3                   *minio.Client
	Bucket               string
	Producer             *kafka.Producer
	RequiredConsumers    []string
	MaxRetryableFailures int
	TTL                  time.Duration
	// Prefix is igs's part of the bucket (s3.media_prefix): the orphan
	// sweep lists and deletes under it alone, never elsewhere in a shared
	// bucket.
	Prefix string
}

// Handle consumes content.media.processed.v1. Every collector's consumers
// acknowledge on it, so acks for media that isn't igs's are skipped.
func (j *Janitor) Handle(ctx context.Context, rec *kgo.Record) error {
	var ack mediav1.MediaProcessed
	if err := proto.Unmarshal(rec.Value, &ack); err != nil {
		slog.Error("igs janitor: undecodable ack", "err", err)
		return nil
	}
	id, ours := normalize.NativeMediaID(ack.MediaId)
	if !ours || ack.Consumer == "" {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(ack.Status.String(), "PROCESS_STATUS_"))
	at := time.Now().UTC()
	if ack.ProcessedAt != nil {
		at = ack.ProcessedAt.AsTime()
	}
	detail := ack.Error
	if ack.Reason != "" {
		detail = strings.TrimSpace(ack.Reason + ": " + ack.Error)
	}
	tag, err := j.DB.Exec(ctx, `INSERT INTO media_acks (media_id, consumer, status, error, acked_at)
		SELECT $1::uuid, $2, $3, NULLIF($4,''), $5 WHERE EXISTS (SELECT 1 FROM media WHERE media_id = $1::uuid)
		ON CONFLICT (media_id, consumer) DO UPDATE SET status = EXCLUDED.status, error = EXCLUDED.error, acked_at = EXCLUDED.acked_at`,
		id, ack.Consumer, status, detail, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // unknown media (already purged)
	}
	if ack.Status == mediav1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE {
		return j.retry(ctx, id)
	}
	return j.deleteIfAcked(ctx, id)
}

// deleteIfAcked deletes the object when all required consumers are done:
// processed, failed for good, or skipped by their policy.
func (j *Janitor) deleteIfAcked(ctx context.Context, mediaID string) error {
	var done int
	if err := j.DB.QueryRow(ctx, `SELECT count(DISTINCT consumer) FROM media_acks WHERE media_id = $1
		AND consumer = ANY($2) AND status IN ('processed','failed_permanent','skipped')`, mediaID, j.RequiredConsumers).Scan(&done); err != nil {
		return err
	}
	if done < len(j.RequiredConsumers) {
		return nil
	}
	return j.Delete(ctx, mediaID, mediav1.DeleteReason_DELETE_REASON_PROCESSED)
}

// MaxReannouncements is the contract's limit: a file is announced again at
// most 3 times after retryable failures.
const MaxReannouncements = 3

// retry announces the object again after a retryable failure, at most
// min(MaxRetryableFailures, 3) times; after that it is done with.
func (j *Janitor) retry(ctx context.Context, mediaID string) error {
	var failures int
	err := j.DB.QueryRow(ctx, `UPDATE media SET retryable_failures = retryable_failures + 1 WHERE media_id = $1 AND status = 'ready'
		RETURNING retryable_failures`, mediaID).Scan(&failures)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if failures > min(j.MaxRetryableFailures, MaxReannouncements) {
		return j.Delete(ctx, mediaID, mediav1.DeleteReason_DELETE_REASON_PROCESSED)
	}
	ready, err := readyEvent(ctx, j.DB, mediaID, int32(failures+1), j.RequiredConsumers, nil)
	if err != nil {
		return err
	}
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.ContentMediaReady, ready.MediaId, ready, normalize.Headers(ready)...); err != nil {
		return err
	}
	return b.Wait(ctx)
}

// Delete removes a ready object, marks its row and records the deletion on
// content.media.deleted.v1. The media row itself is kept.
func (j *Janitor) Delete(ctx context.Context, mediaID string, reason mediav1.DeleteReason) error {
	var bucket, key *string
	err := j.DB.QueryRow(ctx, `SELECT s3_bucket, s3_key FROM media WHERE media_id = $1 AND status = 'ready'`, mediaID).Scan(&bucket, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if key != nil {
		if err := j.S3.RemoveObject(ctx, str(bucket), *key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	status, label := "deleted", "acked"
	if reason == mediav1.DeleteReason_DELETE_REASON_EXPIRED {
		status, label = "expired", "ttl_expired"
	}
	if _, err := j.DB.Exec(ctx, `UPDATE media SET status = $2, deleted_at = now(),
		processed_at = CASE WHEN $2 = 'deleted' THEN now() ELSE processed_at END WHERE media_id = $1`, mediaID, status); err != nil {
		return err
	}
	metrics.MediaDeleted.WithLabelValues(label).Inc()
	ev := deletedEvent(mediaID, reason, time.Now())
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.ContentMediaDeleted, ev.MediaId, ev, normalize.Headers(ev)...); err != nil {
		return err
	}
	return b.Wait(ctx)
}

// Sweep deletes ready objects past delete_after (no ack in time).
func (j *Janitor) Sweep(ctx context.Context) error {
	for {
		rows, err := j.DB.Query(ctx, `SELECT media_id::text FROM media WHERE status = 'ready' AND delete_after < now() LIMIT 500`)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := j.Delete(ctx, id, mediav1.DeleteReason_DELETE_REASON_EXPIRED); err != nil {
				return err
			}
		}
		if len(ids) < 500 {
			return nil
		}
	}
}

// Orphans deletes stored objects no ready media row references, older than
// a file's lifetime plus an hour, under igs's prefix only.
func (j *Janitor) Orphans(ctx context.Context) error {
	if j.Prefix == "" {
		// The whole bucket: other services' objects too. Never sweep it.
		return errors.New("igs janitor: no media prefix: the orphan sweep would list the whole bucket")
	}
	cutoff := time.Now().Add(-Lifetime(j.TTL) - time.Hour)
	for obj := range j.S3.ListObjects(ctx, j.Bucket, minio.ListObjectsOptions{Prefix: j.Prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.LastModified.After(cutoff) {
			continue
		}
		base := obj.Key[strings.LastIndexByte(obj.Key, '/')+1:]
		id := strings.SplitN(base, ".", 2)[0]
		var ready bool
		_ = j.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM media WHERE media_id::text = $1 AND status = 'ready')`, id).Scan(&ready)
		if ready {
			continue
		}
		if err := j.S3.RemoveObject(ctx, j.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		metrics.MediaDeleted.WithLabelValues("orphan").Inc()
		slog.Info("igs janitor: orphan removed", "key", obj.Key)
	}
	return nil
}

func str(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func val(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// Lifecycle sets the bucket's expiry rule for igs's objects (under
// prefix), a safety net behind the janitor: objects expire a day after
// their lifetime. It replaces the bucket's whole lifecycle configuration, so it is
// only for a bucket igs owns (s3.manage_buckets).
func Lifecycle(ctx context.Context, s3 *minio.Client, bucket, prefix string, ttl time.Duration) {
	if prefix == "" {
		return
	}
	ttl = Lifetime(ttl)
	cfg := &lifecycle.Configuration{Rules: []lifecycle.Rule{{
		ID: "igs-ig-media-ttl", Status: "Enabled", RuleFilter: lifecycle.Filter{Prefix: prefix},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(int(ttl.Hours()/24) + 1)},
	}}}
	if err := s3.SetBucketLifecycle(ctx, bucket, cfg); err != nil {
		slog.Warn("igs media: bucket lifecycle not applied (the store may not support it)", "bucket", bucket, "err", err)
	}
}
