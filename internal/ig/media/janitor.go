package media

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
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
}

// Handle consumes ig.media.processed.v1.
func (j *Janitor) Handle(ctx context.Context, rec *kgo.Record) error {
	var ack commonv1.MediaProcessed
	if err := proto.Unmarshal(rec.Value, &ack); err != nil {
		slog.Error("igs janitor: undecodable ack", "err", err)
		return nil
	}
	if ack.MediaId == "" || ack.Consumer == "" {
		return nil
	}
	status := strings.ToLower(strings.TrimPrefix(ack.Status.String(), "PROCESS_STATUS_"))
	at := time.Now().UTC()
	if ack.ProcessedAt != nil {
		at = ack.ProcessedAt.AsTime()
	}
	tag, err := j.DB.Exec(ctx, `INSERT INTO media_acks (media_id, consumer, status, error, acked_at)
		SELECT $1::uuid, $2, $3, NULLIF($4,''), $5 WHERE EXISTS (SELECT 1 FROM media WHERE media_id = $1::uuid)
		ON CONFLICT (media_id, consumer) DO UPDATE SET status = EXCLUDED.status, error = EXCLUDED.error, acked_at = EXCLUDED.acked_at`,
		ack.MediaId, ack.Consumer, status, ack.Error, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // unknown media (already purged)
	}
	if ack.Status == commonv1.ProcessStatus_PROCESS_STATUS_FAILED_RETRYABLE {
		return j.retry(ctx, ack.MediaId)
	}
	return j.deleteIfAcked(ctx, ack.MediaId)
}

// deleteIfAcked deletes the object when all required consumers are done.
func (j *Janitor) deleteIfAcked(ctx context.Context, mediaID string) error {
	var done int
	if err := j.DB.QueryRow(ctx, `SELECT count(DISTINCT consumer) FROM media_acks WHERE media_id = $1
		AND consumer = ANY($2) AND status IN ('processed','failed_permanent')`, mediaID, j.RequiredConsumers).Scan(&done); err != nil {
		return err
	}
	if done < len(j.RequiredConsumers) {
		return nil
	}
	return j.Delete(ctx, mediaID, commonv1.DeleteReason_DELETE_REASON_ACKED)
}

// retry announces the object again after a retryable failure, until
// MaxRetryableFailures.
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
	if failures > j.MaxRetryableFailures {
		return j.Delete(ctx, mediaID, commonv1.DeleteReason_DELETE_REASON_ACKED)
	}
	ready, err := j.readyEvent(ctx, mediaID, uint32(failures+1))
	if err != nil {
		return err
	}
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaReady, mediaID, ready); err != nil {
		return err
	}
	return b.Wait(ctx)
}

func (j *Janitor) readyEvent(ctx context.Context, mediaID string, attempt uint32) (*commonv1.MediaReady, error) {
	var kind, variant, src string
	var user, post, parent, comment, size, dur *int64
	var w, h *int32
	var sha []byte
	var bucket, key, ct, alt *string
	var deleteAfter *time.Time
	err := j.DB.QueryRow(ctx, `SELECT source_media_id, kind, variant, user_pk, media_pk, parent_media_pk, comment_pk, width, height,
		duration_ms, sha256, s3_bucket, s3_key, size_bytes, mime, alt_text, delete_after FROM media WHERE media_id = $1`, mediaID).
		Scan(&src, &kind, &variant, &user, &post, &parent, &comment, &w, &h, &dur, &sha, &bucket, &key, &size, &ct, &alt, &deleteAfter)
	if err != nil {
		return nil, err
	}
	r := &commonv1.MediaReady{EventId: mediaID + ":ready:" + strconv.Itoa(int(attempt)), Platform: commonv1.Platform_PLATFORM_INSTAGRAM,
		MediaId: mediaID, SourceMediaId: src, Variant: variant, Attempt: attempt,
		Kind:      commonv1.MediaKind(commonv1.MediaKind_value["MEDIA_KIND_"+strings.ToUpper(kind)]),
		AccountId: val(user), PostId: val(post), CommentId: comment, Width: val32(w), Height: val32(h), DurationMs: val(dur),
		AltText: str(alt), Object: &commonv1.S3Object{Bucket: str(bucket), Key: str(key), Size: val(size), Sha256: sha, ContentType: str(ct)}}
	if parent != nil {
		r.PostId = *parent
	}
	if deleteAfter != nil {
		r.DeleteAfter = timestamppb.New(*deleteAfter)
	}
	return r, nil
}

// Delete removes a ready object, marks its row and publishes
// ig.media.deleted.v1.
func (j *Janitor) Delete(ctx context.Context, mediaID string, reason commonv1.DeleteReason) error {
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
	status := "deleted"
	if reason == commonv1.DeleteReason_DELETE_REASON_TTL_EXPIRED {
		status = "expired"
	}
	if _, err := j.DB.Exec(ctx, `UPDATE media SET status = $2, deleted_at = now(),
		processed_at = CASE WHEN $2 = 'deleted' THEN now() ELSE processed_at END WHERE media_id = $1`, mediaID, status); err != nil {
		return err
	}
	name := strings.ToLower(strings.TrimPrefix(reason.String(), "DELETE_REASON_"))
	metrics.MediaDeleted.WithLabelValues(name).Inc()
	b := j.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaDeleted, mediaID, &commonv1.MediaDeleted{EventId: mediaID + ":" + name, MediaId: mediaID,
		Object: &commonv1.S3Object{Bucket: str(bucket), Key: str(key)}, Reason: reason, DeletedAt: timestamppb.Now(),
		Platform: commonv1.Platform_PLATFORM_INSTAGRAM}); err != nil {
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
			if err := j.Delete(ctx, id, commonv1.DeleteReason_DELETE_REASON_TTL_EXPIRED); err != nil {
				return err
			}
		}
		if len(ids) < 500 {
			return nil
		}
	}
}

// Orphans deletes stored objects no ready media row references, older than
// the TTL (at least a day) plus an hour.
func (j *Janitor) Orphans(ctx context.Context) error {
	cutoff := time.Now().Add(-max(j.TTL, 24*time.Hour) - time.Hour)
	for obj := range j.S3.ListObjects(ctx, j.Bucket, minio.ListObjectsOptions{Prefix: "ig/", Recursive: true}) {
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

func val32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

// Lifecycle sets the bucket's expiry rule for igs's objects (prefix ig/),
// a safety net behind the janitor: objects expire a day after the TTL.
func Lifecycle(ctx context.Context, s3 *minio.Client, bucket string, ttl time.Duration) {
	if ttl <= 0 {
		// Kept until analysed: no expiry rule.
		return
	}
	cfg := &lifecycle.Configuration{Rules: []lifecycle.Rule{{
		ID: "igs-ig-media-ttl", Status: "Enabled", RuleFilter: lifecycle.Filter{Prefix: "ig/"},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(int(ttl.Hours()/24) + 1)},
	}}}
	if err := s3.SetBucketLifecycle(ctx, bucket, cfg); err != nil {
		slog.Warn("igs media: bucket lifecycle not applied (the store may not support it)", "bucket", bucket, "err", err)
	}
}
