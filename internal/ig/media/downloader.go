// Package media is igs's temporary media pipeline (D5: used only when a job
// asks for files): download from Instagram's CDN into object storage,
// announce on ig.media.ready.v1, and delete once every required consumer
// acknowledged on ig.media.processed.v1 (or the TTL passed). It mirrors the
// copied Facebook pipeline on schema ig and the common media contracts.
package media

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Downloader consumes igs.task.media. DB is a pool on schema ig.
type Downloader struct {
	DB         *pgxpool.Pool
	S3         *minio.Client
	Bucket     string
	Producer   *kafka.Producer
	HTTP       *http.Client // direct egress, never the proxies
	TTL        time.Duration
	MaxBytes   int64
	Presign    bool
	PresignTTL time.Duration
}

// NewHTTPClient returns a client for CDN downloads.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Minute,
		Transport: &http.Transport{
			Proxy:               nil, // media is fetched over datacenter egress
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// ObjectKey is the deterministic storage key of a media item.
func ObjectKey(m *igv1.Media, contentType string) string {
	ext := ".bin"
	if exts, _ := mime.ExtensionsByType(contentType); len(exts) > 0 {
		ext = exts[0]
		if contentType == "image/jpeg" {
			ext = ".jpg"
		}
	} else if e := path.Ext(strings.SplitN(m.SourceUrl, "?", 2)[0]); len(e) > 1 && len(e) <= 5 {
		ext = e
	}
	return "ig/" + m.MediaId[:2] + "/" + m.MediaId + ext
}

func kindName(k commonv1.MediaKind) string {
	return strings.ToLower(strings.TrimPrefix(k.String(), "MEDIA_KIND_"))
}

var errExpired = errors.New("source url expired")

type permanentError struct{ error }

// Handle implements kafka.Handler.
func (d *Downloader) Handle(ctx context.Context, rec *kgo.Record) error {
	var t igstaskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		slog.Error("igs media: undecodable task", "err", err)
		return nil
	}
	m := t.GetMedia().GetMedia()
	if m == nil || len(m.MediaId) < 2 {
		return nil
	}
	kind := kindName(m.Kind)
	var status string
	err := d.DB.QueryRow(ctx, `SELECT status FROM media WHERE media_id = $1`, m.MediaId).Scan(&status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	switch status {
	case "ready", "deleted", "expired":
		// Handed to the analysis service once already: a re-scrape does
		// not download the same media again.
		metrics.MediaOutcome.WithLabelValues(kind, "skipped").Inc()
		return nil
	}
	if exp := m.SourceExpiresAt; exp != nil && exp.AsTime().Before(time.Now()) {
		return d.markFailed(ctx, m, "expired", "source url expired before download")
	}
	start := time.Now()
	obj, err := d.download(ctx, m)
	switch {
	case errors.Is(err, errExpired):
		metrics.MediaOutcome.WithLabelValues(kind, "expired").Inc()
		return d.markFailed(ctx, m, "expired", err.Error())
	case err != nil:
		var perm permanentError
		if errors.As(err, &perm) {
			metrics.MediaOutcome.WithLabelValues(kind, "failed").Inc()
			return d.markFailed(ctx, m, "failed", err.Error())
		}
		metrics.MediaOutcome.WithLabelValues(kind, "retry").Inc()
		return err // the consumer retries with backoff
	}
	now := time.Now().UTC()
	deleteAfter := now.Add(d.TTL)
	_, err = d.DB.Exec(ctx, `
		INSERT INTO media (media_id, source_media_id, variant, kind, owner_type, user_pk, media_pk, parent_media_pk, position,
			comment_pk, source_url, width, height, duration_ms, mime, alt_text, status, s3_bucket, s3_key, size_bytes, sha256,
			attempts, delete_after, stored_at, ready_at)
		VALUES ($1,$2,$3,$4,$5,NULLIF($6::bigint,0),NULLIF($7::bigint,0),$8,$9,$10,$11,NULLIF($12::int,0),NULLIF($13::int,0),NULLIF($14::bigint,0),$15,NULLIF($16,''),
			'ready',$17,$18,$19,$20,1,$21,$22,$22)
		ON CONFLICT (media_id) DO UPDATE SET status = 'ready', s3_bucket = EXCLUDED.s3_bucket, s3_key = EXCLUDED.s3_key,
			size_bytes = EXCLUDED.size_bytes, sha256 = EXCLUDED.sha256, mime = EXCLUDED.mime, attempts = media.attempts + 1,
			delete_after = EXCLUDED.delete_after, stored_at = EXCLUDED.stored_at, ready_at = EXCLUDED.ready_at, last_error = NULL`,
		m.MediaId, m.SourceMediaId, m.Variant, kind, m.OwnerType, m.UserPk, m.MediaPk, m.ParentMediaPk, m.Position, m.CommentPk,
		m.SourceUrl, m.Width, m.Height, m.DurationMs, obj.ContentType, m.AltText, obj.Bucket, obj.Key, obj.Size, obj.Sha256,
		deleteAfter, now)
	if err != nil {
		return err
	}
	ready := &commonv1.MediaReady{
		EventId: m.MediaId + ":ready:1", Platform: commonv1.Platform_PLATFORM_INSTAGRAM, MediaId: m.MediaId,
		SourceMediaId: m.SourceMediaId, Kind: m.Kind, Variant: m.Variant, AccountId: m.UserPk, PostId: m.MediaPk,
		CommentId: m.CommentPk, Object: obj, Width: m.Width, Height: m.Height, DurationMs: m.DurationMs, AltText: m.AltText,
		DiscoveredAt: m.GetObs().GetObservedAt(), StoredAt: timestamppb.New(now), DeleteAfter: timestamppb.New(deleteAfter), Attempt: 1,
	}
	if m.ParentMediaPk != nil {
		ready.PostId = *m.ParentMediaPk // a carousel child belongs to its post
	}
	if t.JobId != "" {
		ready.JobIds = []string{t.JobId}
	}
	if d.Presign {
		if u, err := d.S3.PresignedGetObject(ctx, obj.Bucket, obj.Key, d.PresignTTL, nil); err == nil {
			ready.PresignedUrl = u.String()
		}
	}
	b := d.Producer.NewBatch()
	if err := b.AddProto(ctx, topics.MediaReady, m.MediaId, ready); err != nil {
		return err
	}
	if err := b.Wait(ctx); err != nil {
		return err
	}
	metrics.MediaOutcome.WithLabelValues(kind, "ready").Inc()
	if at := m.GetObs().GetObservedAt(); at != nil {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(time.Since(at.AsTime()).Seconds())
	} else {
		metrics.MediaDiscoveryToStored.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	}
	return nil
}

func (d *Downloader) download(ctx context.Context, m *igv1.Media) (*commonv1.S3Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.SourceUrl, nil)
	if err != nil {
		return nil, permanentError{err}
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,video/*,*/*;q=0.8")
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone:
		return nil, errExpired
	case resp.StatusCode == http.StatusNotFound:
		return nil, permanentError{fmt.Errorf("http %d", resp.StatusCode)}
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	if d.MaxBytes > 0 && resp.ContentLength > d.MaxBytes {
		return nil, permanentError{fmt.Errorf("object too large: %d bytes", resp.ContentLength)}
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	ct = strings.TrimSpace(strings.SplitN(ct, ";", 2)[0])
	key := ObjectKey(m, ct)
	h := sha256.New()
	var src io.Reader = resp.Body
	if d.MaxBytes > 0 {
		src = io.LimitReader(resp.Body, d.MaxBytes+1)
	}
	info, err := d.S3.PutObject(ctx, d.Bucket, key, io.TeeReader(src, h), resp.ContentLength, minio.PutObjectOptions{
		ContentType: ct, PartSize: 8 << 20,
		UserMetadata: map[string]string{"media-id": m.MediaId, "source-media-id": m.SourceMediaId},
	})
	if err != nil {
		return nil, fmt.Errorf("s3 put: %w", err)
	}
	if d.MaxBytes > 0 && info.Size > d.MaxBytes {
		_ = d.S3.RemoveObject(ctx, d.Bucket, key, minio.RemoveObjectOptions{})
		return nil, permanentError{fmt.Errorf("object too large: %d bytes", info.Size)}
	}
	return &commonv1.S3Object{Bucket: d.Bucket, Key: key, Size: info.Size, Sha256: h.Sum(nil), ContentType: ct}, nil
}

func (d *Downloader) markFailed(ctx context.Context, m *igv1.Media, status, reason string) error {
	_, err := d.DB.Exec(ctx, `
		INSERT INTO media (media_id, source_media_id, variant, kind, owner_type, user_pk, media_pk, source_url, status, attempts, last_error)
		VALUES ($1,$2,$3,$4,$5,NULLIF($6::bigint,0),NULLIF($7::bigint,0),$8,$9,1,$10)
		ON CONFLICT (media_id) DO UPDATE SET status = EXCLUDED.status, attempts = media.attempts + 1, last_error = EXCLUDED.last_error
		WHERE media.status NOT IN ('ready','deleted')`,
		m.MediaId, m.SourceMediaId, m.Variant, kindName(m.Kind), m.OwnerType, m.UserPk, m.MediaPk, m.SourceUrl, status, reason)
	return err
}
