package app

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
)

// MediaRoles are igs-media's roles.
var MediaRoles = []string{"downloader", "janitor"}

// NewMedia builds igs's media roles: the downloader (igs.task.media into
// object storage, ig.media.ready.v1) and the janitor (acks on
// ig.media.processed.v1, the TTL sweep, orphans). db is a pool on schema ig.
func NewMedia(cfg *config.Config, db *pgxpool.Pool, s3 *minio.Client, producer *kafka.Producer, roles []string) ([]run.Func, error) {
	kcfg, err := clients.Kafka(cfg, "-igs-media")
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		roles = MediaRoles
	}
	var comps []run.Func
	for _, r := range roles {
		switch r {
		case "downloader":
			d := &media.Downloader{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer, HTTP: media.NewHTTPClient(),
				TTL: cfg.Media.TTL, MaxBytes: cfg.Media.MaxBytes, Presign: cfg.Media.Presign, PresignTTL: cfg.Media.PresignTTL,
				Prefix: cfg.S3.MediaPrefix}
			c, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-ig-media-downloader",
				Topics: []string{topics.TaskMedia}, MaxInflight: max(cfg.Media.Concurrency, 1), DrainTimeout: 60 * time.Second}, d.Handle)
			if err != nil {
				return nil, err
			}
			comps = append(comps, c.Run)
		case "janitor":
			j := &media.Janitor{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer,
				RequiredConsumers: cfg.Media.RequiredConsumers, MaxRetryableFailures: cfg.Media.MaxRetryableFailures, TTL: cfg.Media.TTL,
				Prefix: cfg.S3.MediaPrefix}
			c, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "igs-ig-media-janitor",
				Topics: []string{topics.MediaProcessed}, MaxInflight: 32, DrainTimeout: 20 * time.Second}, j.Handle)
			if err != nil {
				return nil, err
			}
			comps = append(comps, c.Run,
				func(ctx context.Context) error {
					return scheduler.Every(ctx, "media-sweep", cfg.Media.SweepInterval, j.Sweep)
				},
				func(ctx context.Context) error {
					return scheduler.Every(ctx, "media-orphans", cfg.Media.OrphanInterval, j.Orphans)
				})
		default:
			return nil, fmt.Errorf("unknown igs-media role %q (want %v)", r, MediaRoles)
		}
	}
	// Only on a bucket igs owns: the rule replaces the bucket's whole
	// lifecycle, which on the platform's shared bucket is not igs's to set.
	if slices.Contains(roles, "downloader") && cfg.S3.ManageBuckets {
		media.Lifecycle(context.Background(), s3, cfg.S3.MediaBucket, cfg.S3.MediaPrefix, cfg.Media.TTL)
	}
	return comps, nil
}
