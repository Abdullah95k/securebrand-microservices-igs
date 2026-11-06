package app

import (
	"context"

	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
)

// OpenS3 opens the object store for buckets: creating any that is missing
// when igs manages its buckets (s3.manage_buckets), else only checking
// they exist (the platform's shared buckets).
func OpenS3(ctx context.Context, cfg *config.Config, buckets ...string) (*minio.Client, error) {
	s := cfg.S3
	if s.ManageBuckets {
		return s3x.Open(ctx, s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL, buckets...)
	}
	return s3x.OpenExisting(ctx, s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL, buckets...)
}
