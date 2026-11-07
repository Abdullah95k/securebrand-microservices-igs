package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
)

// OpenS3 opens the object store for buckets: creating any that is missing
// when igs manages its buckets (s3.manage_buckets), else only checking
// they exist (the platform's shared buckets).
// It fails without an access key: with the platform's credentials only
// the components that use the object store get one (S3_ACCESS_KEY and
// S3_SECRET_KEY, in platform-credentials or the component's credentials
// Secret).
func OpenS3(ctx context.Context, cfg *config.Config, buckets ...string) (*minio.Client, error) {
	s := cfg.S3
	if s.AccessKey == "" || s.SecretKey == "" {
		return nil, fmt.Errorf("s3: no access key for %s: set S3_ACCESS_KEY and S3_SECRET_KEY "+
			"(platform-credentials or this component's credentials Secret)", strings.Join(buckets, ", "))
	}
	if s.ManageBuckets {
		return s3x.Open(ctx, s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL, buckets...)
	}
	return s3x.OpenExisting(ctx, s.Endpoint, s.AccessKey, s.SecretKey, s.Region, s.UseSSL, buckets...)
}
