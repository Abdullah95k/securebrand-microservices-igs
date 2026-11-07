// Command igs-media runs igs's temporary media pipeline over schema ig:
// the downloader (igs.task.media into object storage, ig.media.ready.v1)
// and the janitor (acks on ig.media.processed.v1, the TTL sweep, orphan
// objects). Media files are downloaded only for jobs that ask for them
// (ig.limits.include_media, D5). --roles runs one of them per Deployment.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igapp "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

func main() {
	fs := flag.NewFlagSet("igs-media", flag.ExitOnError)
	rolesFlag := fs.String("roles", "downloader,janitor", "comma separated roles: downloader, janitor")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-media", cfg.Log.Level, cfg.Log.Format)
	if err := runMedia(context.Background(), cfg, strings.Split(*rolesFlag, ",")); err != nil {
		slog.Error("igs-media exited", "err", err)
		os.Exit(1)
	}
}

func runMedia(ctx context.Context, cfg *config.Config, roles []string) error {
	// The ops server first: opening the stores can take minutes.
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	stopOps, err := ops.Start()
	if err != nil {
		return err
	}
	defer stopOps()
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
	if err != nil {
		return err
	}
	defer db.Close()
	s3, err := igapp.OpenS3(ctx, cfg, cfg.S3.MediaBucket)
	if err != nil {
		return err
	}
	kcfg, err := clients.Kafka(cfg, "-igs-media")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	comps, err := igapp.NewMedia(cfg, db, s3, producer, roles)
	if err != nil {
		return err
	}
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, append([]run.Func{ops.Run}, comps...)...)
}
