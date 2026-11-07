// Command igs-api serves igs's REST API (/v1) over schema ig.
//
// igs-api --create-client NAME --scopes jobs:write,profiles:write,data:read
// creates an API client, prints its key once and exits.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	igapi "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

func main() {
	fs := flag.NewFlagSet("igs-api", flag.ExitOnError)
	create := fs.String("create-client", "", "create an API client with this name, print its key and exit")
	scopes := fs.String("scopes", "jobs:write,profiles:write,data:read", "the new client's scopes (with --create-client)")
	rate := fs.Int("rate", 0, "the new client's requests per minute (0: api.default_rate_per_minute)")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-api", cfg.Log.Level, cfg.Log.Format)
	ctx := context.Background()
	var err error
	if *create != "" {
		err = createClient(ctx, cfg, *create, strings.Split(*scopes, ","), *rate)
	} else {
		err = serve(ctx, cfg)
	}
	if err != nil {
		slog.Error("igs-api exited", "err", err)
		os.Exit(1)
	}
}

func createClient(ctx context.Context, cfg *config.Config, name string, scopes []string, rate int) error {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, 2, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
	if err != nil {
		return err
	}
	defer db.Close()
	key, id, err := httpapi.CreateClient(ctx, db, name, scopes, rate)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"client_id": id, "name": name, "scopes": scopes, "api_key": key,
		"note": "store this key now; it cannot be shown again"})
}

func serve(ctx context.Context, cfg *config.Config) error {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
	if err != nil {
		return err
	}
	defer db.Close()
	read := db
	if cfg.Postgres.ReadDSN != "" {
		if read, err = pg.OpenSchema(ctx, cfg.Postgres.ReadDSN, cfg.Postgres.MaxConns, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled)); err != nil {
			return err
		}
		defer read.Close()
	}
	rdb, err := clients.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := clients.Kafka(cfg, "-igs-api")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	srv := &igapi.Server{Cfg: cfg, DB: db, ReadDB: read, Redis: rdb,
		Dispatch: &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: producer}}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return httpserver.Serve(ctx, cfg.HTTP.Addr, srv.Handler())
	})
}
