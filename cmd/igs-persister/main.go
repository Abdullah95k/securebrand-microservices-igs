// Command igs-persister writes the public ig.* entity topics into schema ig.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

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
	fs := flag.NewFlagSet("igs-persister", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-persister", cfg.Log.Level, cfg.Log.Format)
	if err := runPersister(context.Background(), cfg); err != nil {
		slog.Error("igs-persister exited", "err", err)
		os.Exit(1)
	}
}

func runPersister(ctx context.Context, cfg *config.Config) error {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns, store.IGSchema)
	if err != nil {
		return err
	}
	defer db.Close()
	kcfg, err := clients.Kafka(cfg, "-igs-persister")
	if err != nil {
		return err
	}
	dlq, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer dlq.Close()
	c, err := igapp.NewPersister(cfg, db, dlq, 2000, 250*time.Millisecond)
	if err != nil {
		return err
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	return run.Group(ctx, ops.Run, c.Run)
}
