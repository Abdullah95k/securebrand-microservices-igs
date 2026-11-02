// Command fbs-persister writes the public fb.* entity topics into Postgres.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
)

func main() {
	fs := flag.NewFlagSet("fbs-persister", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-persister", cfg.Log.Level, cfg.Log.Format)
	ctx := context.Background()
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	kcfg, err := app.KafkaClient(cfg, "-persister")
	if err != nil {
		slog.Error("kafka", "err", err)
		os.Exit(1)
	}
	dlq, err := kafka.NewProducer(kcfg)
	if err != nil {
		slog.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer dlq.Close()
	c, err := persist.NewConsumer(kcfg, &persist.Writer{DB: db}, dlq, 2000, 250*time.Millisecond)
	if err != nil {
		slog.Error("kafka", "err", err)
		os.Exit(1)
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	if err := run.Group(ctx, ops.Run, c.Run); err != nil {
		slog.Error("fbs-persister exited", "err", err)
		os.Exit(1)
	}
}
