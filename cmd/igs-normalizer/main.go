// Command igs-normalizer maps igs's ig.* entity topics to the shared
// content.* contract (architecture §5) that storage, analysis and alerts
// read.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igapp "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
)

func main() {
	fs := flag.NewFlagSet("igs-normalizer", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-normalizer", cfg.Log.Level, cfg.Log.Format)
	if err := runNormalizer(context.Background(), cfg); err != nil {
		slog.Error("igs-normalizer exited", "err", err)
		os.Exit(1)
	}
}

func runNormalizer(ctx context.Context, cfg *config.Config) error {
	// The ops server first: opening the stores can take minutes.
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	stopOps, err := ops.Start()
	if err != nil {
		return err
	}
	defer stopOps()
	kcfg, err := clients.Kafka(cfg, "-igs-normalizer")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	c, err := igapp.NewNormalizer(cfg, producer)
	if err != nil {
		return err
	}
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, ops.Run, c.Run)
}
