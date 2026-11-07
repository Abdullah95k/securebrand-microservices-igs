// Command igs-content-writer fills the shared content store (ClickHouse,
// architecture §6) from the content.* topics. It knows no platform: every
// collector's normalized events land in the same tables.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/content/clickhouse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
)

func main() {
	fs := flag.NewFlagSet("igs-content-writer", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-content-writer", cfg.Log.Level, cfg.Log.Format)
	if err := runWriter(context.Background(), cfg); err != nil {
		slog.Error("igs-content-writer exited", "err", err)
		os.Exit(1)
	}
}

func runWriter(ctx context.Context, cfg *config.Config) error {
	// The ops server first: opening the stores can take minutes.
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	stopOps, err := ops.Start()
	if err != nil {
		return err
	}
	defer stopOps()
	if len(cfg.ClickHouse.Addr) == 0 {
		return errors.New("clickhouse.addr is not set")
	}
	conn, err := clickhouse.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	kcfg, err := clients.Kafka(cfg, "-igs-content-writer")
	if err != nil {
		return err
	}
	c, err := clickhouse.NewConsumer(kcfg, clickhouse.Conn{C: conn}, cfg.ClickHouse.BatchSize, cfg.ClickHouse.Linger)
	if err != nil {
		return err
	}
	ops.AddCheck("clickhouse", conn.Ping)
	return run.Group(ctx, ops.Run, c.Run)
}
