// Command igs-registry-sync follows Instagram's doc_id rotations: every
// ig.registry_sync.interval it scans the canary profiles' pages and script
// bundles for the operations' doc_ids, checks them on a canary, and
// promotes a working one when the active doc_id went stale. --once runs
// once, prints the report and exits (3 when degraded).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igapp "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

func main() {
	fs := flag.NewFlagSet("igs-registry-sync", flag.ExitOnError)
	once := fs.Bool("once", false, "run once, print the report and exit")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-registry-sync", cfg.Log.Level, cfg.Log.Format)
	code, err := runSync(context.Background(), cfg, *once)
	if err != nil {
		slog.Error("igs-registry-sync exited", "err", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runSync(ctx context.Context, cfg *config.Config, once bool) (int, error) {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns, store.IGSchema)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	rdb, err := clients.Redis(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rdb.Close() }()
	s, reg, rests, err := igapp.NewRegistrySync(ctx, cfg, db, rdb, nil)
	if err != nil {
		return 0, err
	}
	if err := s.InitMetrics(ctx); err != nil {
		slog.Warn("igs registry-sync: last run unknown", "err", err)
	}
	rc := cfg.IG.RegistrySync
	sync := func(ctx context.Context) (registrysync.Report, error) {
		ctx, cancel := context.WithTimeout(ctx, rc.Timeout)
		defer cancel()
		if err := reg.Reload(ctx); err != nil {
			return registrysync.Report{}, err
		}
		return s.Run(ctx)
	}
	if once {
		rep, err := sync(ctx)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		switch {
		case err != nil:
			return 0, err
		case rep.Status == registrysync.StatusDegraded:
			return 3, nil
		}
		return 0, nil
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	return 0, run.Group(ctx, ops.Run, rests, func(ctx context.Context) error {
		return scheduler.Every(ctx, "registry-sync", rc.Interval, func(ctx context.Context) error {
			_, err := sync(ctx)
			return err
		})
	})
}
