// Command igs-scheduler runs igs's leader-elected control loops over schema
// ig: the planner (scheduled runs every ig.schedule.interval, the profile
// itself every ig.schedule.profile_refresh, comment refreshes of recent
// posts), the job finalizer, the reaper, partition maintenance, AIMD and
// consumer-lag gauges, and the erasure requests.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	iggdpr "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/leader"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// leaderKey is igs's scheduler lock, apart from the copied Facebook
// scheduler's 4_300_100.
const leaderKey = 4_300_200

func main() {
	fs := flag.NewFlagSet("igs-scheduler", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("igs-scheduler", cfg.Log.Level, cfg.Log.Format)
	if err := runScheduler(context.Background(), cfg); err != nil {
		slog.Error("igs-scheduler exited", "err", err)
		os.Exit(1)
	}
}

func runScheduler(ctx context.Context, cfg *config.Config) error {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns, store.IGSchema)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := clients.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := clients.Kafka(cfg, "-igs-scheduler")
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	tracker := jobs.New(rdb)
	disp := &dispatch.Dispatcher{DB: db, Jobs: tracker, Producer: producer}
	planner := &scheduler.Planner{DB: db, Dispatch: disp, Cfg: cfg, Redis: rdb}
	fin := &scheduler.Finalizer{DB: db, Jobs: tracker, Producer: producer, Cfg: cfg, Timeout: cfg.Scheduler.FinalizeTimeout}
	reaper := &scheduler.Reaper{DB: db, Jobs: tracker, After: cfg.Scheduler.ReaperAfter}
	maint := &scheduler.Maintenance{DB: db, Cfg: cfg}
	tierer := &scheduler.Tierer{DB: db, Cfg: cfg}
	lag := &scheduler.LagGauges{Admin: kadm.NewClient(producer.Client()), Prefix: cfg.Kafka.TopicPrefix}
	eraser := &iggdpr.Eraser{DB: db, Producer: producer}
	if cfg.S3.Endpoint != "" {
		if eraser.S3, err = s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL, cfg.S3.MediaBucket); err != nil {
			return err
		}
	}
	loops := []run.Func{
		func(ctx context.Context) error { return scheduler.Every(ctx, "maintenance", time.Hour, maint.Tick) },
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "planner", cfg.Scheduler.PlannerInterval, planner.Tick)
		},
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "finalizer", cfg.Scheduler.FinalizerInterval, fin.Tick)
		},
		func(ctx context.Context) error { return scheduler.Every(ctx, "reaper", time.Minute, reaper.Tick) },
		func(ctx context.Context) error { return scheduler.Every(ctx, "tiers", 5*time.Minute, tierer.Tick) },
		func(ctx context.Context) error { return scheduler.Every(ctx, "consumer-lag", 30*time.Second, lag.Tick) },
		func(ctx context.Context) error { return scheduler.Every(ctx, "erasure", 10*time.Second, eraser.Tick) },
	}
	if cfg.IG.AIMD.Enabled {
		aimd := ratelimit.NewController(rdb, cfg.Kafka.TopicPrefix, cfg.IG.AIMD, cfg.IG.GlobalRPS)
		loops = append(loops, func(ctx context.Context) error { return scheduler.Every(ctx, "aimd", cfg.IG.AIMD.Interval, aimd.Tick) })
	}
	if cfg.IG.Discovery.Enabled {
		vetter := &discovery.Vetter{DB: db, Dispatch: disp, Cfg: cfg}
		loops = append(loops, func(ctx context.Context) error {
			return scheduler.Every(ctx, "discovery", cfg.IG.Discovery.Interval, vetter.Tick)
		}, func(ctx context.Context) error {
			return keywords.Watch(ctx, db, discovery.KeywordsRefresh, discovery.UseKeywords)
		})
	}
	if cfg.IG.BudgetOn() {
		budget := ratelimit.NewBudgetController(rdb, cfg.Kafka.TopicPrefix, cfg.IG.Budget)
		loops = append(loops, func(ctx context.Context) error {
			return scheduler.Every(ctx, "budget", cfg.IG.Budget.Interval, budget.Tick)
		})
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return leader.Run(ctx, db, leaderKey, func(ctx context.Context) error {
			metrics.SchedulerLeader.Set(1)
			defer metrics.SchedulerLeader.Set(0)
			return run.Group(ctx, loops...)
		})
	})
}
