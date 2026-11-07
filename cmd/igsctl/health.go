package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/health"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// errUnhealthy makes igsctl exit 1 after the report when a check is BAD.
var errUnhealthy = errors.New("a check is BAD (above)")

// healthCmd prints igs's state in one read-only pass (REQ-008 fix 19):
// profiles on the schedule, how late it is, the last hour's jobs, the last
// data stored, the exits resting and the consumers' lag. It exits 1 when a
// check is BAD. Valkey or Kafka that do not answer give "?" lines, not a
// failure, so the report still shows the rest.
func healthCmd(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	src := health.Sources{DB: db, Cfg: cfg, Prefix: cfg.Kafka.TopicPrefix}
	if rdb, err := clients.Redis(ctx, cfg); err == nil {
		defer func() { _ = rdb.Close() }()
		src.Redis = rdb
	} else {
		fmt.Fprintln(os.Stderr, "igsctl health: Valkey:", err)
	}
	if kcfg, err := clients.Kafka(cfg, "-igsctl"); err == nil {
		if p, err := kafka.NewProducer(kcfg); err == nil {
			defer p.Close()
			src.Admin = kadm.NewClient(p.Client())
		} else {
			fmt.Fprintln(os.Stderr, "igsctl health: Kafka:", err)
		}
	}
	checks := health.Gather(ctx, src)
	if health.Print(os.Stdout, checks, time.Now()) {
		return errUnhealthy
	}
	return nil
}
