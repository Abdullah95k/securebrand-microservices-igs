// Command igs-soak drives igs's soak against fakeig (REQ-008 fix 12;
// deploy/soak, docs/runbooks/igs-soak.md). It registers every fakeig
// profile through igs's API and keeps them on a short schedule while
// fakeig's world changes, kills a crawler pod every --kill-every and
// restarts Valkey once (--chaos, through the Kubernetes API as its service
// account). Then it stops the schedules, reads every profile in full and
// compares igs's store with fakeig's ground truth. Its last log lines are
// the report; it exits 0 on PASS, 1 on FAIL and 2 when the soak could not
// run. It reads igs's config (the store's DSN) like any igs binary, and
// never reaches Instagram.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/soak"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

func main() {
	fs := flag.NewFlagSet("igs-soak", flag.ExitOnError)
	apiURL := fs.String("api", "http://igs-api", "igs-api's base URL")
	fakeURL := fs.String("fakeig", "http://fakeig:8091", "fakeig's base URL (fakeig -admin)")
	duration := fs.Duration("duration", 23*time.Hour, "the driving phase: schedules, world changes and chaos")
	interval := fs.Duration("interval", 30*time.Minute, "every profile's scheduled interval during the soak")
	mutateEvery := fs.Duration("mutate-every", 2*time.Second, "one change of fakeig's world this often")
	sampleEvery := fs.Duration("sample-every", time.Minute, "log the jobs this often")
	chaos := fs.Bool("chaos", false, "kill crawler pods and restart Valkey through the Kubernetes API (in a pod)")
	killEvery := fs.Duration("kill-every", 2*time.Hour, "kill a crawler pod this often (with --chaos)")
	valkeyAt := fs.Duration("valkey-at", 12*time.Hour, "restart Valkey once, this far in (with --chaos)")
	crawlers := fs.String("crawlers", "app.kubernetes.io/name=igs,app.kubernetes.io/component in (crawler-profile,crawler-post)",
		"label selector of the crawler pods")
	valkey := fs.String("valkey", "valkey", "Valkey's StatefulSet")
	settle := fs.Duration("settle", 30*time.Minute, "how long running jobs may take once the schedules stop")
	finalWait := fs.Duration("final-wait", 2*time.Hour, "how long the final full reads may take")
	wait := fs.Duration("wait", 30*time.Minute, "how long to wait at the start for igs (its schema, its API) and fakeig")
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	os.Exit(run(cfg, soak.Options{API: *apiURL, FakeIG: *fakeURL, Duration: *duration, Interval: *interval,
		MutateEvery: *mutateEvery, SampleEvery: *sampleEvery, KillEvery: *killEvery, ValkeyAt: *valkeyAt, Settle: *settle,
		FinalWait: *finalWait, MaxAge: time.Duration(cfg.IG.Limits.MaxAgeDays) * 24 * time.Hour, Log: os.Stdout},
		*chaos, *crawlers, *valkey, *wait))
}

func run(cfg *config.Config, o soak.Options, chaos bool, crawlers, valkey string, wait time.Duration) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// igs and fakeig may still be starting (they are deployed beside this
	// Job): wait for igs's schema, its API and fakeig.
	deadline := time.Now().Add(wait)
	var db *pgxpool.Pool
	var err error
	for {
		if db, err = pg.OpenSchema(ctx, cfg.Postgres.DSN, 4, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM)); err == nil {
			if err = soak.Ready(ctx, o.API, o.FakeIG); err == nil {
				break
			}
			db.Close()
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			fmt.Println("igs soak: cannot run: igs or fakeig not ready:", err)
			return 2
		}
		fmt.Println("igs soak: waiting for igs and fakeig:", err)
		time.Sleep(10 * time.Second)
	}
	defer db.Close()
	o.DB = db
	if chaos {
		k, err := soak.InCluster()
		if err != nil {
			fmt.Println("igs soak: cannot run:", err)
			return 2
		}
		k.Crawlers, k.Valkey = crawlers, valkey
		o.Chaos = k
	}
	rep, err := soak.Run(ctx, o)
	for _, l := range rep.Lines() {
		fmt.Println(l)
	}
	if err != nil {
		fmt.Println("igs soak: cannot run:", err)
		return 2
	}
	if !rep.Pass() {
		return 1
	}
	return 0
}
