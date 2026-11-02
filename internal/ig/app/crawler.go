// Package app wires igs's services from config, as internal/app does for
// the copied Facebook code: binaries and the end-to-end tests share it, so
// tests exercise the real wiring.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/lanes"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/stage"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// DefaultStages are igs's crawler stages.
var DefaultStages = []string{"profile", "post", "retry"}

// Consumer groups. They differ from the copied Facebook crawler's, which
// shares the cluster in tests.
const (
	groupCrawler = "igs-ig-crawler-"
	groupRelay   = "igs-ig-retry-relay"
)

// Crawler is a configured igs crawler process.
type Crawler struct {
	Deps        *stage.Deps
	Worker      *stage.Worker
	Registry    *registry.DB
	Suppression *gdpr.Suppression
	Components  []run.Func
}

// CrawlerOptions customise construction (tests).
type CrawlerOptions struct {
	Stages      []string
	DoerFactory session.DoerFactory // nil: the fingerprinted tls-client
	RawS3       *minio.Client       // nil disables raw capture
	// DrainTimeout bounds how long in-flight tasks may run at shutdown
	// before they are abandoned (default 75s).
	DrainTimeout time.Duration
	// RetryScale shortens retry delays (tests).
	RetryScale float64
}

// NewCrawler builds igs's crawler: identity pool, GraphQL client, the
// profile and post stages, one consumer per stage lane and the retry
// relay. db is a pool on schema ig.
func NewCrawler(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, rdb *redis.Client, producer *kafka.Producer, o CrawlerOptions) (*Crawler, error) {
	g := cfg.IG
	seedBytes, err := configs.FS.ReadFile(g.RegistrySeed)
	if err != nil {
		return nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	reg, err := registry.In(registry.IGTables).NewDB(ctx, db, seed)
	if err != nil {
		return nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	proxies, err := proxy.New(g.Proxy)
	if err != nil {
		return nil, err
	}
	// crawler.stages names the copied Facebook crawler's stages; igs's
	// binary takes its own with --stages.
	var stages []string
	for _, s := range o.Stages {
		if s = strings.TrimSpace(s); s != "" && !slices.Contains(stages, s) {
			stages = append(stages, s)
		}
	}
	if len(stages) == 0 {
		stages = DefaultStages
	}
	slots := cfg.Crawler.Slots
	if slots <= 0 {
		for _, v := range cfg.Crawler.Lanes {
			slots += v
		}
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: g.BaseURL, Profile: g.FingerprintProfile, Profiles: g.FingerprintProfiles, Timeout: g.RequestTimeout,
		IdentityRPS: g.IdentityRPS, IdentityBurst: g.IdentityBurst,
		MaxRequestsMin: g.IdentityMaxRequestsMin, MaxRequestsMax: g.IdentityMaxRequestsMax,
		MaxPerTier: max(16, slots*len(stages)*13/10),
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	rules, err := igdetect.Rules().Apply(g.Detect)
	if err != nil {
		return nil, err
	}
	gql := &graphql.Client{
		Registry: reg, Rules: rules, Limiter: ratelimit.NewRedis(rdb),
		PageRPS: g.ProfileRPS, PageRPSFast: g.ProfileRPSP0, GlobalRPS: g.GlobalRPS,
		Builder:  igclient.Builder{AcceptLanguage: g.AcceptLanguage},
		Breakers: breaker.New(g.QueryBreaker),
	}
	gql.Breakers.OnChange = func(key string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("query:" + key).Set(float64(to))
		slog.Warn("graphql: query breaker", "query", key, "from", from.String(), "to", to.String())
	}
	supp := gdpr.NewSuppression(db)
	loader := &session.Loader{Rules: rules, AcceptLanguage: g.AcceptLanguage, Hooks: igclient.Hooks()}
	if g.Browser.Enabled {
		b, err := browser.NewBootstrapper(g.Browser, g.AcceptLanguage, igclient.Hooks())
		if err != nil {
			return nil, fmt.Errorf("ig.browser is enabled: %w", err)
		}
		b.Rules = rules
		loader.Browser = b
	}
	deps := &stage.Deps{
		Cfg: cfg, Pool: pool, Proxies: proxies, Loader: loader, GQL: gql, Producer: producer,
		Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp,
		Emit: &emit.Emitter{HMACKey: []byte(g.Privacy.AuthorHMACKey), Pseudonymize: g.Privacy.PIIMode == "pseudonymized", Suppression: supp},
	}
	worker := &stage.Worker{D: deps, MaxAttempts: uint32(cfg.Crawler.MaxAttempts), MaxTotalAttempts: uint32(cfg.Crawler.MaxTotalAttempts),
		LeaseTTL: cfg.Crawler.LeaseTTL, RetryScale: o.RetryScale,
		Stages: map[igstaskv1.Kind]stage.Stage{
			igstaskv1.Kind_KIND_PROFILE: &stage.ProfileStage{D: deps},
			igstaskv1.Kind_KIND_POST:    &stage.PostStage{D: deps},
		}}
	c := &Crawler{Deps: deps, Worker: worker, Registry: reg, Suppression: supp}
	c.Components = []run.Func{
		func(ctx context.Context) error { return reg.Run(ctx, g.RegistryRefresh) },
		func(ctx context.Context) error { return supp.Run(ctx, cfg.Crawler.SuppressionRefresh) },
	}
	if g.AIMD.Enabled {
		rep := ratelimit.NewReporter(rdb, cfg.Kafka.TopicPrefix)
		gql.Outcomes = rep
		ceil := ratelimit.NewCeiling(rdb, cfg.Kafka.TopicPrefix, g.GlobalRPS)
		gql.Ceiling = ceil
		c.Components = append(c.Components, rep.Run, ceil.Run)
	}
	if o.RawS3 != nil && g.CaptureRaw != "none" {
		sink := rawsink.New(o.RawS3, cfg.S3.RawBucket)
		gql.Raw, gql.CaptureAll = sink, g.CaptureRaw == "all"
		c.Components = append(c.Components, func(ctx context.Context) error { return sink.Run(ctx, time.Minute) })
	}
	kcfg, err := clients.Kafka(cfg, "-igs-crawler")
	if err != nil {
		return nil, err
	}
	drain := o.DrainTimeout
	if drain <= 0 {
		drain = 75 * time.Second
	}
	kinds := map[string]igstaskv1.Kind{"profile": igstaskv1.Kind_KIND_PROFILE, "post": igstaskv1.Kind_KIND_POST}
	for _, s := range stages {
		if s == "retry" {
			relay := &stage.RetryRelay{Producer: producer}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: groupRelay,
				Topics: topics.RetryTopics, MaxInflight: 64, MaxInflightPerPartition: 1, DrainTimeout: 5 * time.Second}, relay.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
			continue
		}
		kind, ok := kinds[s]
		if !ok {
			return nil, fmt.Errorf("unknown igs stage %q", s)
		}
		sched := laneScheduler(cfg.Crawler, s)
		for i, lane := range topics.Lanes {
			n := cfg.Crawler.Lanes[topics.LaneName(lane)]
			if n <= 0 {
				continue
			}
			var shared kafka.Slots
			if sched != nil {
				shared = lanes.Slots{S: sched, Lane: i}
			}
			topic, _ := topics.TaskTopic(kind, lane)
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: groupCrawler + s + "-" + topics.LaneName(lane), Topics: []string{topic},
				MaxInflight: n, MaxInflightPerPartition: cfg.Crawler.MaxInflightPerPartition, DrainTimeout: drain, Slots: shared,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
		}
		slog.Info("igs stage enabled", "stage", s)
	}
	return c, nil
}

// laneScheduler shares a stage's slots between its lanes, or returns nil
// when crawler.slots is 0 (fixed slots per lane).
func laneScheduler(c config.Crawler, stage string) *lanes.Scheduler {
	if c.Slots <= 0 {
		return nil
	}
	var ls []lanes.Lane
	for _, name := range []string{"p0", "p1", "p2"} {
		ls = append(ls, lanes.Lane{Name: name, Max: c.Lanes[name], Min: c.LaneMin})
	}
	s := lanes.New(c.Slots, ls...)
	s.OnChange = func(lane string, used, waiting int) {
		metrics.LaneSlots.WithLabelValues("ig-"+stage, lane, "used").Set(float64(used))
		metrics.LaneSlots.WithLabelValues("ig-"+stage, lane, "waiting").Set(float64(waiting))
	}
	return s
}
