package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// NewRegistrySync builds igs's registry sync over schema ig (db): its own
// small identity pool on ig.registry_sync.tier (by default the first tier
// routed for the "registry_sync" class), and the registry for the client
// parameters. With ig.browser enabled, a rendered canary profile's own
// requests supply doc_ids the bundles do not declare. Callers run
// reg.Reload before each sync, or reg.Run alongside.
func NewRegistrySync(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, rdb *redis.Client, doers session.DoerFactory) (*registrysync.Sync, *registry.DB, run.Func, error) {
	g := cfg.IG
	seedBytes, err := configs.FS.ReadFile(g.RegistrySeed)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	reg, err := registry.In(registry.IGTables).NewDB(ctx, db, seed)
	if err != nil {
		return nil, nil, nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	proxies, err := proxy.New(g.Proxy)
	if err != nil {
		return nil, nil, nil, err
	}
	rc := g.RegistrySync
	if rc.Tier == "" {
		rc.Tier = proxies.TiersFor("registry_sync")[0]
	}
	pc := session.PoolConfig{
		BaseURL: g.BaseURL, Profile: g.FingerprintProfile, Profiles: g.FingerprintProfiles, Timeout: g.RequestTimeout,
		IdentityRPS: g.IdentityRPS, IdentityBurst: g.IdentityBurst,
		MaxRequestsMin: g.IdentityMaxRequestsMin, MaxRequestsMax: g.IdentityMaxRequestsMax,
		MaxPerTier: rc.PageLoads() + 2,
	}
	// The sync shares the crawlers' exits and their rests (with Valkey;
	// without it, its own), but keeps its own pace on each exit, a fifth of
	// the crawlers' (registrySyncExitShare): in the pacer's free-for-all
	// with dozens of crawler sessions per exit, each of its checks waited
	// about 80 seconds, and every run hit its timeout while the crawlers
	// read (pilot, 7 Nov 2026). Its few requests a run add at most a fifth
	// to an exit's rate, for the minutes a run lasts.
	sync := func(ctx context.Context) error { <-ctx.Done(); return nil }
	if rdb != nil {
		pc.ExitRPS, pc.ExitPacer = g.ExitRPS*registrySyncExitShare, ratelimit.NewRedis(rdb)
		pc.ExitKey = "exit-registry-sync:"
		pc.Rest = map[detect.Class]time.Duration{detect.RateLimited: g.RateLimitRest, detect.Checkpoint: g.CheckpointRest}
		if g.ShareExitRests {
			proxies.ShareRests(ratelimit.NewExitRests(rdb, cfg.Kafka.TopicPrefix))
			sync = func(ctx context.Context) error { return proxies.SyncRests(ctx, g.ExitRestSync) }
		}
	}
	pool, err := session.NewPool(pc, proxies, fps, doers)
	if err != nil {
		return nil, nil, nil, err
	}
	rules, err := igdetect.Rules().Apply(g.Detect)
	if err != nil {
		return nil, nil, nil, err
	}
	s := &registrysync.Sync{
		DB: db, Cfg: rc, Pool: pool, Rules: rules, Params: reg.ClientParams,
		Loader:         &session.Loader{Rules: rules, AcceptLanguage: g.AcceptLanguage, Hooks: igclient.Hooks()},
		AcceptLanguage: g.AcceptLanguage,
		Fetch:          docids.HTTPFetcher(media.NewHTTPClient()),
	}
	if g.Browser.Enabled {
		b, err := browser.NewBootstrapper(g.Browser, g.AcceptLanguage, igclient.Hooks())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("ig.browser is enabled: %w", err)
		}
		b.Rules, b.CaptureFilter = rules, browser.IsInstagramGraphQL
		s.Loader.Browser = b
		s.Render = b.Capture
	}
	return s, reg, sync, nil
}

// registrySyncExitShare is registry-sync's own pace on each exit, as a
// share of ig.exit_rps.
const registrySyncExitShare = 0.2
