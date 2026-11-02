package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/media"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/registrysync"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// NewRegistrySync builds igs's registry sync over schema ig (db): its own
// small identity pool on ig.registry_sync.tier (by default the first tier
// routed for the "registry_sync" class), and the registry for the client
// parameters. With ig.browser enabled, a rendered canary profile's own
// requests supply doc_ids the bundles do not declare. Callers run
// reg.Reload before each sync, or reg.Run alongside.
func NewRegistrySync(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, doers session.DoerFactory) (*registrysync.Sync, *registry.DB, error) {
	g := cfg.IG
	seedBytes, err := configs.FS.ReadFile(g.RegistrySeed)
	if err != nil {
		return nil, nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, nil, err
	}
	reg, err := registry.In(registry.IGTables).NewDB(ctx, db, seed)
	if err != nil {
		return nil, nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, nil, err
	}
	proxies, err := proxy.New(g.Proxy)
	if err != nil {
		return nil, nil, err
	}
	rc := g.RegistrySync
	if rc.Tier == "" {
		rc.Tier = proxies.TiersFor("registry_sync")[0]
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: g.BaseURL, Profile: g.FingerprintProfile, Profiles: g.FingerprintProfiles, Timeout: g.RequestTimeout,
		IdentityRPS: g.IdentityRPS, IdentityBurst: g.IdentityBurst,
		MaxRequestsMin: g.IdentityMaxRequestsMin, MaxRequestsMax: g.IdentityMaxRequestsMax,
		MaxPerTier: len(rc.CanaryPages) + 2,
	}, proxies, fps, doers)
	if err != nil {
		return nil, nil, err
	}
	rules, err := igdetect.Rules().Apply(g.Detect)
	if err != nil {
		return nil, nil, err
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
			return nil, nil, fmt.Errorf("ig.browser is enabled: %w", err)
		}
		b.Rules, b.CaptureFilter = rules, browser.IsInstagramGraphQL
		s.Loader.Browser = b
		s.Render = b.Capture
	}
	return s, reg, nil
}
