package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/phase0"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Phase 0 exit codes.
const (
	exitGo          = 0
	exitError       = 1
	exitUsage       = 2
	exitNoGo        = 3
	exitNeedsAction = 4
	exitInterrupted = 130
)

// runPhase0 runs igs's Phase 0 kit (docs/runbooks/igs-phase0.md).
func runPhase0(args []string) int {
	fs := flag.NewFlagSet("igs-spike phase0", flag.ExitOnError)
	cfgPath := fs.String("config", os.Getenv("IGS_CONFIG"), "config file with the proxy tiers (ig.proxy, or fb.proxy as in phase0/proxies.yaml)")
	profilesFile := fs.String("profiles", "", "file with one public profile per line (about 150 for C3)")
	tiersFlag := fs.String("tiers", "isp", "comma separated proxy tiers to measure (direct = this machine's IP)")
	outDir := fs.String("out-dir", "phase0/out-ig-phase0", "output directory")
	from := fs.String("from", "", "first step: "+strings.Join(phase0.Steps, ", ")+" (to resume)")
	regFile := fs.String("registry", "", "registry file to use instead of the docids step")
	rps := fs.Float64("rps", 0.3, "requests per second per identity (the probe runs' pace)")
	parallel := fs.Int("parallel", 3, "profiles read at once, each by its own identity and exit")
	idRequests := fs.Int("identity-requests", 500, "retire an identity (and so move to another exit) after about this many requests")
	maxAge := fs.Int("max-age-days", 365, "read each grid back this many days (the backfill window)")
	maxPages := fs.Int("max-grid-pages", 60, "the kit's cap on grid pages per profile; grids it stops are reported apart")
	gridPageSize := fs.Int("grid-page-size", 50, "posts asked for per grid page (Instagram served 50 in the probe runs)")
	commentPosts := fs.Int("comment-posts", 3, "read the comments of each profile's newest N posts")
	commentPages := fs.Int("comment-pages", 2, "comment pages per post")
	postDetails := fs.Int("post-details", 3, "post pages read per profile (about 125 KB each)")
	reelsPages := fs.Int("reels-pages", 3, "reels tab pages per profile")
	reelsPageSize := fs.Int("reels-page-size", 50, "reels asked for per page (Instagram served 50)")
	maxWait := fs.Duration("max-wait", 20*time.Minute, "how long a profile waits when every exit of the tier cools down")
	pageWallRest := fs.Duration("page-wall-rest", 30*time.Minute, "how long an exit rests once it walls fresh page loads; 0 never")
	fingerprint := fs.String("fingerprint", "chrome_146", "browser fingerprint profile")
	base := fs.String("base", igclient.BaseURL, "Instagram base URL (fakeig's for the dry run)")
	echoURL := fs.String("ip-echo-url", "https://api.ipify.org?format=json", "service that echoes the exit IP (empty skips the check)")
	minBoot := fs.Float64("min-bootstrap", phase0.DefaultThresholds.MinBootstrapSuccess, "GO needs at least this share of page loads with tokens")
	maxBlock := fs.Float64("max-block-rate", phase0.DefaultThresholds.MaxBlockRate, "GO needs at most this share of blocked queries")
	minComplete := fs.Float64("min-complete", phase0.DefaultThresholds.MinComplete, "GO needs at least this share of grids read to the age window or their end")
	maxP95 := fs.Float64("max-p95-ms", phase0.DefaultThresholds.MaxP95QueryMs, "GO needs the p95 query latency under this")
	perDay := fs.Float64("requests-per-day", phase0.DefaultRequestsPerDay, "production requests a day, for the monthly cost projection")
	_ = fs.Parse(args)
	if *profilesFile == "" {
		fmt.Fprintln(os.Stderr, "phase0: --profiles is required")
		return exitUsage
	}
	names, err := readProfiles(*profilesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return exitError
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0: config:", err)
		return exitError
	}
	pc := cfg.IG.Proxy
	if len(pc.Tiers) == 0 {
		pc = cfg.FB.Proxy // the Phase 0 machine's proxies file, shared with fbs's kit
	}
	proxies, err := proxy.New(pc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0: proxies:", err)
		return exitError
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return exitError
	}
	reqMin, reqMax := 100000, 100001
	if *idRequests > 0 {
		reqMin, reqMax = *idRequests, *idRequests+*idRequests/2
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: *base, Profile: *fingerprint, Timeout: 30 * time.Second,
		IdentityRPS: *rps, IdentityBurst: 1, MaxRequestsMin: reqMin, MaxRequestsMax: reqMax, MaxPerTier: *parallel + 3}, proxies, fps, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return exitError
	}
	seed, err := igclient.Seed()
	if err != nil {
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return exitError
	}
	rules := igdetect.Rules()
	costs, secrets := map[string]float64{}, []string{}
	for _, t := range pc.Tiers {
		costs[t.Name] = t.CostPerGBUSD
		secrets = append(secrets, t.Username, t.Password)
		for _, raw := range append([]string{t.Gateway}, t.Endpoints...) {
			if u, err := url.Parse(raw); err == nil && u.User != nil {
				p, _ := u.User.Password()
				secrets = append(secrets, u.User.Username(), p)
			}
		}
	}
	var tiers []string
	for _, t := range strings.Split(*tiersFlag, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tiers = append(tiers, t)
		}
	}
	k := &phase0.Kit{
		Stack: phase0.Stack{BaseURL: *base, Proxies: proxies, Pool: pool,
			Loader: &session.Loader{Rules: rules, AcceptLanguage: cfg.IG.AcceptLanguage, Hooks: igclient.Hooks()},
			NewGQL: func(s registry.Seed) *graphql.Client {
				return &graphql.Client{Registry: registry.NewStatic(s), Rules: rules, Builder: igclient.Builder{AcceptLanguage: cfg.IG.AcceptLanguage}}
			},
			HTTP: &http.Client{Timeout: 60 * time.Second},
			EchoClient: func(ep proxy.Endpoint) *http.Client {
				tr := &http.Transport{}
				if ep.URL != "" {
					if u, err := url.Parse(ep.URL); err == nil {
						tr.Proxy = http.ProxyURL(u)
					}
				}
				return &http.Client{Timeout: 20 * time.Second, Transport: tr}
			}},
		Cfg: phase0.KitConfig{Out: *outDir, Tiers: tiers, Profiles: names, BaseSeed: seed, Registry: *regFile, From: *from,
			Options: phase0.Options{RPS: *rps, Parallel: max(*parallel, 1), MaxAge: time.Duration(*maxAge) * 24 * time.Hour, MaxGridPages: *maxPages,
				CommentPosts: *commentPosts, CommentPages: *commentPages, PostDetails: *postDetails, ReelsPages: *reelsPages,
				GridPageSize: *gridPageSize, ReelsPageSize: *reelsPageSize, MaxWait: *maxWait, PageWallRest: restOrNever(*pageWallRest),
				Fingerprint: *fingerprint},
			CostPerGB: costs, IPEchoURL: *echoURL, Secrets: secrets, Version: version, RequestsPerDay: *perDay,
			Thresholds: phase0.Thresholds{MinBootstrapSuccess: *minBoot, MaxBlockRate: *maxBlock, MinComplete: *minComplete, MaxP95QueryMs: *maxP95}},
		Log: os.Stderr,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "phase0: %d profiles, tiers %v, %.2f requests/s per identity, %d at once -> %s\n", len(names), tiers, *rps, *parallel, *outDir)
	res, err := k.Run(ctx)
	if ctx.Err() != nil {
		fmt.Fprintf(os.Stderr, "phase0: interrupted; resume with --from <step>\n")
		return exitInterrupted
	}
	var se *phase0.StepError
	switch {
	case errors.As(err, &se) && se.NeedsAction:
		fmt.Fprintf(os.Stderr, "phase0: needs action: %v\nsend %s/%s\n", se, *outDir, phase0.FileBundle)
		return exitNeedsAction
	case err != nil:
		fmt.Fprintln(os.Stderr, "phase0:", err)
		return exitError
	case !res.Go:
		fmt.Fprintf(os.Stderr, "phase0: NO-GO; send %s/%s\n", *outDir, phase0.FileBundle)
		return exitNoGo
	}
	fmt.Fprintf(os.Stderr, "phase0: GO (recommended tier %s); send %s/%s\n", res.Recommended, *outDir, phase0.FileBundle)
	return exitGo
}
