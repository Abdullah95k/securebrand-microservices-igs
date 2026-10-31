// Command igs-spike holds the tools the owner runs against real Instagram
// from their own machine. Today: probe (checkpoint C2), which answers the
// open questions U1, U3, U5, U8 and U9 for a short list of public profiles
// and writes a JSON report with no usernames, ids or text.
//
//	igs-spike probe --profiles canary.txt                      # direct
//	igs-spike probe --profiles canary.txt --config phase0/proxies.yaml --tier isp
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "probe":
		os.Exit(runProbe(os.Args[2:]))
	case "version", "--version":
		fmt.Println(version)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: igs-spike probe --profiles FILE [--config FILE] [--tier NAME] [--out FILE]

probe loads each profile's page and reads it with the logged-out queries,
pacing like a person (one request every few seconds), and writes a JSON
report with no usernames, ids or text. Run it once direct, then once per
proxy tier.
`)
	os.Exit(2)
}

func runProbe(args []string) int {
	fs := flag.NewFlagSet("igs-spike probe", flag.ExitOnError)
	profilesFile := fs.String("profiles", "", "file with one public profile per line (username, @username or profile URL); # starts a comment")
	cfgPath := fs.String("config", os.Getenv("IGS_CONFIG"), "config file with proxy tiers under fb.proxy (the Phase 0 proxies.yaml format)")
	tier := fs.String("tier", "direct", "proxy tier to use (direct = no proxy)")
	out := fs.String("out", "", "report file (default probe-<tier>-<time>.json)")
	rps := fs.Float64("rps", 0.3, "requests per second per identity (0.3 = one every ~3.3 s, as in the recordings)")
	maxPages := fs.Int("max-grid-pages", 32, "grid pages to read per profile after the first (12 posts each)")
	fingerprint := fs.String("fingerprint", "chrome_146", "browser fingerprint profile (configs/fingerprints.yaml)")
	base := fs.String("base", igclient.BaseURL, "Instagram base URL (tests point it at fakeig)")
	_ = fs.Parse(args)
	if *profilesFile == "" {
		fmt.Fprintln(os.Stderr, "probe: --profiles is required")
		return 2
	}
	names, err := readProfiles(*profilesFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe: config:", err)
		return 1
	}
	stack, err := newStack(cfg, *base, *fingerprint, *rps)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	if *out == "" {
		*out = fmt.Sprintf("probe-%s-%s.json", *tier, time.Now().UTC().Format("20060102-150405"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "probe: %d profiles, tier %s, %.2f requests/s per identity; Ctrl-C stops and keeps what was read\n", len(names), *tier, *rps)
	rep := probe.Run(ctx, probe.Options{BaseURL: *base, Profiles: names, Tier: *tier, MaxGridPages: *maxPages,
		Pool: stack.pool, Loader: stack.loader, GQL: stack.gql, Fingerprint: *fingerprint, Version: version,
		Progress: func(line string) { fmt.Fprintln(os.Stderr, "probe:", line) }})
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(*out, append(b, '\n'), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "probe: write report:", err)
		return 1
	}
	s := rep.Summary
	fmt.Fprintf(os.Stderr, "probe: report %s\n  bootstrapped %d/%d; grid: %d ended, %d capped, walls %v at %v posts, %d paged past 24 (max %d)\n"+
		"  cursor from another identity: %d worked, %d failed; grid by pk alone: %d worked, %d failed\n  queries without client state: %v; cookies %v\n",
		*out, s.Bootstrapped, s.Profiles, s.GridEnds, s.GridCaps, s.GridWalls, s.WalledAtPosts, s.PagedPast24, s.MaxPosts,
		s.CursorPortable, s.CursorNotPortable, s.GridByPKWorks, s.GridByPKFails, s.QueriesWithoutClientState, s.Cookies)
	return 0
}

type stack struct {
	pool   *session.Pool
	loader *session.Loader
	gql    *graphql.Client
}

func newStack(cfg *config.Config, base, fingerprint string, rps float64) (stack, error) {
	fpBytes, err := configs.FS.ReadFile("fingerprints.yaml")
	if err != nil {
		return stack{}, err
	}
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return stack{}, err
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return stack{}, err
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: base, Profile: fingerprint, Timeout: 30 * time.Second,
		IdentityRPS: rps, IdentityBurst: 1, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 4}, proxies, fps, nil)
	if err != nil {
		return stack{}, err
	}
	seed, err := igclient.Seed()
	if err != nil {
		return stack{}, err
	}
	rules := igdetect.Rules()
	return stack{pool: pool,
		loader: &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Hooks: igclient.Hooks()},
		gql:    &graphql.Client{Registry: registry.NewStatic(seed), Rules: rules, Builder: igclient.Builder{AcceptLanguage: cfg.FB.AcceptLanguage}},
	}, nil
}

// readProfiles reads usernames: one per line, @ and profile URLs accepted,
// blank lines and # comments skipped.
func readProfiles(path string) ([]string, error) {
	f, err := os.Open(path) //nolint:gosec // the owner's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if u := username(sc.Text()); u != "" {
			out = append(out, u)
		}
	}
	if len(out) == 0 && sc.Err() == nil {
		return nil, fmt.Errorf("%s holds no profiles", path)
	}
	return out, sc.Err()
}

func username(line string) string {
	line, _, _ = strings.Cut(line, "#")
	line = strings.TrimSpace(line)
	for _, p := range []string{"https://", "http://", "www.", "instagram.com/"} {
		line = strings.TrimPrefix(line, p)
	}
	line = strings.TrimPrefix(line, "@")
	line, _, _ = strings.Cut(line, "/")
	line, _, _ = strings.Cut(line, "?")
	return line
}
