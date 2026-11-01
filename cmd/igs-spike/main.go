// Command igs-spike holds the tools the owner runs against real Instagram
// from the Phase 0 machine (docs/runbooks/igs-probe.md). Today: probe
// (checkpoint C2), which answers the open questions U1, U3, U5, U8 and U9
// for a short list of public profiles and writes what it read: the
// profiles, their posts, the newest post's comments and the profile
// pictures, as JSON, CSV and a .tar.gz to download.
//
//	igs-spike probe --dry-run                                  # rehearse against fakeig
//	igs-spike probe --profiles phase0/ig-profiles.txt --config phase0/proxies.yaml --tier isp
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
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
	fmt.Fprint(os.Stderr, `usage: igs-spike probe --profiles FILE [--config FILE] [--tier NAME] [--out-dir DIR]
       igs-spike probe --dry-run

probe loads each profile's page and reads it with the logged-out queries,
pacing like a person (one request every few seconds). It writes report.json,
profiles.csv, posts.csv, comments.csv and images/ to --out-dir, and the
same as a .tar.gz beside it. The files hold personal data: keep them off git.
--dry-run rehearses against a built-in fake Instagram, with no traffic.
`)
	os.Exit(2)
}

func runProbe(args []string) int {
	fs := flag.NewFlagSet("igs-spike probe", flag.ExitOnError)
	profilesFile := fs.String("profiles", "", "file with one public profile per line (username, @username or profile URL); # starts a comment")
	cfgPath := fs.String("config", os.Getenv("IGS_CONFIG"), "config file with proxy tiers under fb.proxy (phase0/proxies.yaml)")
	tier := fs.String("tier", "direct", "proxy tier to use (direct = this machine's own IP)")
	outDir := fs.String("out-dir", "", "output directory (default phase0/out-ig/<tier>-<time>)")
	images := fs.Bool("images", true, "save each profile picture under <out-dir>/images")
	raw := fs.Bool("raw", true, "save Instagram's raw answers under <out-dir>/raw (first of each query per profile, every failure, the first 3 pages' HTML)")
	rps := fs.Float64("rps", 0.3, "requests per second per identity (0.3 = one every ~3.3 s, as in the recordings)")
	maxPages := fs.Int("max-grid-pages", 32, "grid pages to read per profile after the first (12 posts each)")
	maxAge := fs.Int("max-age-days", 0, "stop a profile's grid once its posts are older than this many days (0 = no limit; the backfill window is 365)")
	commentPosts := fs.Int("comment-posts", 1, "read the comments of each profile's newest N posts")
	commentPages := fs.Int("comment-pages", 1, "comment pages to read per post (10 comments each, newest first)")
	startAt := fs.Int("start-at", 1, "start at this line of the profiles list, to continue a stopped run (use a new --out-dir)")
	parallel := fs.Int("parallel", 1, "profiles read at once, each by its own identity and exit IP, each at --rps")
	idRequests := fs.Int("identity-requests", 500, "retire an identity (and so move to another exit) after about this many requests; the full run met Instagram's rate limit after 1,100 to 1,300 on one IP (0 = never)")
	maxWait := fs.Duration("max-wait", 20*time.Minute, "how long a profile waits when every exit of the tier is cooling down, before it is given up")
	pageWallRest := fs.Duration("page-wall-rest", 30*time.Minute, "how long an exit rests once it sends fresh visits to the login page (no good page load yet, or two walls in a row); 0 never")
	gridPageSize := fs.Int("grid-page-size", 0, "posts to ask for per grid page (0 = the web app's 12); a speed test: does Instagram serve more?")
	commentPageSize := fs.Int("comment-page-size", 0, "comments to ask for per page (0 = the web app's 10); a speed test: does Instagram serve more?")
	commentRPS := fs.Float64("comment-rps", 0, "requests per second per identity for comment requests only (0 = --rps); everything else keeps --rps")
	commentHop := fs.Int("comment-hop", 0, "send comment requests through this many extra identities in turn, each page from another exit (0 = off)")
	reelsPages := fs.Int("reels-pages", 0, "pages of each profile's reels tab to read: view, like and comment counts per reel (0 = none)")
	reelsPageSize := fs.Int("reels-page-size", 0, "reels to ask for per page (0 = the web app's 4); a speed test: does Instagram serve more?")
	postDetails := fs.Int("post-details", 1, "read the pages of each profile's newest N posts (by time) for likes, comment count, tags, location, music, topics, duration (about 125 KB each)")
	fingerprint := fs.String("fingerprint", "chrome_146", "browser fingerprint profile (configs/fingerprints.yaml)")
	base := fs.String("base", igclient.BaseURL, "Instagram base URL")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake Instagram (no traffic, no proxies)")
	_ = fs.Parse(args)

	var names []string
	cfg := &config.Config{}
	if *dryRun {
		w := fakeig.NewWorld(fakeig.DefaultSpec())
		hs := httptest.NewServer(fakeig.NewServer(w, fakeig.Options{}))
		defer hs.Close()
		*base, *tier, *rps = hs.URL, "direct", 1000
		for _, p := range w.Profiles() {
			names = append(names, p.Username)
		}
		if *outDir == "" {
			*outDir = "phase0/out-ig-dryrun"
		}
	} else {
		if *profilesFile == "" {
			fmt.Fprintln(os.Stderr, "probe: --profiles is required (or --dry-run)")
			return 2
		}
		var err error
		if names, err = readProfiles(*profilesFile); err != nil {
			fmt.Fprintln(os.Stderr, "probe:", err)
			return 1
		}
		if cfg, err = config.Load(*cfgPath); err != nil {
			fmt.Fprintln(os.Stderr, "probe: config:", err)
			return 1
		}
	}
	if *parallel < 1 {
		*parallel = 1
	}
	poolRPS := *rps
	if *commentRPS > poolRPS {
		poolRPS = *commentRPS // the probe holds everything but comments to --rps
	}
	stack, err := newStack(cfg, *base, *fingerprint, poolRPS, *parallel+max(*commentHop, 0), *idRequests)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	if *outDir == "" {
		*outDir = filepath.Join("phase0", "out-ig", *tier+"-"+time.Now().UTC().Format("20060102-150405"))
	}
	imageDir, rawDir := "", ""
	if *images {
		imageDir = filepath.Join(*outDir, "images")
	}
	if *raw {
		rawDir = filepath.Join(*outDir, "raw")
	}
	if *startAt < 1 || *startAt > len(names) {
		fmt.Fprintf(os.Stderr, "probe: --start-at %d is outside the list (1 to %d)\n", *startAt, len(names))
		return 2
	}
	// The CSVs grow as profiles are read, so they can be opened mid-run.
	saver, err := probe.NewSaver(*outDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "probe: %d profiles, tier %s, %d at once, %.2f requests/s per identity; Ctrl-C stops and keeps what was read\n", len(names), *tier, *parallel, *rps)
	rep := probe.Run(ctx, probe.Options{BaseURL: *base, Profiles: names, Tier: *tier, MaxGridPages: *maxPages,
		Pool: stack.pool, Loader: stack.loader, GQL: stack.gql, Fingerprint: *fingerprint, MaxAge: time.Duration(*maxAge) * 24 * time.Hour, Version: version, ImageDir: imageDir, RawDir: rawDir, RawPages: 3,
		StartAt: *startAt, CommentPosts: *commentPosts, CommentPages: *commentPages, Parallel: *parallel, MaxWait: *maxWait, PageWallRest: restOrNever(*pageWallRest),
		GridPageSize: *gridPageSize, CommentPageSize: *commentPageSize, RPS: *rps, CommentRPS: *commentRPS, CommentHop: *commentHop, PostDetails: *postDetails,
		ReelsPages: *reelsPages, ReelsPageSize: *reelsPageSize,
		Progress: func(line string) { fmt.Fprintln(os.Stderr, "probe:", line) },
		OnProfile: func(p probe.Profile) {
			if err := saver.Add(p); err != nil {
				fmt.Fprintln(os.Stderr, "probe: saving profile:", err)
			}
		}})
	if err := saver.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
	}
	if err := probe.Write(*outDir, rep); err != nil {
		fmt.Fprintln(os.Stderr, "probe: write:", err)
		return 1
	}
	bundle, err := probe.Bundle(*outDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		return 1
	}
	s := rep.Summary
	fmt.Fprintf(os.Stderr, "probe: wrote %s and %s\n  read %d posts (%d with details, %d pinned) and %d comments from %d profiles; %d page loads walled (retried), %d waits for a cooling tier\n  bootstrapped %d/%d; grid: %d ended, %d capped, %d at the age limit, walls %v at %v posts, %d paged past 24 (max %d)\n"+
		"  cursor from another identity: %d worked, %d failed; grid by pk alone: %d worked, %d failed\n  queries without client state: %v; cookies %v\n",
		*outDir, bundle, s.Posts, s.PostDetails, s.PinnedPosts, s.Comments, s.Profiles, s.PageLoadWalls, s.TierWaits, s.Bootstrapped, s.Profiles, s.GridEnds, s.GridCaps, s.GridAgeStops, s.GridWalls, s.WalledAtPosts, s.PagedPast24, s.MaxPosts,
		s.CursorPortable, s.CursorNotPortable, s.GridByPKWorks, s.GridByPKFails, s.QueriesWithoutClientState, s.Cookies)
	if len(s.PageWallExits) > 0 {
		fmt.Fprintf(os.Stderr, "  login page on page loads, by exit: %v; exits rested %d times\n", s.PageWallExits, s.ExitsRested)
	}
	perSec := 0.0
	if s.CommentMs > 0 {
		perSec = float64(s.Comments) / (float64(s.CommentMs) / 1000)
	}
	if *reelsPages > 0 {
		fmt.Fprintf(os.Stderr, "  reels: %d read (pages of up to %d); first page by id %d times, from the reels page %d times\n",
			s.Reels, s.ReelsMaxPage, s.ReelsByIDWorks, s.ReelsPageFallback)
	}
	fmt.Fprintf(os.Stderr, "  speed: grid pages of up to %d posts; comments: %d requests, pages of up to %d, %.1f comments a second per chain, answers %v, %d hop identities\n",
		s.GridMaxPage, s.CommentRequests, s.CommentMaxPage, perSec, s.CommentAnswers, s.CommentHopIdentities)
	if s.StoppedEarly && rep.ContinueAt > 0 {
		fmt.Fprintf(os.Stderr, "  stopped early (Ctrl-C): profile %d of %d is the first not read completely (partial profiles count for no question).\n"+
			"  To continue, run again with --start-at %d and a new output directory.\n", rep.ContinueAt, len(names), rep.ContinueAt)
	}
	return 0
}

type stack struct {
	pool   *session.Pool
	loader *session.Loader
	gql    *graphql.Client
}

func newStack(cfg *config.Config, base, fingerprint string, rps float64, parallel, idRequests int) (stack, error) {
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
	// An identity retires after a random count in [n, 1.5n), so exits
	// rest in turn instead of each running until Instagram limits it.
	reqMin, reqMax := 100000, 100001
	if idRequests > 0 {
		reqMin, reqMax = idRequests, idRequests+idRequests/2
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: base, Profile: fingerprint, Timeout: 30 * time.Second,
		IdentityRPS: rps, IdentityBurst: 1, MaxRequestsMin: reqMin, MaxRequestsMax: reqMax, MaxPerTier: parallel + 3}, proxies, fps, nil)
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

// restOrNever maps the flag's 0 (never) to the probe's negative.
func restOrNever(d time.Duration) time.Duration {
	if d <= 0 {
		return -1
	}
	return d
}
