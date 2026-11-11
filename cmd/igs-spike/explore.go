package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/explore"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/iraqtime"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
)

func runExplore(args []string) int {
	fs := flag.NewFlagSet("igs-spike explore", flag.ExitOnError)
	cfgPath := fs.String("config", os.Getenv("IGS_CONFIG"), "config file with proxy tiers under ig.proxy (and ig.postgres for --locations-from-db)")
	tier := fs.String("tier", "direct", "proxy tier to use (direct = this machine's own IP)")
	hashtags := fs.String("hashtags", "", "comma-separated hashtags (default: Iraq's places from the keyword list)")
	locations := fs.String("locations", "", "comma-separated Instagram location ids")
	fromDB := fs.Bool("locations-from-db", false, "take the most used locations in Iraq from the posts igs stored")
	maxPages := fs.Int("max-pages", 16, fmt.Sprintf("page loads in all, half each kind (%d at most)", explore.MaxPagesLimit))
	rps := fs.Float64("rps", 0.05, "requests per second (0.05: one every 20 seconds, the crawlers' exit pace)")
	outDir := fs.String("out-dir", "", "output directory (default out-explore/<tier>-<time>)")
	fingerprint := fs.String("fingerprint", "chrome_146", "browser fingerprint profile (configs/fingerprints.yaml)")
	base := fs.String("base", igclient.BaseURL, "Instagram base URL")
	dryRun := fs.Bool("dry-run", false, "rehearse against a built-in fake Instagram (no traffic, no proxies)")
	_ = fs.Parse(args)

	half := max(1, min(*maxPages, explore.MaxPagesLimit)/2)
	tags := split(*hashtags)
	if len(tags) == 0 {
		var err error
		if tags, err = placeHashtags(half); err != nil {
			fmt.Fprintln(os.Stderr, "explore:", err)
			return 1
		}
	}
	locs := split(*locations)
	cfg := &config.Config{}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *dryRun {
		w := fakeig.NewWorld(fakeig.DefaultSpec())
		hs := httptest.NewServer(fakeig.NewServer(w, fakeig.Options{Faults: fakeig.Faults{Explore: fakeig.ExploreOpen}}))
		defer hs.Close()
		*base, *tier, *rps = hs.URL, "direct", 1000
		if len(locs) == 0 {
			locs = []string{"1000001", "1000002"}
		}
		if *outDir == "" {
			*outDir = "out-explore-dryrun"
		}
	} else {
		var err error
		if cfg, err = config.Load(*cfgPath); err != nil {
			fmt.Fprintln(os.Stderr, "explore: config:", err)
			return 1
		}
		if *fromDB {
			got, err := iraqLocations(ctx, cfg, half)
			if err != nil {
				fmt.Fprintln(os.Stderr, "explore: locations:", err)
				return 1
			}
			locs = append(locs, got...)
		}
	}
	if len(locs) > half {
		locs = locs[:half]
	}
	if len(tags) > half {
		tags = tags[:half]
	}
	st, err := newStack(cfg, *base, *fingerprint, *rps, 1, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "explore:", err)
		return 1
	}
	if *outDir == "" {
		*outDir = filepath.Join("out-explore", *tier+"-"+time.Now().UTC().Format("20060102-150405"))
	}
	fmt.Fprintf(os.Stderr, "explore: %d hashtag and %d location pages, tier %s, one exit; Ctrl-C stops and keeps what was read\n",
		len(tags), len(locs), *tier)
	rep := explore.Run(ctx, explore.Options{BaseURL: *base, Hashtags: tags, Locations: locs, Tier: *tier, Pool: st.pool, Loader: st.loader,
		MaxPages: *maxPages, Progress: func(line string) { fmt.Fprintln(os.Stderr, "explore:", line) }})
	if err := explore.Write(*outDir, rep); err != nil {
		fmt.Fprintln(os.Stderr, "explore: write:", err)
		return 1
	}
	b, _ := json.Marshal(struct {
		Hashtag  explore.Totals `json:"hashtag"`
		Location explore.Totals `json:"location"`
	}{rep.Hashtag, rep.Location})
	stopped := rep.Stopped
	if stopped == "" {
		stopped = "no"
	}
	fmt.Printf("SUMMARY (counts only, safe to send back)\n  started %s, tier %s, %d requests, stopped early: %s\n  %s\n",
		iraqtime.Format(rep.StartedAt), rep.Tier, rep.Requests, stopped, b)
	return 0
}

func split(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

// placeHashtags are n hashtags from the Iraq keyword list: each group's
// first one-word term in English, then in Arabic, the country first.
func placeHashtags(n int) ([]string, error) {
	seed, err := keywords.IraqSeed()
	if err != nil {
		return nil, err
	}
	var groups []string
	first := map[string]map[string]string{}
	for _, k := range seed {
		if !k.Enabled || strings.ContainsAny(k.Term, " -'") || (k.Language != "en" && k.Language != "ar") {
			continue
		}
		if first[k.Group] == nil {
			first[k.Group] = map[string]string{}
			groups = append(groups, k.Group)
		}
		if first[k.Group][k.Language] == "" {
			first[k.Group][k.Language] = strings.ToLower(k.Term)
		}
	}
	var out []string
	for _, g := range groups {
		for _, lang := range []string{"en", "ar"} {
			if t := first[g][lang]; t != "" && len(out) < n {
				out = append(out, t)
			}
		}
	}
	return out, nil
}

// iraqLocations are the n locations in Iraq most used by the posts igs
// stored, by id. Read only.
func iraqLocations(ctx context.Context, cfg *config.Config, n int) ([]string, error) {
	db, err := pg.OpenSchema(ctx, cfg.Postgres.DSN, 1, "ig", pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(ctx, `SELECT location->>'pk', coalesce(location->>'name', ''), (location->>'lat')::float8, (location->>'lng')::float8
		FROM posts WHERE location ? 'pk' AND taken_at > now() - interval '90 days'
		GROUP BY 1, 2, 3, 4 ORDER BY count(*) DESC LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() && len(out) < n {
		var pk, name string
		var lat, lng *float64
		if err := rows.Scan(&pk, &name, &lat, &lng); err != nil {
			return nil, err
		}
		if pk != "" && pk != "0" && discovery.InIraq(name, lat, lng) {
			out = append(out, pk)
		}
	}
	return out, rows.Err()
}
