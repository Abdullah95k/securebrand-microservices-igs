package phase0

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/fixtures"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// recorder collects a probe run's requests, safely across its workers.
type recorder struct {
	mu   sync.Mutex
	reqs map[string]*kindStats
}

type kindStats struct {
	classes map[string]int
	ms      []float64
	bytes   []float64
}

func newRecorder() *recorder { return &recorder{reqs: map[string]*kindStats{}} }

func (r *recorder) add(kind string, s probe.Step) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := r.reqs[kind]
	if k == nil {
		k = &kindStats{classes: map[string]int{}}
		r.reqs[kind] = k
	}
	k.classes[s.Class]++
	k.ms = append(k.ms, float64(s.Ms))
	k.bytes = append(k.bytes, float64(s.WireBytes))
}

// probeRun reads profiles through a tier with the probe. rawDir, when set,
// receives the raw answers (capture only).
func (k *Kit) probeRun(ctx context.Context, tier string, seed registry.Seed, profiles []string, rawDir string) (probe.Report, *recorder) {
	o := k.Cfg.Options
	rec := newRecorder()
	rep := probe.Run(ctx, probe.Options{
		BaseURL: k.Stack.BaseURL, Profiles: profiles, Tier: tier, MaxGridPages: o.MaxGridPages, MaxAge: o.MaxAge,
		Pool: k.Stack.Pool, Loader: k.Stack.Loader, GQL: k.Stack.NewGQL(seed), Fingerprint: o.Fingerprint, Version: k.Cfg.Version,
		RawDir: rawDir, RawPages: len(profiles), CommentPosts: o.CommentPosts, CommentPages: o.CommentPages, Parallel: o.Parallel,
		MaxWait: o.MaxWait, PageWallRest: o.PageWallRest, GridPageSize: o.GridPageSize, CommentPageSize: o.CommentPageSize,
		RPS: o.RPS, PostDetails: o.PostDetails, ReelsPages: o.ReelsPages, ReelsPageSize: o.ReelsPageSize,
		OnRequest: rec.add,
	})
	return rep, rec
}

// Preflight is one tier's readiness: whether a page load gets tokens, and
// the exit IPs it leaves from.
type Preflight struct {
	Tier         string   `json:"tier"`
	Bootstrapped bool     `json:"bootstrapped"`
	Class        string   `json:"class"`
	Ms           int64    `json:"ms"`
	PageWalls    int      `json:"page_walls"`
	ExitIPs      []string `json:"exit_ips,omitempty"`
	EchoError    string   `json:"echo_error,omitempty"`
}

// preflight loads the first profile's page through each tier; a tier that
// gets no tokens stops the run.
func (k *Kit) preflight(ctx context.Context) error {
	var out []Preflight
	var failed []string
	for _, tier := range k.Cfg.Tiers {
		p := Preflight{Tier: tier}
		if k.Stack.EchoClient != nil && k.Cfg.IPEchoURL != "" {
			for range 2 {
				ep, err := k.Stack.Proxies.Next(tier)
				if err != nil {
					p.EchoError = err.Error()
					break
				}
				ip, err := echo(ctx, k.Stack.EchoClient(ep), k.Cfg.IPEchoURL)
				if err != nil {
					p.EchoError = err.Error()
					continue
				}
				p.ExitIPs = append(p.ExitIPs, ip)
			}
		}
		rep, _ := k.probeRun(ctx, tier, k.Cfg.BaseSeed, first(k.Cfg.Profiles, 1), "")
		if len(rep.Profiles) > 0 {
			b := rep.Profiles[0].Bootstrap
			p.Class, p.Ms, p.PageWalls = b.Class, b.Ms, rep.Profiles[0].PageLoadWalls
			p.Bootstrapped = b.Class == "ok"
		}
		if !p.Bootstrapped {
			failed = append(failed, tier)
		}
		k.logf("%s: bootstrap %s (%d ms, %d walls) exits %v", tier, p.Class, p.Ms, p.PageWalls, p.ExitIPs)
		out = append(out, p)
	}
	if err := writeJSON(k.path(FilePreflight), out); err != nil {
		return err
	}
	if len(failed) > 0 {
		return &StepError{Step: StepPreflight, Err: fmt.Errorf("no tokens through %v: check the tier's credentials and exits", failed)}
	}
	return nil
}

// echo asks an IP echo service (plain text or {"ip": ...}) which address a
// request left from.
func echo(ctx context.Context, hc *http.Client, u string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	if v := gjson.GetBytes(b, "ip"); v.Exists() {
		return v.String(), nil
	}
	return strings.TrimSpace(string(b)), nil
}

// DocIDs is what the docids step found and checked.
type DocIDs struct {
	// Found maps friendly names to the doc_ids the page's bundles carry.
	Found map[string]string `json:"found"`
	// Changed lists the queries whose doc_id differs from the seed.
	Changed []string `json:"changed,omitempty"`
	// Checks maps each query to how it answered with the doc_id kept.
	Checks map[string]string `json:"checks"`
	// Failing lists the queries no doc_id answered.
	Failing []string `json:"failing,omitempty"`
}

// docids scans the first profile's page bundles for the operations'
// doc_ids, takes them over the seed's, checks every query by reading a
// profile, and writes the registry the measuring steps use.
func (k *Kit) docids(ctx context.Context) error {
	tier := k.Cfg.Tiers[0]
	d := DocIDs{Found: map[string]string{}, Checks: map[string]string{}}
	id, err := k.Stack.Pool.Acquire(ctx, tier)
	if err != nil {
		return &StepError{Step: StepDocIDs, Err: err}
	}
	page := strings.TrimRight(k.Stack.BaseURL, "/") + "/" + k.Cfg.Profiles[0] + "/"
	pl, err := k.Stack.Loader.Load(ctx, id, page)
	k.Stack.Pool.Release(id)
	if err != nil {
		return &StepError{Step: StepDocIDs, Err: fmt.Errorf("page load: %w", err)}
	}
	if pl == nil {
		return &StepError{Step: StepDocIDs, Err: errors.New("page load: no page")}
	}
	pu, _ := url.Parse(page)
	sc := &docids.Scanner{Fetch: docids.HTTPFetcher(k.Stack.HTTP), MaxBundles: 60}
	n := sc.Page(ctx, pl.HTML, pu, d.Found)
	k.logf("scanned %d bundles: %d operations", n, len(d.Found))
	seed := k.Cfg.BaseSeed
	seed.Queries = append([]registry.Query(nil), seed.Queries...)
	for i, q := range seed.Queries {
		if id, ok := d.Found[q.FriendlyName]; ok && id != q.DocID {
			seed.Queries[i].DocID = id
			d.Changed = append(d.Changed, q.Name)
		}
	}
	rep, _ := k.probeRun(ctx, tier, seed, first(k.Cfg.Profiles, k.Cfg.DocIDProfiles), "")
	check := func(name string, s *probe.Step) {
		if s == nil {
			return
		}
		if prev, ok := d.Checks[name]; ok && prev == "ok" {
			return
		}
		d.Checks[name] = s.Class
	}
	for _, p := range rep.Profiles {
		check("profile", p.Profile)
		check("grid_first", p.GridFirst)
		check("grid", p.GridByPK)
		check("comments", p.CommentsRead)
		check("highlights", p.Highlights)
		check("reels", p.ReelsFirst)
	}
	for name, class := range d.Checks {
		if class == "registry_stale" || class == "soft_error" {
			d.Failing = append(d.Failing, name)
		}
	}
	sort.Strings(d.Failing)
	b, err := registry.MarshalSeed(seed)
	if err != nil {
		return err
	}
	if err := os.WriteFile(k.path(FileRegistry), b, 0o600); err != nil {
		return err
	}
	if err := writeJSON(k.path(FileDocIDs), d); err != nil {
		return err
	}
	k.logf("doc_ids changed: %v; checks: %v", d.Changed, d.Checks)
	if len(d.Failing) > 0 {
		return &StepError{Step: StepDocIDs, NeedsAction: true, Err: fmt.Errorf("no working doc_id for %v: record a HAR of a profile and send it", d.Failing)}
	}
	return nil
}

// FixtureCheck is how the parsers read one captured answer.
type FixtureCheck struct {
	Case    string   `json:"case"`
	Query   string   `json:"query"`
	OK      bool     `json:"ok"`
	Missing []string `json:"missing,omitempty"`
}

// capture reads a few profiles keeping Instagram's raw answers, checks the
// parsers on each, and keeps them as scrubbed fixtures. Parser drift stops
// the run (a parser change is needed before the measurements mean much).
func (k *Kit) capture(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	raw := k.path(dirRaw)
	_ = os.RemoveAll(raw)
	defer func() { _ = os.RemoveAll(raw) }()
	if err := os.MkdirAll(raw, 0o700); err != nil {
		return err
	}
	k.probeRun(ctx, k.Cfg.Tiers[0], seed, first(k.Cfg.Profiles, k.Cfg.CaptureProfiles), raw)
	salt := k.Cfg.Salt
	if salt == "" {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		salt = hex.EncodeToString(b)
	}
	scrub := fixtures.New(salt)
	var checks []FixtureCheck
	var drift []string
	err = filepath.WalkDir(raw, func(path string, de os.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		body, err := os.ReadFile(path) //nolint:gosec // the kit's own capture directory
		if err != nil {
			return err
		}
		q := fixtures.QueryOf(path)
		html := strings.HasSuffix(path, ".html")
		ok, missing := parseCheck(q, body, html)
		name := fmt.Sprintf("p0-%03d", len(checks)+1)
		checks = append(checks, FixtureCheck{Case: name, Query: q, OK: ok, Missing: missing})
		if !ok {
			drift = append(drift, q)
		}
		c := fixtures.Case{Query: q, Name: name, Raw: body, HTML: html}
		if html {
			c.Wanted = []string{"xig_polaris_media", "polaris_clips_connection", "xig_user_by_username"}
		}
		if _, err := scrub.Write(k.path(DirFixtures), c); err != nil {
			k.logf("fixture %s (%s) not kept: %v", name, q, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := writeJSON(k.path(FileFixtures), checks); err != nil {
		return err
	}
	k.logf("captured %d answers; parser drift in %v", len(checks), drift)
	if len(checks) == 0 {
		return &StepError{Step: StepCapture, Err: errors.New("no answers captured")}
	}
	if len(drift) > 0 {
		return &StepError{Step: StepCapture, NeedsAction: true, Err: fmt.Errorf("the parsers no longer read %v: send the bundle", uniq(drift))}
	}
	return nil
}

// parseCheck runs the parser of query q over a raw answer. Only answers
// that carried data are judged: an error answer (a wall, a rate limit) is
// the measurement's business, not the parser's.
func parseCheck(q string, body []byte, html bool) (bool, []string) {
	if html {
		if q != "post_page" {
			return true, nil
		}
		res, rep := parse.PostPage(body, "", "https://www.instagram.com")
		if res.Gated {
			return true, nil
		}
		return rep.OK(), rep.Missing
	}
	parts := graphql.SplitParts(body)
	if len(parts) == 0 || !strings.Contains(string(body), `"data"`) {
		return true, nil
	}
	var rep walk.Report
	switch q {
	case "profile":
		res, r := parse.Profile(parts)
		if res.NotFound {
			return true, nil
		}
		rep = r
	case "grid", "grid_first":
		_, rep = parse.Grid(parts)
	case "comments":
		_, rep = parse.Comments(parts, parse.CommentContext{})
	case "reels":
		_, rep = parse.Reels(parts)
	default:
		return true, nil
	}
	return rep.OK(), rep.Missing
}

func uniq(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
