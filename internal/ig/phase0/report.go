package phase0

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

// Thresholds are the GO / NO-GO criteria. The defaults come from the
// owner's probe runs (C2, build-status): on ISP exits at 0.3 requests a
// second every profile loaded once flagged exits rested, an exit met the
// rate limit only after 1,100 to 1,300 requests, and grids paged to the end
// of the age window.
type Thresholds struct {
	// MinBootstrapSuccess: profiles whose page load got tokens (after the
	// probe's retries on other exits).
	MinBootstrapSuccess float64 `json:"min_bootstrap_success"`
	// MaxBlockRate: queries answered by a rate limit, a login wall or a
	// checkpoint.
	MaxBlockRate float64 `json:"max_block_rate"`
	// MinComplete: profiles whose grid reached the age window or its end,
	// among those the kit's page cap did not stop.
	MinComplete float64 `json:"min_complete"`
	// MaxP95QueryMs: the 95th percentile query latency.
	MaxP95QueryMs float64 `json:"max_p95_query_ms"`
}

// DefaultThresholds are the C2-based criteria.
var DefaultThresholds = Thresholds{MinBootstrapSuccess: 0.95, MaxBlockRate: 0.05, MinComplete: 0.90, MaxP95QueryMs: 5000}

// DefaultRequestsPerDay is the production volume of the cost projection:
// about 300 profiles every 3 hours plus their post pages (a hypothesis
// until Phase 0 measures requests per profile).
const DefaultRequestsPerDay = 200_000

// Check keys, in TierSummary.Failed.
const (
	CheckBootstrap = "bootstrap"
	CheckBlockRate = "block_rate"
	CheckComplete  = "complete"
	CheckLatency   = "latency"
)

// Advice says what to try when a check fails.
var Advice = map[string]string{
	CheckBootstrap: "page loads are walled: rest or replace the flagged exits (preflight lists them), try the other fingerprint, or another tier",
	CheckBlockRate: "lower --rps, add exits or lower --identity-requests so exits rest in turn, or use another tier",
	CheckComplete:  "grids stop before the age window: compare the walls by depth; set ig.limits.max_age_days to what is reached, or accept partial backfills",
	CheckLatency:   "the tier is slow: check the provider's region and load, or use another tier",
}

// KindStats summarises one request kind.
type KindStats struct {
	N         int            `json:"n"`
	Classes   map[string]int `json:"classes"`
	P50Ms     float64        `json:"p50_ms"`
	P95Ms     float64        `json:"p95_ms"`
	MeanBytes float64        `json:"mean_wire_bytes"`
	P95Bytes  float64        `json:"p95_wire_bytes"`
	WireBytes int64          `json:"wire_bytes"`
}

// ProfileResult is one profile's outcome, by its line in the owner's file.
type ProfileResult struct {
	Line      int     `json:"line"`
	Bootstrap string  `json:"bootstrap"`
	PageWalls int     `json:"page_walls,omitempty"`
	GridStop  string  `json:"grid_stop,omitempty"`
	Posts     int     `json:"posts"`
	Days      float64 `json:"days"`
	Comments  int     `json:"comments"`
	Reels     int     `json:"reels"`
	Private   bool    `json:"private,omitempty"`
}

// TierRun is one tier's measurement (the smoke run or the full run).
type TierRun struct {
	Tier       string               `json:"tier"`
	StartedAt  time.Time            `json:"started_at"`
	FinishedAt time.Time            `json:"finished_at"`
	Profiles   []ProfileResult      `json:"profiles"`
	Requests   map[string]KindStats `json:"requests"`
	// Summary is the probe's, without anything that names an exit.
	Summary  probe.Summary `json:"summary"`
	Estimate *Estimate     `json:"estimate,omitempty"`
}

// Estimate projects the full run from the smoke run.
type Estimate struct {
	Profiles  int     `json:"profiles"`
	Requests  int     `json:"requests"`
	WireBytes int64   `json:"wire_bytes"`
	Minutes   float64 `json:"minutes"`
	CostUSD   float64 `json:"cost_usd"`
}

func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[int(math.Round(q*float64(len(s)-1)))]
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var t float64
	for _, x := range xs {
		t += x
	}
	return t / float64(len(xs))
}

func tierRun(tier string, started time.Time, rep probe.Report, rec *recorder) TierRun {
	tr := TierRun{Tier: tier, StartedAt: started, FinishedAt: time.Now().UTC(), Requests: map[string]KindStats{}, Summary: rep.Summary}
	tr.Summary.PageWallExits = nil // exit groups are the owner's business, not the bundle's
	for _, p := range rep.Profiles {
		pr := ProfileResult{Line: p.Index, Bootstrap: p.Bootstrap.Class, PageWalls: p.PageLoadWalls, Posts: len(p.Posts),
			Comments: len(p.Comments), Reels: len(p.Reels)}
		if p.Data != nil {
			pr.Private = p.Data.Private
		}
		if p.Grid != nil {
			pr.GridStop = p.Grid.Stop
		}
		// How far back the grid was read: its oldest post's time.
		var oldest time.Time
		for _, x := range p.Posts {
			if !x.PostedAt.IsZero() && (oldest.IsZero() || x.PostedAt.Before(oldest)) {
				oldest = x.PostedAt
			}
		}
		if !oldest.IsZero() {
			pr.Days = math.Round(time.Since(oldest).Hours()/24*10) / 10
		}
		tr.Profiles = append(tr.Profiles, pr)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for kind, k := range rec.reqs {
		var wire float64
		for _, b := range k.bytes {
			wire += b
		}
		tr.Requests[kind] = KindStats{N: len(k.ms), Classes: k.classes, P50Ms: quantile(k.ms, 0.5), P95Ms: quantile(k.ms, 0.95),
			MeanBytes: math.Round(mean(k.bytes)), P95Bytes: quantile(k.bytes, 0.95), WireBytes: int64(wire)}
	}
	return tr
}

// smoke reads a few profiles through each tier and projects the full run.
func (k *Kit) smoke(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	for _, tier := range k.Cfg.Tiers {
		started := time.Now().UTC()
		rep, rec := k.probeRun(ctx, tier, seed, first(k.Cfg.Profiles, k.Cfg.SmokeProfiles), "")
		tr := tierRun(tier, started, rep, rec)
		var reqs int
		var wire int64
		for _, s := range tr.Requests {
			reqs += s.N
			wire += s.WireBytes
		}
		n := max(len(tr.Profiles), 1)
		f := float64(len(k.Cfg.Profiles)) / float64(n)
		est := &Estimate{Profiles: len(k.Cfg.Profiles), Requests: int(float64(reqs) * f), WireBytes: int64(float64(wire) * f),
			Minutes: math.Round(tr.FinishedAt.Sub(started).Minutes()*f/float64(max(k.Cfg.Options.Parallel, 1))*10) / 10}
		est.CostUSD = math.Round(float64(est.WireBytes)/1e9*k.Cfg.CostPerGB[tier]*100) / 100
		tr.Estimate = est
		if err := writeJSON(k.path(SmokeFile(tier)), tr); err != nil {
			return err
		}
		k.logf("%s smoke: %d profiles; the full run: about %d requests, %.1f MB, %.0f minutes, $%.2f",
			tier, len(tr.Profiles), est.Requests, float64(est.WireBytes)/1e6, est.Minutes, est.CostUSD)
	}
	return nil
}

// run reads every profile through each tier.
func (k *Kit) run(ctx context.Context) error {
	seed, err := k.registry()
	if err != nil {
		return err
	}
	for _, tier := range k.Cfg.Tiers {
		started := time.Now().UTC()
		rep, rec := k.probeRun(ctx, tier, seed, k.Cfg.Profiles, "")
		if err := writeJSON(k.path(RunFile(tier)), tierRun(tier, started, rep, rec)); err != nil {
			return err
		}
		k.logf("%s run: %d profiles, %d posts, %d comments", tier, rep.Summary.Profiles, rep.Summary.Posts, rep.Summary.Comments)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

// TierSummary is one tier's result.
type TierSummary struct {
	Tier             string               `json:"tier"`
	Profiles         int                  `json:"profiles"`
	BootstrapSuccess float64              `json:"bootstrap_success"`
	PageLoadWalls    int                  `json:"page_load_walls"`
	Queries          int                  `json:"queries"`
	BlockRate        float64              `json:"block_rate"`
	ClassRate        map[string]float64   `json:"class_rate"`
	GridStops        map[string]int       `json:"grid_stops"`
	Complete         float64              `json:"complete"`
	Capped           int                  `json:"capped"`
	MedianDays       float64              `json:"median_days"`
	MedianPosts      float64              `json:"median_posts"`
	P50QueryMs       float64              `json:"p50_query_ms"`
	P95QueryMs       float64              `json:"p95_query_ms"`
	Requests         map[string]KindStats `json:"requests"`
	WireBytes        int64                `json:"wire_bytes"`
	WirePerProfile   float64              `json:"wire_bytes_per_profile"`
	RequestsPerProf  float64              `json:"requests_per_profile"`
	CostPerGBUSD     float64              `json:"cost_per_gb_usd,omitempty"`
	RunCostUSD       float64              `json:"run_cost_usd"`
	MonthlyUSD       float64              `json:"projected_monthly_usd"`
	Failed           []string             `json:"failed,omitempty"`
	Go               bool                 `json:"go"`
}

// Result is the report over every tier.
type Result struct {
	Tiers []TierSummary `json:"tiers"`
	// Go: at least one tier passes every check.
	Go bool `json:"go"`
	// Recommended is the passing tier with the lowest projected cost.
	Recommended string `json:"recommended,omitempty"`
}

var blocked = map[string]bool{"rate_limited": true, "login_wall": true, "checkpoint": true}

// Summarize judges one tier's run.
func Summarize(tr TierRun, th Thresholds, costPerGB, requestsPerDay float64) TierSummary {
	s := TierSummary{Tier: tr.Tier, Profiles: len(tr.Profiles), Requests: tr.Requests, ClassRate: map[string]float64{},
		GridStops: map[string]int{}, CostPerGBUSD: costPerGB}
	var days, posts []float64
	var ok, fullyRead, judged int
	for _, p := range tr.Profiles {
		s.PageLoadWalls += p.PageWalls
		if p.Bootstrap == "ok" {
			ok++
		}
		stop := p.GridStop
		if i := strings.IndexByte(stop, ':'); i > 0 {
			stop = stop[:i]
		}
		if stop == "" {
			continue
		}
		s.GridStops[stop]++
		switch stop {
		case "cap":
			s.Capped++ // the kit's own limit: says nothing about Instagram
			continue
		case "end", "age":
			fullyRead++
		}
		judged++
		days = append(days, p.Days)
		posts = append(posts, float64(p.Posts))
	}
	if s.Profiles > 0 {
		s.BootstrapSuccess = float64(ok) / float64(s.Profiles)
	}
	if judged > 0 {
		s.Complete = float64(fullyRead) / float64(judged)
	}
	s.MedianDays, s.MedianPosts = quantile(days, 0.5), quantile(posts, 0.5)
	classes := map[string]int{}
	var reqs int
	for kind, k := range tr.Requests {
		reqs += k.N
		s.WireBytes += k.WireBytes
		if kind == "page" || kind == "post_page" || kind == "reels_page" || kind == "profile_picture" {
			continue
		}
		s.Queries += k.N
		for c, n := range k.Classes {
			classes[c] += n
		}
	}
	for c, n := range classes {
		if s.Queries > 0 {
			s.ClassRate[c] = float64(n) / float64(s.Queries)
		}
		if blocked[c] && s.Queries > 0 {
			s.BlockRate += float64(n) / float64(s.Queries)
		}
	}
	s.P50QueryMs, s.P95QueryMs = p50p95(tr.Requests)
	if s.Profiles > 0 {
		s.WirePerProfile = math.Round(float64(s.WireBytes) / float64(s.Profiles))
		s.RequestsPerProf = math.Round(float64(reqs)/float64(s.Profiles)*10) / 10
	}
	s.RunCostUSD = math.Round(float64(s.WireBytes)/1e9*costPerGB*100) / 100
	if reqs > 0 {
		s.MonthlyUSD = math.Round(float64(s.WireBytes)/float64(reqs)*requestsPerDay*30/1e9*costPerGB*100) / 100
	}
	if s.BootstrapSuccess < th.MinBootstrapSuccess {
		s.Failed = append(s.Failed, CheckBootstrap)
	}
	if s.BlockRate > th.MaxBlockRate {
		s.Failed = append(s.Failed, CheckBlockRate)
	}
	if judged > 0 && s.Complete < th.MinComplete {
		s.Failed = append(s.Failed, CheckComplete)
	}
	if th.MaxP95QueryMs > 0 && s.P95QueryMs > th.MaxP95QueryMs {
		s.Failed = append(s.Failed, CheckLatency)
	}
	s.Go = len(s.Failed) == 0 && s.Profiles > 0
	return s
}

// p50p95 weighs each query kind's quantiles by its count: an approximation
// of the quantiles over all queries that needs no raw samples.
func p50p95(reqs map[string]KindStats) (float64, float64) {
	var p50, p95 float64
	var n int
	for kind, k := range reqs {
		if kind == "page" || kind == "post_page" || kind == "reels_page" || kind == "profile_picture" {
			continue
		}
		p50 += k.P50Ms * float64(k.N)
		p95 = math.Max(p95, k.P95Ms)
		n += k.N
	}
	if n == 0 {
		return 0, 0
	}
	return math.Round(p50 / float64(n)), p95
}

// report judges every tier's run and writes report.json and report.txt.
func (k *Kit) report() (Result, error) {
	var res Result
	perDay := k.Cfg.RequestsPerDay
	if perDay <= 0 {
		perDay = DefaultRequestsPerDay
	}
	for _, tier := range k.Cfg.Tiers {
		var tr TierRun
		if err := readJSON(k.path(RunFile(tier)), &tr); err != nil {
			return res, fmt.Errorf("%s: %w (run the run step first)", tier, err)
		}
		s := Summarize(tr, k.Cfg.Thresholds, k.Cfg.CostPerGB[tier], perDay)
		res.Tiers = append(res.Tiers, s)
		if s.Go && (res.Recommended == "" || s.MonthlyUSD < cost(res, res.Recommended)) {
			res.Recommended = s.Tier
		}
	}
	res.Go = res.Recommended != ""
	if err := writeJSON(k.path(FileReport), res); err != nil {
		return res, err
	}
	txt := Format(res, k.Cfg.Thresholds)
	if err := os.WriteFile(k.path(FileReportTxt), []byte(txt), 0o600); err != nil {
		return res, err
	}
	k.logf("%s", txt)
	return res, nil
}

func cost(res Result, tier string) float64 {
	for _, t := range res.Tiers {
		if t.Tier == tier {
			return t.MonthlyUSD
		}
	}
	return math.Inf(1)
}

// Format renders the report for people.
func Format(res Result, th Thresholds) string {
	var b strings.Builder
	verdict := "NO-GO"
	if res.Go {
		verdict = "GO (recommended tier: " + res.Recommended + ")"
	}
	fmt.Fprintf(&b, "igs Phase 0: %s\n\n", verdict)
	fmt.Fprintf(&b, "Thresholds: bootstrap >= %.0f%%, block rate <= %.1f%%, complete grids >= %.0f%%, p95 query <= %.0f ms\n\n",
		th.MinBootstrapSuccess*100, th.MaxBlockRate*100, th.MinComplete*100, th.MaxP95QueryMs)
	for _, t := range res.Tiers {
		v := "GO"
		if !t.Go {
			v = "NO-GO"
		}
		fmt.Fprintf(&b, "== %s: %s\n", t.Tier, v)
		fmt.Fprintf(&b, "  profiles %d, bootstrap %.1f%% (%d page loads walled first), queries %d, block rate %.2f%%\n",
			t.Profiles, t.BootstrapSuccess*100, t.PageLoadWalls, t.Queries, t.BlockRate*100)
		fmt.Fprintf(&b, "  grids: complete %.1f%% (stops %v; %d cut by the kit's page cap, not counted), median %.0f days, %.0f posts\n",
			t.Complete*100, t.GridStops, t.Capped, t.MedianDays, t.MedianPosts)
		fmt.Fprintf(&b, "  query latency p50 %.0f ms, p95 %.0f ms\n", t.P50QueryMs, t.P95QueryMs)
		kinds := make([]string, 0, len(t.Requests))
		for kind := range t.Requests {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)
		for _, kind := range kinds {
			s := t.Requests[kind]
			fmt.Fprintf(&b, "    %-16s n=%-5d p50 %5.0f ms  p95 %5.0f ms  mean %7.0f B  classes %v\n", kind, s.N, s.P50Ms, s.P95Ms, s.MeanBytes, s.Classes)
		}
		fmt.Fprintf(&b, "  wire %.1f MB (%.0f KB, %.1f requests a profile); run $%.2f, projected $%.2f a month\n",
			float64(t.WireBytes)/1e6, t.WirePerProfile/1e3, t.RequestsPerProf, t.RunCostUSD, t.MonthlyUSD)
		for _, f := range t.Failed {
			fmt.Fprintf(&b, "  FAILED %s: %s\n", f, Advice[f])
		}
		b.WriteString("\n")
	}
	return b.String()
}
