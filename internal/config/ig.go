package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

// IG configures igs, the Instagram scraper (section "ig:"). The shared
// sections (postgres, redis, kafka, s3, log, http) are the deployment's;
// igs's binaries connect to schema ig of the postgres database (ADR 0005 §4).
type IG struct {
	// BaseURL of the Instagram web surface. Tests point it at fakeig.
	BaseURL string `koanf:"base_url"`
	// RegistrySeed is igs's query registry seed, seeded into ig.queries.
	RegistrySeed    string        `koanf:"registry_seed"`
	RegistryRefresh time.Duration `koanf:"registry_refresh"`
	// FingerprintProfile and FingerprintProfiles: as fb's, per identity.
	FingerprintProfile  string        `koanf:"fingerprint_profile"`
	FingerprintProfiles []string      `koanf:"fingerprint_profiles"`
	AcceptLanguage      string        `koanf:"accept_language"`
	RequestTimeout      time.Duration `koanf:"request_timeout"`
	// IdentityRPS and IdentityBurst pace every identity. The owner's probe
	// read at 0.3 requests a second per exit; one exit met Instagram's rate
	// limit after 1,100 to 1,300 requests (build-status, full run).
	IdentityRPS   float64 `koanf:"identity_rps"`
	IdentityBurst int     `koanf:"identity_burst"`
	// An identity retires after a random number of requests in this range,
	// so exits rest in turn (the probe's --identity-requests).
	IdentityMaxRequestsMin int `koanf:"identity_max_requests_min"`
	IdentityMaxRequestsMax int `koanf:"identity_max_requests_max"`
	// ProfileRPS paces one profile fleet-wide; GlobalRPS is the fleet
	// ceiling and AIMD's start.
	ProfileRPS   int `koanf:"profile_rps"`
	ProfileRPSP0 int `koanf:"profile_rps_p0"`
	GlobalRPS    int `koanf:"global_rps"`
	// ExitRPS paces each proxy exit fleet-wide, however many identities and
	// pods share it: Instagram limits per IP (U4: one exit at 0.3 a second
	// met its limit after about an hour). The fleet reads at most exits ×
	// exit_rps; 0 leaves exits to identity pacing alone.
	ExitRPS float64 `koanf:"exit_rps"`
	// RateLimitRest and CheckpointRest rest an exit Instagram rate-limits
	// or sends a checkpoint, PageWallRest one that sends fresh page loads
	// to the login page (three of the owner's ten ISP exits did). After a
	// rest one identity probes the exit; a probe blocked again rests it
	// twice as long, up to ig.proxy.breaker.max_cooldown.
	RateLimitRest  time.Duration `koanf:"rate_limit_rest"`
	CheckpointRest time.Duration `koanf:"checkpoint_rest"`
	PageWallRest   time.Duration `koanf:"page_wall_rest"`
	// ExitPageLoadsPerDay caps the page loads (HTML pages: profiles, posts)
	// each static exit makes in a UTC day, fleet-wide: an exit that reaches
	// it rests until the day ends (03:00 Iraq time). How many a day an IP
	// tolerates is unmeasured (3 of 10 ISP exits were flagged in Phase 0),
	// so it is off (0) until the pilot measures it (owner, 6 Nov 2026).
	ExitPageLoadsPerDay int `koanf:"exit_page_loads_per_day"`
	// BlockHops is how many times a task run replaces a blocked identity
	// (rate limit, checkpoint, login wall) with a fresh one on another exit
	// and carries on, before it waits for a retry.
	BlockHops int `koanf:"block_hops"`
	// ShareExitRests spreads exit rests to every process on the same exits
	// through Valkey, read every ExitRestSync: a rest one pod sees holds
	// for all of them.
	ShareExitRests bool          `koanf:"share_exit_rests"`
	ExitRestSync   time.Duration `koanf:"exit_rest_sync"`
	// KeepIdentities keeps a stopping crawler's idle identities (cookies
	// and tokens, never proxy credentials) in Valkey this long, for the next
	// pod on the same exits; 0 = off.
	KeepIdentities time.Duration  `koanf:"keep_identities"`
	CaptureRaw     string         `koanf:"capture_raw"`
	Proxy          Proxy          `koanf:"proxy"`
	QueryBreaker   breaker.Config `koanf:"query_breaker"`
	AIMD           AIMD           `koanf:"aimd"`
	Browser        Browser        `koanf:"browser"`
	Detect         detect.Config  `koanf:"detect"`
	Limits         IGLimits       `koanf:"limits"`
	// RegistrySync follows Instagram's doc_id rotations (igs-registry-sync):
	// canary_pages are public profiles (usernames) with regular posts,
	// comments, reels and highlights.
	RegistrySync RegistrySync `koanf:"registry_sync"`
	Schedule     IGSchedule   `koanf:"schedule"`
	// Budget is the daily request and byte budget, fleet-wide (D9): when
	// the day's projected traffic exceeds it, backfill (p2) pauses; when
	// scheduled and on-demand traffic alone would, every scheduled run's
	// interval is stretched by Budget.Stretch. On-demand (p0) work is never
	// held. Both limits 0: no budget. StretchTiers is not used by igs.
	Budget  Budget    `koanf:"budget"`
	Privacy IGPrivacy `koanf:"privacy"`
	// Discovery finds profiles to register (M11).
	Discovery IGDiscovery `koanf:"discovery"`
}

// IGLimits are the defaults applied to jobs that do not override them.
type IGLimits struct {
	// MaxAgeDays bounds a backfill (D2: up to 365 days).
	MaxAgeDays    int   `koanf:"max_age_days"`
	MaxAgeCapDays int   `koanf:"max_age_cap_days"`
	MaxPosts      int64 `koanf:"max_posts"`
	// MaxCommentsPerPost: 0 reads every top-level comment.
	MaxCommentsPerPost int64 `koanf:"max_comments_per_post"`
	IncludeComments    bool  `koanf:"include_comments"`
	// IncludeMedia downloads media files. Off until the analysis service
	// exists (D5): metadata is always collected.
	IncludeMedia bool `koanf:"include_media"`
	// IncludeReels reads the profile's reels tab, for each reel's views.
	IncludeReels      bool   `koanf:"include_reels"`
	IncludeHighlights bool   `koanf:"include_highlights"`
	VideoPolicy       string `koanf:"video_policy"`
}

// IGSchedule is when profiles are crawled again (D2).
type IGSchedule struct {
	// Interval between a profile's scheduled runs: its newest posts, back to
	// the newest stored one.
	Interval time.Duration `koanf:"interval"`
	// ProfileRefresh is how often the profile itself (counts, bio, verified,
	// highlights list) is read again: daily (owner, 2026-10-31).
	ProfileRefresh time.Duration `koanf:"profile_refresh"`
	// Jitter spreads runs by up to this share of the interval.
	Jitter float64 `koanf:"jitter"`
	// PostRefreshWindow: posts younger than this get comment and count
	// refreshes on the scheduled runs (the grid walk reads only new posts),
	// each refresh waiting twice as long as the last.
	PostRefreshWindow time.Duration `koanf:"post_refresh_window"`
	// PostRefreshPerRun caps the refreshes riding on one scheduled run.
	PostRefreshPerRun int `koanf:"post_refresh_per_run"`
	// PostRecheckAfter: each post is read once more in full (its page and
	// every comment) when it is this old, to catch a deleted post and
	// deleted comments (REQ-008 fix 14). A post is due for a week after
	// that age; PostRecheckPerRun caps them per scheduled run. 0: never.
	PostRecheckAfter  time.Duration `koanf:"post_recheck_after"`
	PostRecheckPerRun int           `koanf:"post_recheck_per_run"`
	// ReelsWindow is how far back a scheduled run reads the reels tab for
	// the views of recent reels (owner, 6 Nov 2026: 7 days). Once every
	// ReelsFullEvery per profile, a run reads it back to limits.max_age_days
	// instead, so older reels' views are refreshed weekly. Backfills and
	// on-demand full runs always read it to max_age_days. 0 reads it back to
	// max_age_days on every run.
	ReelsWindow    time.Duration `koanf:"reels_window"`
	ReelsFullEvery time.Duration `koanf:"reels_full_every"`
	// MinInterval floors any profile's interval (the hot tier, a custom
	// interval).
	MinInterval time.Duration `koanf:"min_interval"`
	// Tiers sets how often each tier is read, and how profiles not pinned
	// to a tier are placed.
	Tiers IGTiers `koanf:"tiers"`
}

// IGTiers are the schedule's tiers (architecture §8): hot (client brands,
// their competitors, trending accounts: pinned by the API, igsctl or a
// client watchlist), warm (active accounts) and long tail (the rest).
// Profiles that are not pinned are placed by their posting rate.
type IGTiers struct {
	Hot  time.Duration `koanf:"hot"`
	Warm time.Duration `koanf:"warm"`
	// LongTail defaults to ig.schedule.interval.
	LongTail time.Duration `koanf:"long_tail"`
	// Auto places profiles that are not pinned: warm at WarmPostsPerDay or
	// more over AutoWindow, else long tail, checked every AutoEvery.
	Auto            bool          `koanf:"auto"`
	WarmPostsPerDay float64       `koanf:"warm_posts_per_day"`
	AutoWindow      time.Duration `koanf:"auto_window"`
	AutoEvery       time.Duration `koanf:"auto_every"`
}

// TierInterval returns tier's interval (hot, warm or long_tail).
func (s IGSchedule) TierInterval(tier string) time.Duration {
	switch tier {
	case "hot":
		return s.Tiers.Hot
	case "warm":
		return s.Tiers.Warm
	}
	if s.Tiers.LongTail > 0 {
		return s.Tiers.LongTail
	}
	return s.Interval
}

// IGDiscovery is profile discovery (M11): the accounts the crawl meets in
// the profiles it reads are vetted with one profile read each, scored for
// Iraqi signals, and registered for the schedule at Threshold or above.
type IGDiscovery struct {
	Enabled bool `koanf:"enabled"`
	// Sources are how candidates are met: tags, coauthors, mentions,
	// commenters (commenters only with privacy.pii_mode full).
	Sources []string `koanf:"sources"`
	// Threshold is the score at which a public candidate is registered.
	Threshold float64 `koanf:"threshold"`
	// MinPriority: a candidate is vetted once its weighted sightings reach
	// it (a co-author counts 3, a tag 2, a mention 2, a comment 1, a
	// sighting on a post located in Iraq 2 more).
	MinPriority float64 `koanf:"min_priority"`
	// VetInFlight caps vets running at once (each a backfill-lane job).
	VetInFlight int `koanf:"vet_in_flight"`
	// Interval is how often the leader scheduler vets and decides.
	Interval time.Duration `koanf:"interval"`
	// RevetAfter: a candidate below the threshold, private or gone is
	// vetted again when seen again this long after its decision.
	RevetAfter time.Duration `koanf:"revet_after"`
	// MinFollowers below which a candidate is not registered.
	MinFollowers int64 `koanf:"min_followers"`
	// FlushEvery is how often crawlers write their sightings.
	FlushEvery time.Duration `koanf:"flush_every"`
	// MaxNewPerDay caps the profiles discovery registers per UTC day (03:00
	// Iraq time); 0 = no cap. Candidates over the cap wait as "capped" and
	// are registered on the next days, highest score first.
	MaxNewPerDay int `koanf:"max_new_per_day"`
	// Discovery ranks below regular collection (owner, 6 Nov 2026). With
	// HoldWhenBehind, no vet starts while a scheduled profile is a full
	// interval overdue or its run is still going a full interval after it
	// started. With HoldRestingShare > 0, none starts while more than that
	// share of a static proxy tier's exits rest (the shared rests,
	// ig.share_exit_rests). Vets already running carry on.
	HoldWhenBehind   bool    `koanf:"hold_when_behind"`
	HoldRestingShare float64 `koanf:"hold_resting_share"`
}

// IGPrivacy keeps igs's person keys apart from fbs's (ADR 0005 §1).
type IGPrivacy struct {
	PIIMode string `koanf:"pii_mode"`
	// AuthorHMACKey keys author_key = HMAC(key, "ig:user:" + pk).
	AuthorHMACKey string `koanf:"author_hmac_key"`
}

// validateIG checks the ig section.
func (c *Config) validateIG() []error {
	g := c.IG
	var errs []error
	if g.BaseURL == "" || (!strings.HasPrefix(g.BaseURL, "https://") && !strings.HasPrefix(g.BaseURL, "http://")) {
		errs = append(errs, fmt.Errorf("ig.base_url %q must be an http(s) URL", g.BaseURL))
	}
	if g.RegistrySeed == "" {
		errs = append(errs, errors.New("ig.registry_seed must be set"))
	}
	if g.IdentityRPS <= 0 || g.IdentityBurst <= 0 {
		errs = append(errs, errors.New("ig.identity_rps and ig.identity_burst must be > 0"))
	}
	if g.Budget.RequestsPerDay < 0 || g.Budget.BytesPerDay < 0 || (g.Budget.Enabled && g.Budget.Interval <= 0) {
		errs = append(errs, errors.New("ig.budget: requests_per_day and bytes_per_day must not be negative, and interval must be > 0"))
	}
	if d := g.Discovery; d.Enabled {
		for _, src := range d.Sources {
			switch src {
			case "tags", "coauthors", "mentions", "commenters":
			default:
				errs = append(errs, fmt.Errorf("ig.discovery.sources: %q must be tags, coauthors, mentions or commenters", src))
			}
		}
		if d.Threshold <= 0 || d.VetInFlight <= 0 || d.Interval <= 0 || d.FlushEvery <= 0 || d.MinPriority < 0 || d.RevetAfter < 0 {
			errs = append(errs, errors.New("ig.discovery: threshold, vet_in_flight, interval and flush_every must be > 0; min_priority and revet_after not negative"))
		}
		if d.HoldRestingShare < 0 || d.HoldRestingShare > 1 {
			errs = append(errs, errors.New("ig.discovery.hold_resting_share must be in [0, 1]"))
		}
	}
	if g.ExitPageLoadsPerDay < 0 {
		errs = append(errs, errors.New("ig.exit_page_loads_per_day must not be negative"))
	}
	if g.ExitRPS < 0 || g.RateLimitRest < 0 || g.CheckpointRest < 0 || g.PageWallRest < 0 || g.BlockHops < 0 || g.ExitRestSync < 0 ||
		g.KeepIdentities < 0 {
		errs = append(errs, errors.New("ig.exit_rps, ig.rate_limit_rest, ig.checkpoint_rest, ig.page_wall_rest, ig.block_hops, ig.exit_rest_sync and ig.keep_identities must not be negative"))
	}
	if g.IdentityMaxRequestsMin <= 0 || g.IdentityMaxRequestsMax < g.IdentityMaxRequestsMin {
		errs = append(errs, errors.New("ig.identity_max_requests_min must be > 0 and <= identity_max_requests_max"))
	}
	if g.Limits.MaxAgeDays <= 0 || g.Limits.MaxAgeDays > g.Limits.MaxAgeCapDays {
		errs = append(errs, errors.New("ig.limits.max_age_days must be > 0 and <= ig.limits.max_age_cap_days"))
	}
	switch g.Limits.VideoPolicy {
	case "none", "thumbnail", "sd", "hd":
	default:
		errs = append(errs, fmt.Errorf("ig.limits.video_policy %q must be none, thumbnail, sd or hd", g.Limits.VideoPolicy))
	}
	if g.Schedule.Interval < time.Hour {
		errs = append(errs, errors.New("ig.schedule.interval must be at least 1h"))
	}
	if g.Schedule.ProfileRefresh < g.Schedule.Interval {
		errs = append(errs, errors.New("ig.schedule.profile_refresh must not be shorter than ig.schedule.interval"))
	}
	if t := g.Schedule.Tiers; g.Schedule.MinInterval <= 0 || t.Hot < g.Schedule.MinInterval || t.Warm < t.Hot ||
		(t.LongTail != 0 && t.LongTail < t.Warm) || t.WarmPostsPerDay < 0 || (t.Auto && (t.AutoWindow < 24*time.Hour || t.AutoEvery <= 0)) {
		errs = append(errs, errors.New("ig.schedule: min_interval must be > 0, tiers hot >= min_interval, warm >= hot, long_tail >= warm; "+
			"with auto, auto_window >= 24h and auto_every > 0"))
	}
	if g.Schedule.PostRecheckAfter < 0 || g.Schedule.PostRecheckPerRun < 0 {
		errs = append(errs, errors.New("ig.schedule.post_recheck_after and post_recheck_per_run must not be negative"))
	}
	if g.Schedule.PostRefreshWindow < 0 || g.Schedule.PostRefreshPerRun < 0 {
		errs = append(errs, errors.New("ig.schedule.post_refresh_window and post_refresh_per_run must not be negative"))
	}
	if g.Schedule.ReelsWindow < 0 || (g.Schedule.ReelsWindow > 0 && g.Schedule.ReelsFullEvery <= 0) {
		errs = append(errs, errors.New("ig.schedule.reels_window must not be negative, and with a window reels_full_every must be > 0"))
	}
	if g.Schedule.Jitter < 0 || g.Schedule.Jitter >= 0.5 {
		errs = append(errs, errors.New("ig.schedule.jitter must be in [0, 0.5)"))
	}
	switch g.Privacy.PIIMode {
	case "full", "pseudonymized":
	default:
		errs = append(errs, fmt.Errorf("ig.privacy.pii_mode %q must be full or pseudonymized", g.Privacy.PIIMode))
	}
	if c.Service.Env == "prod" && (strings.HasPrefix(g.Privacy.AuthorHMACKey, "dev-only-change-me") || g.Privacy.AuthorHMACKey == c.Privacy.AuthorHMACKey) {
		errs = append(errs, errors.New("ig.privacy.author_hmac_key must be set in prod, and differ from privacy.author_hmac_key"))
	}
	switch g.CaptureRaw {
	case "all", "failures", "none":
	default:
		errs = append(errs, fmt.Errorf("ig.capture_raw %q must be all, failures or none", g.CaptureRaw))
	}
	if _, err := detect.DefaultRules().Apply(g.Detect); err != nil {
		errs = append(errs, fmt.Errorf("ig.detect: %w", err))
	}
	return errs
}

// BudgetOn reports whether a daily budget is set.
func (g IG) BudgetOn() bool {
	return g.Budget.Enabled && (g.Budget.RequestsPerDay > 0 || g.Budget.BytesPerDay > 0)
}
