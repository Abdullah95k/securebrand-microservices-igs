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
	// PageWallRest rests an exit that sends fresh page loads to the login
	// page (three of the owner's ten ISP exits did).
	PageWallRest time.Duration  `koanf:"page_wall_rest"`
	CaptureRaw   string         `koanf:"capture_raw"`
	Proxy        Proxy          `koanf:"proxy"`
	QueryBreaker breaker.Config `koanf:"query_breaker"`
	AIMD         AIMD           `koanf:"aimd"`
	Browser      Browser        `koanf:"browser"`
	Detect       detect.Config  `koanf:"detect"`
	Limits       IGLimits       `koanf:"limits"`
	Schedule     IGSchedule     `koanf:"schedule"`
	Privacy      IGPrivacy      `koanf:"privacy"`
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
