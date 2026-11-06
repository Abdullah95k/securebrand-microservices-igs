package config

import (
	"strings"
	"testing"
	"time"
)

// The ig section's defaults follow the brief: newest posts every 3 hours,
// the profile daily, backfills up to 365 days, media files off (D5),
// metadata and reel views on.
func TestIGDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	g := c.IG
	if g.BaseURL != "https://www.instagram.com" || g.RegistrySeed != "ig/queries.seed.yaml" {
		t.Fatalf("surface: %s %s", g.BaseURL, g.RegistrySeed)
	}
	if g.Schedule.Interval != 3*time.Hour || g.Schedule.ProfileRefresh != 24*time.Hour {
		t.Fatalf("schedule: %+v", g.Schedule)
	}
	if g.Limits.MaxAgeDays != 365 || g.Limits.IncludeMedia || !g.Limits.IncludeReels || !g.Limits.IncludeComments {
		t.Fatalf("limits: %+v", g.Limits)
	}
	// The reels tab: 7 days back on a scheduled run, all of it weekly (the
	// owner, 2026-11-06).
	if g.Schedule.ReelsWindow != 7*24*time.Hour || g.Schedule.ReelsFullEvery != 7*24*time.Hour {
		t.Fatalf("reels: window %s, full every %s", g.Schedule.ReelsWindow, g.Schedule.ReelsFullEvery)
	}
	// Discovery waits for regular collection, with no daily cap (the
	// owner, 2026-11-06).
	if d := g.Discovery; !d.HoldWhenBehind || d.HoldRestingShare != 0.5 || d.MaxNewPerDay != 0 {
		t.Fatalf("discovery: hold_when_behind %v, hold_resting_share %v, max_new_per_day %d", d.HoldWhenBehind, d.HoldRestingShare, d.MaxNewPerDay)
	}
	if g.IdentityMaxRequestsMin != 500 || g.IdentityMaxRequestsMax != 750 || g.PageWallRest != 30*time.Minute {
		t.Fatalf("identities: %d..%d rest %s", g.IdentityMaxRequestsMin, g.IdentityMaxRequestsMax, g.PageWallRest)
	}
	if g.ExitRPS != 0.25 || g.RateLimitRest != 10*time.Minute || g.CheckpointRest != 30*time.Minute || g.BlockHops != 2 {
		t.Fatalf("exits: %v rps, rests %s %s, hops %d", g.ExitRPS, g.RateLimitRest, g.CheckpointRest, g.BlockHops)
	}
	// No page-load limit until the pilot measures one (the owner, 2026-11-06).
	if g.ExitPageLoadsPerDay != 0 {
		t.Fatalf("exit_page_loads_per_day %d, want 0 (off)", g.ExitPageLoadsPerDay)
	}
	// Everything is kept forever (the owner, 2026-11-03 and 2026-11-06):
	// collected data, media rows and the job log.
	if r := c.Privacy.Retention; r != (Retention{}) {
		t.Fatalf("retention %+v: nothing expires by age", r)
	}
	if len(g.FingerprintProfiles) == 0 || g.FingerprintProfile != "chrome_152" {
		t.Fatalf("fingerprints: %s %v", g.FingerprintProfile, g.FingerprintProfiles)
	}
	if g.Privacy.AuthorHMACKey == c.Privacy.AuthorHMACKey {
		t.Fatal("igs's author key must differ from fbs's")
	}
}

// Env vars reach the ig section like any other.
func TestIGEnv(t *testing.T) {
	t.Setenv("IGS_IG__LIMITS__INCLUDE_MEDIA", "true")
	t.Setenv("IGS_IG__SCHEDULE__INTERVAL", "6h")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !c.IG.Limits.IncludeMedia || c.IG.Schedule.Interval != 6*time.Hour {
		t.Fatalf("env: %+v %+v", c.IG.Limits, c.IG.Schedule)
	}
}

func TestIGValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"base url", func(c *Config) { c.IG.BaseURL = "instagram.com" }, "ig.base_url"},
		{"exit rps", func(c *Config) { c.IG.ExitRPS = -1 }, "ig.exit_rps"},
		{"hops", func(c *Config) { c.IG.BlockHops = -1 }, "ig.block_hops"},
		{"page loads", func(c *Config) { c.IG.ExitPageLoadsPerDay = -1 }, "ig.exit_page_loads_per_day"},
		{"age", func(c *Config) { c.IG.Limits.MaxAgeDays = 400 }, "ig.limits.max_age_days"},
		{"interval", func(c *Config) { c.IG.Schedule.Interval = 10 * time.Minute }, "ig.schedule.interval"},
		{"refresh", func(c *Config) { c.IG.Schedule.ProfileRefresh = time.Hour }, "ig.schedule.profile_refresh"},
		{"reels window", func(c *Config) { c.IG.Schedule.ReelsWindow = -time.Hour }, "ig.schedule.reels_window"},
		{"reels full pass", func(c *Config) { c.IG.Schedule.ReelsFullEvery = 0 }, "ig.schedule.reels_window"},
		{"resting share", func(c *Config) { c.IG.Discovery.Enabled = true; c.IG.Discovery.HoldRestingShare = 1.5 }, "ig.discovery.hold_resting_share"},
		{"identities", func(c *Config) { c.IG.IdentityMaxRequestsMax = 10 }, "ig.identity_max_requests_min"},
		{"video", func(c *Config) { c.IG.Limits.VideoPolicy = "4k" }, "ig.limits.video_policy"},
		{"pii", func(c *Config) { c.IG.Privacy.PIIMode = "none" }, "ig.privacy.pii_mode"},
		{"detect", func(c *Config) { c.IG.Detect.StaleMessages = []string{"("} }, "ig.detect"},
		{"prod key", func(c *Config) { c.Service.Env = "prod"; c.Privacy.AuthorHMACKey = "fbs-key" }, "ig.privacy.author_hmac_key"},
		{"shared key", func(c *Config) {
			c.Service.Env = "prod"
			c.Privacy.AuthorHMACKey, c.IG.Privacy.AuthorHMACKey = "same", "same"
		}, "ig.privacy.author_hmac_key"},
	} {
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		tc.edit(c)
		err = c.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error naming %s", tc.name, err, tc.want)
		}
	}
}
