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
	if g.IdentityMaxRequestsMin != 500 || g.IdentityMaxRequestsMax != 750 || g.PageWallRest != 30*time.Minute {
		t.Fatalf("identities: %d..%d rest %s", g.IdentityMaxRequestsMin, g.IdentityMaxRequestsMax, g.PageWallRest)
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
		{"age", func(c *Config) { c.IG.Limits.MaxAgeDays = 400 }, "ig.limits.max_age_days"},
		{"interval", func(c *Config) { c.IG.Schedule.Interval = 10 * time.Minute }, "ig.schedule.interval"},
		{"refresh", func(c *Config) { c.IG.Schedule.ProfileRefresh = time.Hour }, "ig.schedule.profile_refresh"},
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
