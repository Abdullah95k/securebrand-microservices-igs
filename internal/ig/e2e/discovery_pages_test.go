package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
)

// Discovery from hashtag and location pages (D-055): the scheduler lists
// the pages (the configured hashtags, the locations given) and dispatches
// them at the daily pace; a crawler loads each logged out and records the
// accounts it shows as candidates seen in Iraq, with origin place or
// hashtag. They are vetted as usual: an Iraqi account is registered, an
// English one stays below. Nothing of the page is stored: the account
// that is not registered leaves no profile or post.
func TestDiscoveryFromPages(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Faults: fakeig.Faults{Explore: fakeig.ExploreOpen}, Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags"}, Threshold: 4, MinPriority: 2, VetInFlight: 10,
			Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond,
			Pages: config.IGDiscoveryPages{PerDay: 200000, Kinds: []string{"location", "hashtag"}, Hashtags: []string{"#Baghdad"},
				Revisit: time.Hour}}
	}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "basra_lens", FullName: "مصور البصرة", Bio: "مصور فوتوغرافي من البصرة",
		Followers: 800, Captions: []string{"شلونكم", "غروب على شط العرب"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "coffee_lab", FullName: "Coffee Lab", Bio: "Specialty coffee and pastries",
		Followers: 800, Captions: []string{"new beans", "latte art"}})
	ctx := context.Background()
	if _, err := s.DB.Exec(ctx, `INSERT INTO discovery_pages (kind, key) VALUES ('location', '1000001')`); err != nil {
		t.Fatal(err)
	}
	s.StartScheduler()
	status := func(u string) string {
		var st string
		_ = s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = $1`, u).Scan(&st)
		return st
	}
	want := map[string]string{"basra_lens": "registered", "coffee_lab": "below"}
	loaded := func() int {
		return count(t, s, `SELECT count(*) FROM discovery_pages WHERE loads > 0 AND ((kind = 'location' AND key = '1000001')
			OR (kind = 'hashtag' AND key = 'baghdad'))`)
	}
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		got, ok := map[string]string{}, loaded() == 2
		for u, w := range want {
			if got[u] = status(u); got[u] != w {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("candidates %v, want %v; %d of 2 pages loaded", got, want, loaded())
		}
	}
	// Both pages were loaded, each showing the two accounts.
	for _, p := range []struct{ kind, key string }{{"location", "1000001"}, {"hashtag", "baghdad"}} {
		var loads, accounts int
		var class string
		if err := s.DB.QueryRow(ctx, `SELECT loads, coalesce(last_class, ''), coalesce(last_accounts, 0) FROM discovery_pages
			WHERE kind = $1 AND key = $2`, p.kind, p.key).Scan(&loads, &class, &accounts); err != nil {
			t.Fatalf("%s page: %v", p.kind, err)
		}
		if loads < 1 || class != "loaded" || accounts != 2 {
			t.Errorf("%s page: %d loads, last %q with %d accounts, want loaded with 2", p.kind, loads, class, accounts)
		}
	}
	// Candidates seen in Iraq, by origin; vetted as usual.
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE username IN ('basra_lens', 'coffee_lab')
		AND iraq_context > 0 AND 'place' = ANY(origins) AND 'hashtag' = ANY(origins) AND NOT 'network' = ANY(origins)`); n != 2 {
		t.Errorf("%d of 2 candidates seen in Iraq with origins place and hashtag only", n)
	}
	if n := count(t, s, `SELECT count(*) FROM profile_schedule WHERE profile_ref = 'u:basra_lens'`); n != 1 {
		t.Errorf("basra_lens on the schedule %d times, want 1", n)
	}
	// Nothing of the page is kept: the account below has no profile or post.
	if n := count(t, s, `SELECT count(*) FROM profiles WHERE username = 'coffee_lab'`); n != 0 {
		t.Errorf("a page stored the profile of an account not registered")
	}
	if n := count(t, s, `SELECT count(*) FROM posts p JOIN discovery_candidates c ON c.user_pk = p.owner_pk WHERE c.username = 'coffee_lab'`); n != 0 {
		t.Errorf("a page stored %d posts of an account not registered", n)
	}
	// The page jobs are discovery jobs about the page, not an account.
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discovery' AND profile_ref LIKE 'x:%' AND status = 'completed'`); n < 2 {
		t.Errorf("%d completed page jobs, want 2 or more", n)
	}
}
