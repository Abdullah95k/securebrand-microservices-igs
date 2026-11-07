package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
)

// Discovery takes at most its share of each proxy exit (D-027,
// ig.discovery.exit_share): a vet's requests wait for discovery's share of
// their exit before the exit itself, and count in
// igs_exit_share_requests_total{class="discovery"}. A regular crawl on the
// same exits waits for the exit alone. (The share's rate, fleet-wide
// across pods, is TestExitShareFleetWide in internal/scrape/session.)
func TestDiscoveryExitShare(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	s := Start(t, Options{Spec: sp, Exits: 2, Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags"}, Threshold: 4, MinPriority: 2, VetInFlight: 10,
			Interval: time.Hour, FlushEvery: time.Second, ExitShare: 0.1}
	}})
	ctx := context.Background()
	share := func() float64 {
		return counter("igs_exit_share_requests_total", map[string]string{"class": "discovery"})
	}
	before := share()

	// A regular crawl: none of its requests is discovery's.
	s.Wait(t, 2*time.Minute, s.Crawl(t, s.World.Profiles()[0].Username, full, s.Limits(), nil))
	if got := share(); got != before {
		t.Fatalf("a regular crawl sent %v requests as discovery's", got-before)
	}

	// A vet: its requests are discovery's.
	s.World.AddProfile(fakeig.ProfileSpec{Username: "share_01", FullName: "Share", Bio: "a candidate", Followers: 100,
		Captions: []string{"hello"}})
	if _, err := s.DB.Exec(ctx, `INSERT INTO discovery_candidates (username, tags, priority) VALUES ('share_01', 3, 10)`); err != nil {
		t.Fatal(err)
	}
	v := &discovery.Vetter{DB: s.DB, Dispatch: s.Dispatch, Cfg: s.Cfg}
	if err := v.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var vet string
	if err := s.DB.QueryRow(ctx, `SELECT job_id::text FROM jobs WHERE kind = 'discovery'`).Scan(&vet); err != nil {
		t.Fatalf("no vet started: %v", err)
	}
	s.Wait(t, 2*time.Minute, vet)
	if got := share() - before; got < 1 {
		t.Errorf("the vet sent %v requests as discovery's, want at least 1", got)
	}
	if w := counter("igs_exit_share_wait_seconds_total", map[string]string{"class": "discovery"}); w <= 0 {
		t.Errorf("igs_exit_share_wait_seconds_total{class=discovery} %v, want the vet's waits", w)
	}
}
