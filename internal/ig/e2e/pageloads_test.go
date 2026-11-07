package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
)

// With ig.exit_page_loads_per_day, the fleet counts each exit's page loads
// (profile and post pages) for the UTC day, and an exit that reaches the
// limit rests until the day ends; its tasks move to other exits. Every
// page load is also counted per exit in igs_exit_page_loads_total.
func TestExitPageLoadLimit(t *testing.T) {
	const limit = 4
	s := Start(t, Options{Spec: fakeig.DefaultSpec(), Exits: 3, Config: func(c *config.Config) {
		c.IG.ExitPageLoadsPerDay = limit
	}})
	ctx := context.Background()
	before := map[string]float64{}
	for _, x := range []string{"isp#1", "isp#2", "isp#3"} {
		before[x] = counter("igs_exit_page_loads_total", map[string]string{"tier": "isp", "exit": x})
	}
	// 40 posts, each a post page: far more page loads than the 3 exits
	// may make today.
	p := s.World.Profiles()[0]
	s.Crawl(t, p.Username, full, s.Limits(), nil)

	loads := ratelimit.NewPageLoads(s.Redis, s.Cfg.Kafka.TopicPrefix)
	rests := ratelimit.NewExitRests(s.Redis, s.Cfg.Kafka.TopicPrefix)
	endOfDay := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		r, err := rests.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rested := 0
		for _, x := range []string{"isp#1", "isp#2", "isp#3"} {
			if until, ok := r[x]; ok && until.After(endOfDay.Add(-time.Minute)) {
				rested++
			}
		}
		if rested == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of 3 exits rest until the day ends (rests %v)", rested, r)
		}
		time.Sleep(200 * time.Millisecond)
	}
	day, err := loads.Day(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Pods learn of a rest within ig.exit_rest_sync, and loads already
	// under way finish: a few over the limit, not dozens.
	for _, x := range []string{"isp#1", "isp#2", "isp#3"} {
		if n := day[x]; n < limit || n > limit+6 {
			t.Errorf("%s: %d page loads today, want about %d", x, n, limit)
		}
		if d := counter("igs_exit_page_loads_total", map[string]string{"tier": "isp", "exit": x}) - before[x]; d != float64(day[x]) {
			t.Errorf("%s: igs_exit_page_loads_total grew %v, the fleet counted %d", x, d, day[x])
		}
	}
	if n := counter("igs_exit_rests_total", map[string]string{"tier": "isp", "reason": "page_load_limit"}); n < 3 {
		t.Errorf("%v exit rests for the page-load limit, want at least 3", n)
	}
}

// counter reads a counter's child with exactly labels (0 if none).
func counter(name string, labels map[string]string) float64 {
	v, _, _ := sample(name, labels)
	return v
}
