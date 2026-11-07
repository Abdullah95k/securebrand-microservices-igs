package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/soak"
)

// stackChaos breaks the e2e stack as the soak's Kube breaks the cluster: a
// crawler restarts, Valkey loses its data.
type stackChaos struct {
	t *testing.T
	s *Stack
}

func (c stackChaos) KillCrawler(context.Context) (string, error) {
	c.s.RestartCrawler(c.t)
	return "crawler", nil
}

func (c stackChaos) RestartValkey(ctx context.Context) error { return c.s.Redis.FlushDB(ctx).Err() }

// The soak's driver (cmd/igs-soak, REQ-008 fix 12), for 40 seconds instead
// of 24 hours: it registers fakeig's profiles through the API, keeps them
// on a 10-second schedule while fakeig's world changes, a crawler restarts
// and Valkey loses its data, then reads every profile in full and finds
// the store equal to fakeig's truth.
func TestSoakDriver(t *testing.T) {
	sp := spec()
	sp.Seed = 77
	s := Start(t, Options{Spec: sp, Faults: fakeig.Faults{RateLimitEvery: 37}, CrawlerDrain: 50 * time.Millisecond,
		Config: func(c *config.Config) { c.IG.Schedule.PostRefreshPerRun = 10 }})
	s.StartScheduler()
	rep, err := soak.Run(context.Background(), soak.Options{DB: s.DB, API: s.API.URL, FakeIG: s.Cfg.IG.BaseURL,
		Duration: 40 * time.Second, Interval: 10 * time.Second, MutateEvery: 300 * time.Millisecond, SampleEvery: 5 * time.Second,
		KillEvery: 15 * time.Second, ValkeyAt: 20 * time.Second, Chaos: stackChaos{t, s},
		Settle: 2 * time.Minute, FinalWait: 3 * time.Minute, MaxAge: time.Duration(s.Limits().MaxAgeSeconds) * time.Second,
		Log: os.Stdout})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range rep.Lines() {
		t.Log(l)
	}
	if !rep.Pass() || rep.Profiles != len(s.World.Profiles()) || len(rep.Mutations) == 0 ||
		rep.Chaos["crawler pod killed"] == 0 || rep.Chaos["valkey restarted"] != 1 || rep.Posts == 0 {
		t.Fatalf("soak report: pass %v, profiles %d, changes %v, chaos %v, posts %d", rep.Pass(), rep.Profiles, rep.Mutations,
			rep.Chaos, rep.Posts)
	}
}
