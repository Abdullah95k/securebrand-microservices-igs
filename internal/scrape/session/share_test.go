package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Discovery's share of an exit holds fleet-wide, through Valkey: two pools
// (two crawler pods) on one exit at 20 requests a second, discovery's share
// a tenth of it. Over two seconds, discovery's requests from both pods
// together keep to about 2 a second, while regular requests sent alongside
// take the rest of the exit.
func TestExitShareFleetWide(t *testing.T) {
	rdb := testenv.Redis(t)
	pacer := ratelimit.NewRedis(rdb)
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pod := func() *Identity {
		t.Helper()
		proxies, err := proxy.New(config.Proxy{Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://isp.example:10001"}}}})
		if err != nil {
			t.Fatal(err)
		}
		pool, err := NewPool(PoolConfig{BaseURL: "https://www.instagram.com", Profile: "chrome_146", IdentityRPS: 1000,
			IdentityBurst: 10, MaxRequestsMin: 100000, MaxRequestsMax: 100001, MaxPerTier: 4, ExitRPS: 20, ExitPacer: pacer,
			Shares: map[string]float64{ShareDiscovery: 0.1}},
			proxies, fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
		if err != nil {
			t.Fatal(err)
		}
		id, err := pool.Acquire(context.Background(), "isp")
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a, b, c := pod(), pod(), pod()
	if a.Endpoint.Exit() != b.Endpoint.Exit() || a.Endpoint.Exit() != c.Endpoint.Exit() {
		t.Fatalf("one exit: %s %s %s", a.Endpoint.Exit(), b.Endpoint.Exit(), c.Endpoint.Exit())
	}

	const window = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	vet := WithShare(ctx, ShareDiscovery)
	var vets, regular atomic.Int64
	var wg sync.WaitGroup
	send := func(id *Identity, ctx context.Context, n *atomic.Int64) {
		defer wg.Done()
		for id.Wait(ctx) == nil {
			n.Add(1)
		}
	}
	wg.Add(5)
	go send(a, vet, &vets)
	go send(a, vet, &vets) // two vets at once on one pod
	go send(b, vet, &vets)
	go send(c, ctx, &regular)
	go send(c, ctx, &regular)
	wg.Wait()

	// 2 a second over 2 s, the first at once: at most 5, and 1 more for
	// timing.
	if n := vets.Load(); n > 6 || n < 2 {
		t.Errorf("%d discovery requests in %s across two pods, want 2 to 6 (a tenth of 20 a second)", n, window)
	}
	// The exit allows about 40; discovery took at most 6 of them.
	if n := regular.Load(); n < 20 {
		t.Errorf("%d regular requests in %s beside discovery's, want at least 20 of the exit's 40", n, window)
	}
	t.Logf("in %s: %d discovery, %d regular", window, vets.Load(), regular.Load())
}
