package graphql

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// With Valkey down, no request reaches the platform: neither the per-exit
// pacing nor the fleet-wide query limit lets one through, so a crawler
// waits rather than run without the per-exit limit (REQ-008 fix 4).
func TestNoRequestWithoutValkey(t *testing.T) {
	mr := miniredis.RunT(t)
	valkey := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	limiter := ratelimit.NewRedis(valkey)
	mr.Close() // Valkey down from here on

	var reqs []*transport.Request
	proxies, err := proxy.New(config.Proxy{Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
		Endpoints: []string{"http://u:p@203.0.113.10:8000"}}}})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		IdentityRPS: 1000, IdentityBurst: 1000, MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 1,
		ExitRPS: 1000, ExitPacer: limiter}, proxies, fps,
		func(transport.Options) (transport.Doer, error) { return recorder{reqs: &reqs}, nil })
	if err != nil {
		t.Fatal(err)
	}
	exitID, err := pool.Acquire(context.Background(), "isp")
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.NewStatic(registry.Seed{Queries: []registry.Query{
		{Name: "q", DocID: "1", Variables: json.RawMessage(`{}`), Status: registry.StatusActive}}})

	for name, c := range map[string]struct {
		client *Client
		id     *session.Identity
	}{
		// The exit's fleet-wide pace (session.PoolConfig.ExitPacer).
		"exit pacing": {&Client{Rules: detect.DefaultRules(), Builder: stubBuilder{}, Registry: reg}, exitID},
		// The query limits (Client.Limiter), on an identity with no exit.
		"query limit": {&Client{Rules: detect.DefaultRules(), Builder: stubBuilder{}, Registry: reg, Limiter: limiter, GlobalRPS: 1000},
			testIdentity(t, &reqs)},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := c.client.Do(ctx, c.id, Request{Query: "q"})
		cancel()
		if err == nil {
			t.Errorf("%s: a request went through with Valkey down: %+v", name, resp)
		}
	}
	if len(reqs) != 0 {
		t.Fatalf("%d requests reached the platform with Valkey down", len(reqs))
	}
}
