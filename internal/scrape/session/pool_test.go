package session

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

func TestBreakerRetiresIdleIdentitiesAndFeedsOutcomes(t *testing.T) {
	proxies, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://203.0.113.10:8000", "http://198.51.100.10:8000"}}},
		Breaker: breaker.Config{MinEvents: 2, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.facebook.com", Profile: "chrome_146", IdentityRPS: 1, IdentityBurst: 1,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 4}, proxies,
		fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, err := pool.Acquire(ctx, "isp")
	if err != nil || a.Endpoint.Bucket != "isp|203.0.113.0/24" {
		t.Fatalf("%+v %v", a.Endpoint, err)
	}
	pool.Release(a)

	// Outcomes recorded on an identity reach its bucket's breaker.
	b, _ := pool.Acquire(ctx, "isp")
	if b != a {
		t.Fatal("the idle identity is reused while its bucket is closed")
	}
	Outcome(b, detect.Transport)
	Outcome(b, detect.Transport)
	if !proxies.IsOpen(a.Endpoint) {
		t.Fatal("two transport failures of two must open the bucket")
	}
	pool.Release(b)

	c, err := pool.Acquire(ctx, "isp")
	if err != nil || c == a || c.Endpoint.Bucket != "isp|198.51.100.0/24" {
		t.Fatalf("want a fresh identity on the other /24: %+v %v", c.Endpoint, err)
	}
	if pool.live["isp"] != 1 {
		t.Fatalf("the identity on the open bucket must be retired: live=%d", pool.live["isp"])
	}
}

// An exit that stopped answering retires its identity; failures broken up
// by answers do not.
func TestTransportFailuresRetireAnIdentity(t *testing.T) {
	id := newIdentity("x", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, nil, 1, 1, 0)
	for range 20 {
		for range MaxTransportFailures - 1 {
			if retire, _ := id.Record(detect.Transport); retire {
				t.Fatal("retired on failures broken up by answers")
			}
		}
		id.Record(detect.OK)
	}
	for i := 1; i <= MaxTransportFailures; i++ {
		retire, reason := id.Record(detect.Transport)
		if retire != (i == MaxTransportFailures) || (retire && reason != "transport") {
			t.Fatalf("failure %d: retire=%v reason=%q", i, retire, reason)
		}
	}
	if id.RetireReason() != "transport" {
		t.Fatalf("reason %q", id.RetireReason())
	}
}

// doer fails every request with the error fail returns.
type doer struct {
	transport.Doer
	fail func() error
}

func (d doer) Do(context.Context, *transport.Request) (*transport.Response, error) {
	return nil, d.fail()
}

// A load that got no response is marked ErrTransport and counts against the
// exit; one cancelled by its caller does not.
func TestCancelledLoadsDoNotRetireAnIdentity(t *testing.T) {
	base, _ := url.Parse("https://www.facebook.com")
	l := &Loader{Rules: detect.DefaultRules(), Hooks: Hooks{Extract: func([]byte) (Tokens, bool) { return nil, false }}}
	cancelled := newIdentity("c", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	dead := newIdentity("d", proxy.Endpoint{Tier: "isp"}, Fingerprint{}, nil, base, 1000, 10, 0)
	std := transport.NewStd(time.Second)
	for range MaxTransportFailures {
		ctx, cancel := context.WithCancel(context.Background())
		cancelled.Doer = doer{Doer: std, fail: func() error { cancel(); return ctx.Err() }}
		if _, err := l.Load(ctx, cancelled, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("cancelled load: %v", err)
		}
		dead.Doer = doer{Doer: std, fail: func() error { return errors.New("awaiting headers") }}
		if _, err := l.Load(context.Background(), dead, "https://www.facebook.com/x"); !errors.Is(err, ErrTransport) {
			t.Fatalf("dead load: %v", err)
		}
	}
	if r := cancelled.RetireReason(); r != "" {
		t.Fatalf("cancelled loads retired the identity: %q", r)
	}
	if r := dead.RetireReason(); r != "transport" {
		t.Fatalf("dead exit: %q", r)
	}
}

// A rate-limited identity cools down for twice as long each time (30 s,
// 60 s, 2 min, 4 min) and retires at the fifth in a row; it used to retire
// at the second, so the cooldown never grew past 60 s. A good answer
// resets the count, and a login wall still retires at once.
func TestRateLimitCooldownGrows(t *testing.T) {
	id := &Identity{health: 1}
	var cooldowns []time.Duration
	for i := 1; i < MaxRateLimits; i++ {
		before := time.Now()
		if retire, reason := id.Record(detect.RateLimited); retire {
			t.Fatalf("retired at rate limit %d: %s", i, reason)
		}
		cooldowns = append(cooldowns, id.cooldown.Sub(before).Round(time.Second))
	}
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute}
	for i := range want {
		if cooldowns[i] != want[i] {
			t.Fatalf("cooldowns %v, want %v", cooldowns, want)
		}
	}
	if retire, reason := id.Record(detect.RateLimited); !retire || reason != "blocked" {
		t.Fatalf("fifth rate limit: %v %s", retire, reason)
	}
	ok := &Identity{health: 1}
	ok.Record(detect.RateLimited)
	ok.Record(detect.OK)
	ok.Record(detect.RateLimited)
	if d := time.Until(ok.cooldown).Round(time.Second); d != 30*time.Second {
		t.Fatalf("a good answer resets the count: cooldown %s", d)
	}
	lw := &Identity{health: 1}
	if retire, reason := lw.Record(detect.LoginWall); !retire || reason != "login_wall" {
		t.Fatalf("login wall: %v %s", retire, reason)
	}
}
