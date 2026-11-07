package session

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
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
	if err != nil || a.Endpoint.Bucket != "isp/b1" {
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
	if err != nil || c == a || c.Endpoint.Bucket != "isp/b2" {
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

type pacer struct {
	mu    sync.Mutex
	keys  []string
	rates []float64
}

// WaitRate records the key and rate. An exit's own key must carry the
// pool's ExitRPS.
func (p *pacer) WaitRate(_ context.Context, key string, rps float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !strings.HasSuffix(key, ":"+ShareDiscovery) && rps != 0.25 {
		return fmt.Errorf("rps %v", rps)
	}
	p.keys = append(p.keys, key)
	p.rates = append(p.rates, rps)
	return nil
}

// ispPool is a pool on three ISP exits named by port on one host, as some
// providers name theirs, resting an exit 10 minutes on a rate limit and 30
// on a checkpoint, and pacing each exit at 0.25 requests a second, a tenth
// of it for discovery.
func ispPool(t *testing.T, maxPerTier int) (*Pool, *proxy.Pool, *pacer) {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://isp.example:10001", "http://isp.example:10002", "http://isp.example:10003"}}},
		Breaker: breaker.Config{Cooldown: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pc := &pacer{}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.instagram.com", Profile: "chrome_146", IdentityRPS: 1000, IdentityBurst: 10,
		MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: maxPerTier, ExitRPS: 0.25, ExitPacer: pc,
		Shares: map[string]float64{ShareDiscovery: 0.1},
		Rest:   map[detect.Class]time.Duration{detect.RateLimited: 10 * time.Minute, detect.Checkpoint: 30 * time.Minute}},
		proxies, fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool, proxies, pc
}

func exits(ids ...*Identity) []string {
	var out []string
	for _, id := range ids {
		out = append(out, id.Endpoint.Exit())
	}
	slices.Sort(out)
	return out
}

// Concurrent tasks get identities on different exits; a released identity
// on an exit no task uses is reused rather than a new one bootstrapped;
// with every exit in use, an idle identity is reused before a fourth is
// created.
func TestIdentitiesSpreadOverExits(t *testing.T) {
	pool, _, _ := ispPool(t, 8)
	ctx := context.Background()
	a, _ := pool.Acquire(ctx, "isp")
	b, _ := pool.Acquire(ctx, "isp")
	c, err := pool.Acquire(ctx, "isp")
	if err != nil || len(slices.Compact(exits(a, b, c))) != 3 {
		t.Fatalf("three tasks, three exits: %v %v", exits(a, b, c), err)
	}
	pool.Release(b)
	if again, _ := pool.Acquire(ctx, "isp"); again != b {
		t.Fatal("the idle identity on the free exit must be reused")
	}
	pool.Release(a)
	if again, _ := pool.Acquire(ctx, "isp"); again != a {
		t.Fatal("idle identity first")
	}
	// Every exit busy: a fourth identity doubles up on an exit.
	d, err := pool.Acquire(ctx, "isp")
	if err != nil || pool.live["isp"] != 4 {
		t.Fatalf("a fourth identity: live %d %v", pool.live["isp"], err)
	}
	pool.Release(d)
	pool.Release(c)
	// Only a is in use now: the identity taken is not on a's exit.
	if again, _ := pool.Acquire(ctx, "isp"); again.Endpoint.Exit() == a.Endpoint.Exit() {
		t.Fatal("an identity on a free exit comes first")
	}
	if pool.live["isp"] != 4 {
		t.Fatalf("no new identity while one is idle: live %d", pool.live["isp"])
	}
}

// A rate limit rests its exit (Instagram limits per IP): identities idle
// on it retire, new ones go elsewhere, and the rest of its /24 is not
// touched. A checkpoint rests it longer. With every exit resting, Acquire
// fails at once with ErrTierOpen rather than waiting.
func TestBlocksRestTheExit(t *testing.T) {
	pool, proxies, _ := ispPool(t, 8)
	ctx := context.Background()
	a, _ := pool.Acquire(ctx, "isp")
	b, _ := pool.Acquire(ctx, "isp")
	Outcome(a, detect.RateLimited)
	if !proxies.IsOpen(a.Endpoint) || proxies.IsOpen(b.Endpoint) {
		t.Fatal("a rate limit rests its exit alone")
	}
	pool.Release(a) // cooling, and on a resting exit
	for range 3 {
		id, err := pool.Acquire(ctx, "isp")
		if err != nil || id.Endpoint.Exit() == a.Endpoint.Exit() {
			t.Fatalf("no identity on the resting exit: %v %v", id.Endpoint.Exit(), err)
		}
		pool.Release(id)
	}
	if pool.live["isp"] != 2 {
		t.Fatalf("the idle identity on the resting exit retires (b and one new identity left): live %d", pool.live["isp"])
	}
	Outcome(b, detect.Checkpoint)
	pool.Release(b)
	c, err := pool.Acquire(ctx, "isp")
	if err != nil || c.Endpoint.Exit() == a.Endpoint.Exit() || c.Endpoint.Exit() == b.Endpoint.Exit() {
		t.Fatalf("one exit left: %v %v", c.Endpoint.Exit(), err)
	}
	Outcome(c, detect.RateLimited)
	pool.Release(c)
	done := make(chan error, 1)
	go func() { _, err := pool.Acquire(ctx, "isp"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, proxy.ErrTierOpen) {
			t.Fatalf("every exit resting: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire waited with every exit resting")
	}
}

// AcquireElsewhere never returns an identity on the exit to avoid, and
// fails with ErrNoFreeExit when that is the only usable one.
func TestAcquireElsewhere(t *testing.T) {
	pool, proxies, _ := ispPool(t, 8)
	ctx := context.Background()
	a, _ := pool.Acquire(ctx, "isp")
	pool.Release(a)
	for range 4 {
		id, err := pool.AcquireElsewhere(ctx, "isp", a.Endpoint.Exit())
		if err != nil || id.Endpoint.Exit() == a.Endpoint.Exit() {
			t.Fatalf("%v %v", id.Endpoint.Exit(), err)
		}
		pool.Release(id)
	}
	for i, ep := range []string{"http://isp.example:10002", "http://isp.example:10003"} {
		e := proxy.Endpoint{Tier: "isp", URL: ep, Index: i + 2, Bucket: fmt.Sprintf("isp/b%d", i+2)}
		proxies.Rest(e, time.Hour)
	}
	if _, err := pool.AcquireElsewhere(ctx, "isp", a.Endpoint.Exit()); !errors.Is(err, proxy.ErrNoFreeExit) {
		t.Fatalf("only the avoided exit is usable: %v", err)
	}
	if id, err := pool.Acquire(ctx, "isp"); err != nil || id != a {
		t.Fatalf("Acquire may still use it: %v", err)
	}
}

// Every request waits on its exit's fleet-wide pace; direct egress has
// none.
func TestExitPacing(t *testing.T) {
	pool, _, pc := ispPool(t, 8)
	ctx := context.Background()
	a, _ := pool.Acquire(ctx, "isp")
	b, _ := pool.Acquire(ctx, "isp")
	for _, id := range []*Identity{a, a, b} {
		if err := id.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"exit:" + a.Endpoint.Exit(), "exit:" + a.Endpoint.Exit(), "exit:" + b.Endpoint.Exit()}
	if !slices.Equal(pc.keys, want) {
		t.Fatalf("paced %v, want %v", pc.keys, want)
	}
	d, err := pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Wait(ctx); err != nil || len(pc.keys) != 3 {
		t.Fatalf("direct egress is not paced as an exit: %v %v", pc.keys, err)
	}
}

// A request marked as discovery's waits for discovery's share of its exit
// (a tenth of the exit's rate, its own key) and then for the exit like any
// other; an unmarked request, or one of a class with no share, waits only
// for the exit. Direct egress is paced for neither.
func TestExitShare(t *testing.T) {
	pool, _, pc := ispPool(t, 8)
	ctx := context.Background()
	vet := WithShare(ctx, ShareDiscovery)
	a, _ := pool.Acquire(ctx, "isp")
	x := "exit:" + a.Endpoint.Exit()
	for _, c := range []context.Context{vet, ctx, WithShare(ctx, "other")} {
		if err := a.Wait(c); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{x + ":discovery", x, x, x}; !slices.Equal(pc.keys, want) {
		t.Fatalf("paced %v, want %v", pc.keys, want)
	}
	if want := []float64{0.025, 0.25, 0.25, 0.25}; !slices.EqualFunc(pc.rates, want, func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }) {
		t.Fatalf("rates %v, want %v", pc.rates, want)
	}
	d, err := pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Wait(vet); err != nil || len(pc.keys) != 4 {
		t.Fatalf("direct egress has no exit to share: %v %v", pc.keys, err)
	}
	// No share configured for discovery: its requests wait for the exit
	// alone.
	pool.cfg.Shares = nil
	b, _ := pool.Acquire(ctx, "isp")
	if err := b.Wait(vet); err != nil || pc.keys[len(pc.keys)-1] != "exit:"+b.Endpoint.Exit() || len(pc.keys) != 5 {
		t.Fatalf("no share: %v %v", pc.keys, err)
	}
}

// Once its exit rests, an identity sends nothing more: Wait returns
// ErrExitResting, and a page load reports the exit's rate limit unsent.
func TestRestingExitSendsNothing(t *testing.T) {
	pool, proxies, _ := ispPool(t, 8)
	ctx := context.Background()
	a, _ := pool.Acquire(ctx, "isp")
	b, _ := pool.Acquire(ctx, "isp")
	if err := a.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	proxies.Rest(a.Endpoint, time.Minute)
	if err := a.Wait(ctx); !errors.Is(err, ErrExitResting) {
		t.Fatalf("a: %v", err)
	}
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("b's exit does not rest: %v", err)
	}
	sent := false
	a.Doer = doer{Doer: transport.NewStd(time.Second), fail: func() error { sent = true; return errors.New("sent") }}
	l := &Loader{Rules: detect.DefaultRules(), Hooks: Hooks{Extract: func([]byte) (Tokens, bool) { return nil, false }}}
	pl, err := l.Load(ctx, a, "https://www.instagram.com/x/")
	if err != nil || sent || pl.Class != detect.RateLimited {
		t.Fatalf("load on a resting exit: %+v %v sent=%v", pl, err, sent)
	}
}
