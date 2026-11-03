package proxy

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

func testPool(t *testing.T) *Pool {
	t.Helper()
	p, err := New(config.Proxy{
		Tiers: []config.ProxyTier{
			{Name: "isp", Kind: "static_list", Endpoints: []string{
				"http://u:p@203.0.113.10:8000", "http://u:p@203.0.113.11:8000", "http://u:p@198.51.100.7:8000"}},
			{Name: "res", Kind: "gateway", Gateway: "http://gw.example:7777", Username: "u", Password: "p", Country: "us"},
		},
		Routing: map[string][]string{"default": {"isp", "res"}},
		Breaker: breaker.Config{MinEvents: 4, OpenRate: 0.5, Cooldown: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func block(p *Pool, ep Endpoint, n int) {
	for i := 0; i < n; i++ {
		p.Observe(ep, detect.RateLimited)
	}
}

func TestBuckets(t *testing.T) {
	for raw, want := range map[string]string{
		"http://u:p@203.0.113.10:8000":  "isp|203.0.113.0/24",
		"http://proxy.example:8000":     "isp|proxy.example:8000",
		"http://u:p@proxy.example:8001": "isp|proxy.example:8001",
		"http://proxy.example":          "isp|proxy.example",
		"socks5://[2001:db8::1]:1080":   "isp|2001:db8::/48",
	} {
		if got := staticBucket("isp", raw); got != want {
			t.Errorf("staticBucket(%q) = %q, want %q", raw, got, want)
		}
	}
	p := testPool(t)
	ep, err := p.Next("res")
	if err != nil || ep.Bucket != "res|gw.example:7777|us" {
		t.Fatalf("%+v %v", ep, err)
	}
	if strings.Contains(ep.Bucket, "u:p") {
		t.Fatal("buckets must not carry credentials")
	}
}

func TestStaticListSkipsOpenBuckets(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("isp")
	if ep.Bucket != "isp|203.0.113.0/24" {
		t.Fatalf("first endpoint: %+v", ep)
	}
	block(p, ep, 4)
	if !p.IsOpen(ep) || !p.Available("isp") {
		t.Fatal("one bucket open, the tier still has another")
	}
	for i := 0; i < 5; i++ {
		next, err := p.Next("isp")
		if err != nil || next.Bucket != "isp|198.51.100.0/24" {
			t.Fatalf("Next must skip the open /24: %+v %v", next, err)
		}
	}
	other, _ := p.Next("isp")
	block(p, other, 4)
	if p.Available("isp") {
		t.Fatal("every bucket open")
	}
	if _, err := p.Next("isp"); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("want ErrTierOpen, got %v", err)
	}
}

func TestGatewayBreaker(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("res")
	for i := 0; i < 4; i++ {
		p.Observe(ep, detect.OK)
	}
	p.Observe(ep, detect.Transport)
	p.Observe(ep, detect.LoginWall)
	p.Observe(ep, detect.Checkpoint)
	if p.IsOpen(ep) {
		t.Fatal("3 of 7 is below 50%")
	}
	p.Observe(ep, detect.RateLimited)
	if !p.IsOpen(ep) || p.Available("res") {
		t.Fatal("4 of 8 opens the gateway bucket")
	}
	if _, err := p.Next("res"); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("want ErrTierOpen, got %v", err)
	}
	// Direct never opens.
	d, err := p.Next("direct")
	if err != nil || d.Bucket != "" || !p.Available("direct") {
		t.Fatal(d, err)
	}
	block(p, d, 10)
	if p.IsOpen(d) {
		t.Fatal("direct has no breaker")
	}
}

func TestNonExitClassesCountAsSuccess(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("res")
	for _, c := range []detect.Class{detect.SchemaDrift, detect.RegistryStale, detect.SoftError, detect.ContentUnavailable,
		detect.SchemaDrift, detect.RegistryStale, detect.SoftError, detect.ContentUnavailable} {
		p.Observe(ep, c)
	}
	if p.IsOpen(ep) {
		t.Fatal("parser and registry problems are not the exit's fault")
	}
}

func TestRestSkipsAnExit(t *testing.T) {
	p := testPool(t)
	ep, _ := p.Next("isp")
	p.Rest(ep, 30*time.Minute)
	if !p.IsOpen(ep) || !p.Available("isp") {
		t.Fatal("a rested exit is open; the tier still has another")
	}
	for i := 0; i < 4; i++ {
		if next, err := p.Next("isp"); err != nil || next.Exit() == ep.Exit() {
			t.Fatalf("Next must skip the rested exit: %+v %v", next, err)
		}
	}
	// The rest is the exit's alone: the other exit of its /24 carries on.
	if p.breakers.IsOpen(ep.Bucket) || !p.Available("isp") {
		t.Fatal("a rest must not cool down the exit's bucket")
	}
	p.Rest(Endpoint{Tier: "direct"}, time.Minute) // no bucket: nothing to rest
	if !p.Available("direct") {
		t.Fatal("direct cannot rest")
	}
}

// An exit is one IP: a static endpoint's host and port (two in one /24
// share a bucket, not an exit), a gateway session; direct egress has none.
func TestExits(t *testing.T) {
	p := testPool(t)
	a, _ := p.Next("isp")
	b, _ := p.Next("isp")
	if a.Bucket != b.Bucket || a.Exit() == b.Exit() || a.Exit() != "isp|203.0.113.10:8000" {
		t.Fatalf("%q %q", a.Exit(), b.Exit())
	}
	g1, _ := p.Next("res")
	g2, _ := p.Next("res")
	if g1.Exit() == g2.Exit() || !strings.HasPrefix(g1.Exit(), "res|") || strings.Contains(g1.Exit(), "u:p") {
		t.Fatalf("gateway sessions are their own exits: %q %q", g1.Exit(), g2.Exit())
	}
	if d, _ := p.Next("direct"); d.Exit() != "" {
		t.Fatalf("direct: %q", d.Exit())
	}
}

// NextWhere skips the endpoints the caller names, tells "all skipped" from
// "all cooling", and never spends a half-open probe on a skipped exit.
func TestNextWhereSkips(t *testing.T) {
	p := testPool(t)
	busy := map[string]bool{"isp|203.0.113.10:8000": true, "isp|203.0.113.11:8000": true}
	skip := func(ep Endpoint) bool { return busy[ep.Exit()] }
	for range 4 {
		ep, err := p.NextWhere("isp", skip)
		if err != nil || ep.Exit() != "isp|198.51.100.7:8000" {
			t.Fatalf("%+v %v", ep, err)
		}
	}
	busy["isp|198.51.100.7:8000"] = true
	if _, err := p.NextWhere("isp", skip); !errors.Is(err, ErrNoFreeExit) {
		t.Fatalf("every exit skipped: %v", err)
	}
	for _, ep := range p.tiers["isp"].Static() {
		p.Rest(ep, time.Hour)
	}
	if p.Available("isp") {
		t.Fatal("every exit rests")
	}
	if _, err := p.NextWhere("isp", nil); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("every bucket rests: %v", err)
	}
	if _, err := p.NextWhere("isp", skip); !errors.Is(err, ErrTierOpen) {
		t.Fatalf("every bucket rests, whatever is skipped: %v", err)
	}
}

// A rested exit is probed by one identity once its rest ends; a probe that
// is blocked again rests it twice as long, and good answers put it back.
func TestRestedExitIsProbed(t *testing.T) {
	p, err := New(config.Proxy{
		Tiers:   []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://isp.example:10001", "http://isp.example:10002"}}},
		Breaker: breaker.Config{Cooldown: time.Minute, MaxCooldown: time.Hour, ProbeSuccesses: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	clk := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p.rests.SetClock(func() time.Time { return clk })
	a, _ := p.Next("isp")
	p.Rest(a, 10*time.Minute)
	for range 3 {
		if ep, _ := p.Next("isp"); ep.Exit() == a.Exit() {
			t.Fatal("resting")
		}
	}
	clk = clk.Add(10 * time.Minute)
	probes := 0
	for range 4 {
		if ep, _ := p.Next("isp"); ep.Exit() == a.Exit() {
			probes++
		}
	}
	if probes != 1 {
		t.Fatalf("one identity probes the exit after its rest, got %d", probes)
	}
	p.Observe(a, detect.RateLimited)
	clk = clk.Add(20*time.Minute - time.Second)
	if !p.IsOpen(a) {
		t.Fatal("a blocked probe rests the exit twice as long")
	}
	clk = clk.Add(time.Second)
	if p.IsOpen(a) {
		t.Fatal("rested 20 minutes")
	}
	p.Observe(a, detect.OK)
	p.Observe(a, detect.OK)
	for range 4 {
		if _, err := p.NextWhere("isp", func(ep Endpoint) bool { return ep.Exit() != a.Exit() }); err != nil {
			t.Fatalf("good answers put the exit back in use: %v", err)
		}
	}
}
