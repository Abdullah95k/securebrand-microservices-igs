package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
)

// A rest one pool takes (one crawler pod) reaches every pool on the same
// Valkey, until the same end; other exits stay in use, a later end wins,
// and ended rests are left alone.
func TestExitRestsAreSharedBetweenPools(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.Proxy{
		Tiers:   []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://203.0.113.10:8000", "http://203.0.113.11:8000"}}},
		Routing: map[string][]string{"default": {"isp"}},
	}
	pools := make([]*proxy.Pool, 2)
	stores := make([]*ExitRests, 2)
	for i := range pools {
		p, err := proxy.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = NewExitRests(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "")
		p.ShareRests(stores[i])
		pools[i] = p
	}
	a, b := pools[0], pools[1]
	ctx := context.Background()
	ep := proxy.Endpoint{Tier: "isp", URL: "http://203.0.113.10:8000"}
	other := proxy.Endpoint{Tier: "isp", URL: "http://203.0.113.11:8000"}

	a.Rest(ep, 10*time.Minute)
	waitOpen(t, b, ep)
	if b.IsOpen(other) {
		t.Fatal("a rest of one exit rested another")
	}
	// The end is the same, within the publication's rounding.
	got, _ := stores[1].Load(ctx)
	if until := got[ep.Exit()]; until.Before(time.Now().Add(9*time.Minute)) || until.After(time.Now().Add(11*time.Minute)) {
		t.Fatalf("shared rest ends %v", until)
	}

	// A longer rest taken by b wins over a's shorter one; a shorter one
	// published later does not cut it.
	b.Rest(other, time.Hour)
	waitOpen(t, a, other)
	if err := stores[0].Publish(ctx, other.Exit(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, _ = stores[0].Load(ctx)
	if got[other.Exit()].Before(time.Now().Add(50 * time.Minute)) {
		t.Fatal("a shorter rest replaced a longer one")
	}

	// An ended rest is not applied, and is dropped at the next publication.
	fresh, _ := proxy.New(cfg)
	fresh.ShareRests(stores[0])
	if err := stores[0].Publish(ctx, "isp#1", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := fresh.PullRests(ctx); err != nil {
		t.Fatal(err)
	}
	if fresh.IsOpen(proxy.Endpoint{Tier: "isp", URL: "http://ended:1"}) {
		t.Fatal("an ended rest was applied")
	}
	if err := stores[0].Publish(ctx, other.Exit(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, _ = stores[0].Load(ctx)
	if _, ok := got["isp#1"]; ok {
		t.Fatal("an ended rest was kept")
	}
}

func waitOpen(t *testing.T, p *proxy.Pool, ep proxy.Endpoint) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if err := p.PullRests(context.Background()); err != nil {
			t.Fatal(err)
		}
		if p.IsOpen(ep) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s does not rest in the other pool", ep.Exit())
		}
	}
}

// Parked identities are taken once each, per exit.
func TestParkedIdentitiesTakenOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	s := NewParkedIdentities(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "")
	ctx := context.Background()
	if b, err := s.Take(ctx, "isp#2"); b != nil || err != nil {
		t.Fatalf("empty store gave %q %v", b, err)
	}
	for _, v := range []string{"one", "two"} {
		if err := s.Put(ctx, "isp#2", []byte(v), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := s.Take(ctx, "isp#2")
	b, _ := s.Take(ctx, "isp#2")
	c, _ := s.Take(ctx, "isp#2")
	if string(a) != "one" || string(b) != "two" || c != nil {
		t.Fatalf("took %q %q %q", a, b, c)
	}
	if other, _ := s.Take(ctx, "isp#3"); other != nil {
		t.Fatal("another exit's identity was taken")
	}
}
