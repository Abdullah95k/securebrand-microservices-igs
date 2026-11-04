package session

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

type memStore struct {
	mu sync.Mutex
	m  map[string][][]byte
}

func (s *memStore) Put(_ context.Context, exit string, b []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[exit] = append(s.m[exit], b)
	return nil
}

func (s *memStore) Take(_ context.Context, exit string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.m[exit]
	if len(l) == 0 {
		return nil, nil
	}
	s.m[exit] = l[1:]
	return l[0], nil
}

func parkPool(t *testing.T, store IdentityStore) (*Pool, *proxy.Pool) {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{
		Tiers:   []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: []string{"http://user:pass@203.0.113.10:8000"}}},
		Routing: map[string][]string{"default": {"isp"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{BaseURL: "https://www.instagram.com", Profiles: []string{"chrome_146", "chrome_152"},
		IdentityRPS: 1, IdentityBurst: 1, MaxRequestsMin: 500, MaxRequestsMax: 501, MaxPerTier: 4, Store: store, KeepFor: time.Hour},
		proxies, fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool, proxies
}

// A stopping pod parks its idle, bootstrapped identities; the next pod on
// the same exit resumes them (cookies, tokens, fingerprint, request count)
// instead of loading a page again. Retired identities, identities without
// tokens and identities on a resting exit are not parked, and the proxy
// URL's credentials never reach the store.
func TestParkedIdentitiesResumeInTheNextPod(t *testing.T) {
	store := &memStore{m: map[string][][]byte{}}
	a, _ := parkPool(t, store)
	ctx := context.Background()
	id, err := a.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := a.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	retired, err := a.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	id.SetTokens(Tokens{"lsd": "token-1"})
	id.Doer.SetCookie(a.Base(), "csrftoken", "cookie-1")
	for range 3 {
		Outcome(id, detect.OK)
	}
	retired.SetTokens(Tokens{"lsd": "token-2"})
	a.Release(id)
	a.Release(fresh) // never bootstrapped
	a.Retire(retired, "test")
	if n := a.Park(ctx); n != 1 {
		t.Fatalf("parked %d identities, want 1", n)
	}
	for _, l := range store.m {
		for _, b := range l {
			if strings.Contains(string(b), "pass") {
				t.Fatalf("parked record holds the proxy credentials: %s", b)
			}
		}
	}

	b, _ := parkPool(t, store)
	got, err := b.Acquire(ctx, "isp")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Bootstrapped() || got.Tokens()["lsd"] != "token-1" || got.Doer.Cookie(b.Base(), "csrftoken") != "cookie-1" ||
		got.FP.Profile != id.FP.Profile || got.requests != 3 || got.sessionID != id.sessionID {
		t.Fatalf("resumed identity: tokens %v, cookie %q, fp %s, requests %d", got.Tokens(), got.Doer.Cookie(b.Base(), "csrftoken"),
			got.FP.Profile, got.requests)
	}
	// The parked identity is taken once: the next one is fresh.
	next, err := b.Acquire(ctx, "isp")
	if err != nil || next.Bootstrapped() {
		t.Fatalf("second identity bootstrapped %v, %v", next.Bootstrapped(), err)
	}

	// An identity whose exit rests is not parked.
	b.Release(got)
	b.Release(next)
	b.RestExit(got, time.Minute, "test")
	if n := b.Park(ctx); n != 0 {
		t.Fatalf("parked %d identities on a resting exit", n)
	}
}

// A parked identity older than KeepFor is not resumed.
func TestStaleParkedIdentityIsDropped(t *testing.T) {
	store := &memStore{m: map[string][][]byte{}}
	b, proxies := parkPool(t, store)
	ep, _ := proxies.Next("isp")
	rec, _ := json.Marshal(parkedIdentity{Tier: "isp", Exit: ep.Exit(), Profile: "chrome_146", Tokens: Tokens{"lsd": "old"},
		ParkedAt: time.Now().Add(-2 * time.Hour)})
	_ = store.Put(context.Background(), ep.Exit(), rec, time.Hour)
	id, err := b.Acquire(context.Background(), "isp")
	if err != nil || id.Bootstrapped() {
		t.Fatalf("a stale parked identity was resumed: %v %v", id.Tokens(), err)
	}
}
