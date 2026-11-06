package session_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

func storePool(t *testing.T, store session.IdentityStore) *session.Pool {
	t.Helper()
	proxies, err := proxy.New(config.Proxy{
		Tiers: []config.ProxyTier{{Name: "isp", Kind: "static_list",
			Endpoints: []string{"http://user:pass@203.0.113.10:8000", "http://user:pass@203.0.113.11:8000"}}},
		Routing: map[string][]string{"default": {"isp"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: "https://www.instagram.com", Profile: "chrome_146",
		IdentityRPS: 1, IdentityBurst: 1, MaxRequestsMin: 500, MaxRequestsMax: 501, MaxPerTier: 4, Store: store, KeepFor: time.Hour},
		proxies, fps, func(transport.Options) (transport.Doer, error) { return transport.NewStd(time.Second), nil })
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

// A crawler that dies without parking (B5, fbs 27d2062) leaves its last
// checkpoint: every identity it held, idle or in use. Once its lease has
// ended, another crawler adopts them, once, and the next identity on each
// exit resumes one instead of loading a page. A clean stop parks and drops
// the checkpoint, so nothing is handed out twice. No proxy credential
// reaches the store.
func TestCheckpointOutlivesACrash(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := ratelimit.NewParkedIdentities(rdb, "t.")
	ctx := context.Background()

	dead := storePool(t, store)
	idle, _ := dead.Acquire(ctx, "isp")
	busy, _ := dead.Acquire(ctx, "isp")
	fresh, _ := dead.Acquire(ctx, "isp")
	idle.SetTokens(session.Tokens{"lsd": "idle"})
	busy.SetTokens(session.Tokens{"lsd": "busy"})
	dead.Release(idle)
	_ = fresh // never bootstrapped: not kept
	if n, err := dead.Checkpoint(ctx); err != nil || n != 2 {
		t.Fatalf("checkpointed %d identities (%v), want the idle and the busy one", n, err)
	}
	for _, k := range mr.Keys() {
		if v, _ := mr.Get(k); strings.Contains(v, "pass") {
			t.Fatalf("%s holds the proxy credentials", k)
		}
	}

	// The crawler dies (no Park). While its lease holds, nobody takes them.
	next := storePool(t, store)
	if n, err := next.Adopt(ctx); err != nil || n != 0 {
		t.Fatalf("adopted %d while the lease holds (%v)", n, err)
	}
	mr.FastForward(6 * time.Minute)
	if n, err := next.Adopt(ctx); err != nil || n != 2 {
		t.Fatalf("adopted %d after the lease (%v), want 2", n, err)
	}
	if n, _ := next.Adopt(ctx); n != 0 {
		t.Fatalf("adopted %d again: a checkpoint is taken once", n)
	}
	got := map[string]bool{}
	for range 2 {
		id, err := next.Acquire(ctx, "isp")
		if err != nil || !id.Bootstrapped() {
			t.Fatalf("resumed identity: bootstrapped %v, %v", id != nil && id.Bootstrapped(), err)
		}
		got[id.Tokens()["lsd"]] = true
	}
	if !got["idle"] || !got["busy"] {
		t.Fatalf("resumed %v, want the idle and the busy identity", got)
	}

	// A clean stop parks and drops the checkpoint.
	clean := storePool(t, store)
	id, _ := clean.Acquire(ctx, "isp")
	id.SetTokens(session.Tokens{"lsd": "clean"})
	if _, err := clean.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	clean.Release(id)
	if n := clean.Park(ctx); n != 1 {
		t.Fatalf("parked %d, want 1", n)
	}
	mr.FastForward(6 * time.Minute)
	if n, _ := storePool(t, store).Adopt(ctx); n != 0 {
		t.Fatalf("adopted %d from a crawler that parked: they would be handed out twice", n)
	}
}
