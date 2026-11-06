package discovery

import (
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// A static tier is mostly resting when more than hold_resting_share of its
// exits rest now. Ended rests, gateway sessions and names outside the
// tier's exits do not count.
func TestRestingTier(t *testing.T) {
	cfg := &config.Config{}
	cfg.IG.Discovery.HoldRestingShare = 0.5
	cfg.IG.Proxy.Tiers = []config.ProxyTier{
		{Name: "isp", Kind: "static_list", Endpoints: []string{"a", "b", "c", "d"}},
		{Name: "res", Kind: "gateway", Gateway: "http://gw"},
	}
	v := &Vetter{Cfg: cfg}
	now := time.Now()
	later, ended := now.Add(time.Minute), now.Add(-time.Minute)
	for _, tc := range []struct {
		name  string
		rests map[string]time.Time
		want  string
	}{
		{"none", nil, ""},
		{"half", map[string]time.Time{"isp#1": later, "isp#2": later}, ""},
		{"three of four", map[string]time.Time{"isp#1": later, "isp#2": later, "isp#4": later}, "isp"},
		{"one ended", map[string]time.Time{"isp#1": later, "isp#2": later, "isp#3": ended}, ""},
		{"not its exits", map[string]time.Time{"isp#1": later, "isp#2": later, "isp#5": later, "isp#h1a2b3c4d": later}, ""},
		{"gateway sessions", map[string]time.Time{"res#s1": later, "res#s2": later, "res#s3": later}, ""},
	} {
		if got := v.restingTier(tc.rests, now); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
	cfg.IG.Discovery.HoldRestingShare = 0.25
	if got := v.restingTier(map[string]time.Time{"isp#1": later, "isp#2": later}, now); got != "isp" {
		t.Errorf("share 0.25, two of four resting: %q, want isp", got)
	}
}
