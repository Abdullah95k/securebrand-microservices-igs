package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
)

// igs_identities{tier,state} follows the pod's identities: leased while a
// task holds one, idle once released, waiting while a task waits at the
// pod's cap. igs_identity_shortfall_total counts the tasks that wait at
// the cap and those that get none because every exit cools down.
func TestIdentityGaugesAndShortfall(t *testing.T) {
	pool, proxies, _ := ispPool(t, 2)
	ctx := context.Background()
	atCap, cooling := shortfallCount("isp", "at_cap"), shortfallCount("isp", "exits_cooling")

	a, _ := pool.Acquire(ctx, "isp")
	b, _ := pool.Acquire(ctx, "isp")
	wantStates(t, "two leased", 0, 2, 0)

	got := make(chan *Identity)
	go func() {
		c, err := pool.Acquire(ctx, "isp")
		if err != nil {
			t.Error(err)
		}
		got <- c
	}()
	deadline := time.Now().Add(5 * time.Second)
	for identityGauge("isp", "waiting") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("a third task does not wait at the cap of 2")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d := shortfallCount("isp", "at_cap") - atCap; d != 1 {
		t.Errorf("at_cap shortfalls %v, want 1", d)
	}
	pool.Release(a)
	c := <-got
	if c != a {
		t.Fatal("the waiting task takes the released identity")
	}
	wantStates(t, "the waiting task served", 0, 2, 0)
	pool.Release(b)
	pool.Release(c)
	wantStates(t, "both released", 2, 0, 0)

	// Every exit rests: the idle identities retire and a task gets none.
	for i := 1; i <= 3; i++ {
		proxies.Rest(proxy.Endpoint{Tier: "isp", URL: "http://isp.example", Index: i}, time.Minute)
	}
	if _, err := pool.Acquire(ctx, "isp"); !errors.Is(err, proxy.ErrTierOpen) {
		t.Fatalf("every exit resting: %v, want ErrTierOpen", err)
	}
	wantStates(t, "every exit resting", 0, 0, 0)
	if d := shortfallCount("isp", "exits_cooling") - cooling; d != 1 {
		t.Errorf("exits_cooling shortfalls %v, want 1", d)
	}
}

func wantStates(t *testing.T, when string, idle, leased, waiting float64) {
	t.Helper()
	got := [3]float64{identityGauge("isp", "idle"), identityGauge("isp", "leased"), identityGauge("isp", "waiting")}
	if got != [3]float64{idle, leased, waiting} {
		t.Errorf("%s: idle, leased, waiting %v, want %v", when, got, [3]float64{idle, leased, waiting})
	}
}

func identityGauge(tier, state string) float64 {
	return metricValue("igs_identities", map[string]string{"tier": tier, "state": state})
}

func shortfallCount(tier, reason string) float64 {
	return metricValue("igs_identity_shortfall_total", map[string]string{"tier": tier, "reason": reason})
}

// metricValue reads a gauge's or counter's child with exactly labels.
func metricValue(name string, labels map[string]string) float64 {
	mfs, _ := metrics.Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if labels[l.GetName()] != l.GetValue() {
					continue next
				}
			}
			return m.GetGauge().GetValue() + m.GetCounter().GetValue()
		}
	}
	return 0
}
