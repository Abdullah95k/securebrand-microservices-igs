package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// PoolConfig configures identity creation.
type PoolConfig struct {
	BaseURL string
	Profile string // default fingerprint profile
	// Profiles, when set, are the fingerprint profiles new identities pick
	// from at random, so one pool does not show a single browser build;
	// empty means Profile alone.
	Profiles       []string
	Timeout        time.Duration
	IdentityRPS    float64
	IdentityBurst  int
	MaxRequestsMin int
	MaxRequestsMax int
	// MaxPerTier caps live identities per tier; Acquire waits at the cap.
	MaxPerTier int
	// ExitRPS paces each proxy exit through ExitPacer, fleet-wide: the
	// platform limits per IP (Instagram, U4), however many identities and
	// pods share the exit. 0 or a nil pacer leaves it to identity pacing.
	ExitRPS   float64
	ExitPacer ExitPacer
	// Shares caps a class of requests (WithShare) at a fraction of
	// ExitRPS on each exit, fleet-wide (key exit:<exit>:<class>): the
	// class waits for its share and then for the exit like any request.
	// A class with no share, or a share of 0, is not capped.
	Shares map[string]float64
	// Rest rests an exit for the duration when one of its responses has
	// the class (ig.rate_limit_rest, ig.checkpoint_rest).
	Rest map[detect.Class]time.Duration
	// Store keeps idle identities across restarts for KeepFor (Park);
	// a new identity on an exit takes a stored one first. nil or 0: off.
	Store   IdentityStore
	KeepFor time.Duration
}

// ExitPacer paces a key fleet-wide at a rate below one a second
// (ratelimit.Redis).
type ExitPacer interface {
	WaitRate(ctx context.Context, key string, perSecond float64) error
}

// DoerFactory builds a transport for an identity (swappable in tests).
type DoerFactory func(o transport.Options) (transport.Doer, error)

// Pool owns this pod's identities. Each identity is leased exclusively.
type Pool struct {
	cfg     PoolConfig
	base    *url.URL
	proxies *proxy.Pool
	fps     map[string]Fingerprint
	newDoer DoerFactory

	mu   sync.Mutex
	cond *sync.Cond
	idle map[string][]*Identity
	live map[string]int
	// leased counts the identities in use per exit (Endpoint.Exit), so
	// tasks spread over the exits rather than queue on one exit's pacing.
	leased map[string]int
	// waiting counts the Acquire calls waiting at MaxPerTier, per tier.
	waiting map[string]int
	// all holds every live identity, idle or in use, for Checkpoint.
	all map[*Identity]struct{}
	// owner names this pool's checkpoint in the store.
	owner string
}

// gaugesLocked publishes tier's identities (igs_identities{tier,state}):
// idle, leased (in use by a task) and waiting (tasks waiting at the
// pod's cap). Callers hold p.mu.
func (p *Pool) gaugesLocked(tier string) {
	idle := len(p.idle[tier])
	metrics.IdentityActive.WithLabelValues(tier).Set(float64(p.live[tier]))
	metrics.Identities.WithLabelValues(tier, "idle").Set(float64(idle))
	metrics.Identities.WithLabelValues(tier, "leased").Set(float64(max(p.live[tier]-idle, 0)))
	metrics.Identities.WithLabelValues(tier, "waiting").Set(float64(p.waiting[tier]))
}

// shortfall counts an Acquire that found no identity for a task
// (igs_identity_shortfall_total): every exit cooling down, no exit but the
// one to avoid, or the pod's cap.
func shortfall(tier string, err error) {
	switch {
	case errors.Is(err, proxy.ErrTierOpen):
		metrics.IdentityShortfall.WithLabelValues(tier, "exits_cooling").Inc()
	case errors.Is(err, proxy.ErrNoFreeExit):
		metrics.IdentityShortfall.WithLabelValues(tier, "no_other_exit").Inc()
	}
}

func NewPool(cfg PoolConfig, proxies *proxy.Pool, fps map[string]Fingerprint, f DoerFactory) (*Pool, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("session: base url: %w", err)
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = []string{cfg.Profile}
	}
	for _, name := range cfg.Profiles {
		if _, ok := fps[name]; !ok {
			return nil, fmt.Errorf("session: no fingerprint for profile %q", name)
		}
	}
	if f == nil {
		f = transport.NewTLS
	}
	if cfg.MaxPerTier <= 0 {
		cfg.MaxPerTier = 512
	}
	p := &Pool{cfg: cfg, base: base, proxies: proxies, fps: fps, newDoer: f,
		idle: map[string][]*Identity{}, live: map[string]int{}, leased: map[string]int{}, waiting: map[string]int{},
		all: map[*Identity]struct{}{}, owner: "crawler-" + randAlnum(10)}
	p.cond = sync.NewCond(&p.mu)
	return p, nil
}

// Base returns the platform's base URL.
func (p *Pool) Base() *url.URL { return p.base }

// Acquire returns an identity on tier for exclusive use. Fresh identities
// are not bootstrapped yet; the caller bootstraps them on first use.
//
// Identities spread over the exits: an idle identity on an exit no task
// is using comes first, then a new identity on such an exit, then the idle
// identity on the least used exit, then a new one on any exit. It returns
// proxy.ErrTierOpen when every exit of the tier cools down.
func (p *Pool) Acquire(ctx context.Context, tier string) (*Identity, error) {
	return p.acquire(ctx, tier, "")
}

// AcquireElsewhere is Acquire on any exit but avoid (an Endpoint.Exit):
// the failover of a task whose identity was just blocked there. It returns
// proxy.ErrNoFreeExit when no other exit of the tier is usable.
func (p *Pool) AcquireElsewhere(ctx context.Context, tier, avoid string) (*Identity, error) {
	return p.acquire(ctx, tier, avoid)
}

func (p *Pool) acquire(ctx context.Context, tier, avoid string) (*Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.gaugesLocked(tier)
	waited := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		list := p.idle[tier]
		best, bestLoad := -1, 0
		for i := len(list) - 1; i >= 0; i-- {
			id := list[i]
			if p.proxies.IsOpen(id.Endpoint) {
				// Its exit rests or its bucket cools down: retire rather
				// than reuse.
				list = append(list[:i], list[i+1:]...)
				p.idle[tier] = list
				p.live[tier]--
				delete(p.all, id)
				if best > i {
					best--
				}
				metrics.IdentityRetired.WithLabelValues(tier, "breaker").Inc()
				go id.Doer.Close()
				continue
			}
			x := id.Endpoint.Exit()
			if id.coolingDown() || (avoid != "" && x == avoid) {
				continue
			}
			load := 0
			if x != "" {
				load = p.leased[x]
			}
			if best < 0 || load < bestLoad {
				best, bestLoad = i, load
			}
			if load == 0 {
				break
			}
		}
		take := func(i int) *Identity {
			id := p.idle[tier][i]
			p.idle[tier] = append(p.idle[tier][:i], p.idle[tier][i+1:]...)
			p.lease(id)
			return id
		}
		if best >= 0 && bestLoad == 0 {
			return take(best), nil
		}
		if p.live[tier] < p.cfg.MaxPerTier {
			// A new identity on an exit no task uses; failing that, the
			// least used idle identity; failing that, a new one anywhere.
			busy := map[string]bool{}
			for x, n := range p.leased {
				busy[x] = n > 0
			}
			id, err := p.createLocked(tier, func(ep proxy.Endpoint) bool {
				x := ep.Exit()
				return x != "" && (x == avoid || busy[x])
			})
			if errors.Is(err, proxy.ErrNoFreeExit) {
				if best >= 0 {
					return take(best), nil
				}
				id, err = p.createLocked(tier, func(ep proxy.Endpoint) bool { return avoid != "" && ep.Exit() == avoid })
			}
			if err != nil {
				shortfall(tier, err)
				return nil, err
			}
			return id, nil
		}
		if best >= 0 {
			return take(best), nil
		}
		// At the cap: wait for a release (or a cooldown to end).
		if !waited {
			waited = true
			metrics.IdentityShortfall.WithLabelValues(tier, "at_cap").Inc()
		}
		p.waiting[tier]++
		p.gaugesLocked(tier)
		stop := context.AfterFunc(ctx, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		timer := time.AfterFunc(time.Second, func() { p.mu.Lock(); p.cond.Broadcast(); p.mu.Unlock() })
		p.cond.Wait()
		stop()
		timer.Stop()
		p.waiting[tier]--
	}
}

// lease counts id as in use on its exit. Callers hold p.mu.
func (p *Pool) lease(id *Identity) {
	if x := id.Endpoint.Exit(); x != "" {
		p.leased[x]++
	}
}

// Release returns id to the pool, or retires it.
func (p *Pool) Release(id *Identity) {
	tier := id.Endpoint.Tier
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.cond.Broadcast()
	if x := id.Endpoint.Exit(); x != "" {
		if p.leased[x]--; p.leased[x] <= 0 {
			delete(p.leased, x)
		}
	}
	defer p.gaugesLocked(tier)
	if reason := id.RetireReason(); reason != "" {
		p.live[tier]--
		delete(p.all, id)
		metrics.IdentityRetired.WithLabelValues(tier, reason).Inc()
		slog.Debug("session: identity retired", "identity", id.ID, "tier", tier, "reason", reason)
		go id.Doer.Close()
		return
	}
	p.idle[tier] = append(p.idle[tier], id)
}

// RestExit takes id's exit out of use for d: no new identity gets it and
// idle identities on it are retired, until d has passed.
func (p *Pool) RestExit(id *Identity, d time.Duration, reason string) {
	if id.Endpoint.Exit() == "" || d <= 0 {
		return
	}
	p.proxies.Rest(id.Endpoint, d)
	metrics.ExitRests.WithLabelValues(id.Endpoint.Tier, reason).Inc()
	slog.Info("session: exit rests", "exit", id.Endpoint.Exit(), "for", d, "reason", reason)
}

// Retire marks id as unusable and releases it.
func (p *Pool) Retire(id *Identity, reason string) {
	id.mu.Lock()
	if id.retired == "" {
		id.retired = reason
	}
	id.mu.Unlock()
	p.Release(id)
}

// createLocked creates and leases an identity on tier, on an exit skip
// allows. Callers hold p.mu; it is released while the transport is built.
func (p *Pool) createLocked(tier string, skip func(proxy.Endpoint) bool) (*Identity, error) {
	ep, err := p.proxies.NextWhere(tier, skip)
	if err != nil {
		return nil, err
	}
	p.live[tier]++
	p.mu.Unlock()
	id, err := p.create(ep)
	p.mu.Lock()
	if err != nil {
		p.live[tier]--
		return nil, err
	}
	p.lease(id)
	p.all[id] = struct{}{}
	return id, nil
}

// create makes an identity on ep: one parked there by a process that
// stopped, if the store holds one, else a fresh one.
func (p *Pool) create(ep proxy.Endpoint) (*Identity, error) {
	if id := p.unpark(ep); id != nil {
		return id, nil
	}
	fp := p.fps[p.cfg.Profiles[randRange(0, len(p.cfg.Profiles))]]
	return p.build(ep, fp, randRange(p.cfg.MaxRequestsMin, p.cfg.MaxRequestsMax))
}

func (p *Pool) build(ep proxy.Endpoint, fp Fingerprint, maxReq int) (*Identity, error) {
	d, err := p.newDoer(transport.Options{Profile: fp.Profile, ProxyURL: ep.URL, Timeout: p.cfg.Timeout})
	if err != nil {
		return nil, err
	}
	id := newIdentity(newID(), ep, fp, d, p.base, p.cfg.IdentityRPS, p.cfg.IdentityBurst, maxReq)
	id.observe = func(c detect.Class) {
		p.proxies.Observe(ep, c)
		if rest := p.cfg.Rest[c]; rest > 0 {
			// Instagram limits per IP: other identities on the exit would
			// meet the same answer.
			p.RestExit(id, rest, c.String())
		}
	}
	if ep.Exit() != "" || ep.Bucket != "" {
		id.resting = func() bool { return p.proxies.IsOpen(ep) }
	}
	if x := ep.Exit(); x != "" && p.cfg.ExitRPS > 0 && p.cfg.ExitPacer != nil {
		key, rps := "exit:"+x, p.cfg.ExitRPS
		id.exitWait = func(ctx context.Context) error { return p.cfg.ExitPacer.WaitRate(ctx, key, rps) }
		id.shareWait = func(ctx context.Context, class string) error {
			share := p.cfg.Shares[class]
			if share <= 0 {
				return nil
			}
			began := time.Now()
			err := p.cfg.ExitPacer.WaitRate(ctx, key+":"+class, rps*share)
			metrics.ExitShareRequests.WithLabelValues(class).Inc()
			metrics.ExitShareWait.WithLabelValues(class).Add(time.Since(began).Seconds())
			return err
		}
	}
	return id, nil
}

// Outcome records a request outcome on id. It is a convenience used by the
// GraphQL client and bootstrapper.
func Outcome(id *Identity, c detect.Class) {
	if id.observe != nil {
		id.observe(c)
	}
	if retire, reason := id.Record(c); retire {
		slog.Debug("session: identity marked for retirement", "identity", id.ID, "reason", reason, "class", c.String())
	}
}
