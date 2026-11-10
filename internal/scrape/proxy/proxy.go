// Package proxy hands out proxy endpoints by tier. Only the platform's HTML
// and GraphQL traffic goes through these; media and static files use direct
// datacenter egress.
package proxy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

// Endpoint is one proxy exit to use for an identity's lifetime.
//
// Exits and buckets are named by tier and number, never by address (the
// owner's rule D4): the names appear in logs, metric labels and Valkey keys,
// and an address or a provider's host must appear in none of them. URL holds
// the address and credentials; it is never logged.
type Endpoint struct {
	Tier      string
	URL       string // empty for direct; never logged
	SessionID string
	Country   string
	ExpiresAt time.Time // zero = no sticky expiry
	// Index is a static endpoint's position in its tier's list, from 1.
	Index int
	// Bucket groups exits the platform likely treats alike: the /24 of a
	// static endpoint, or a gateway and country. Breakers work per bucket.
	// Named "<tier>/b<n>" (static) or "<tier>/gw[-<country>]" (gateway).
	Bucket string
}

// Exit names the IP the endpoint sends from, for pacing and spreading
// identities: "<tier>#<n>" for the nth endpoint of a static list, and
// "<tier>#s<session>" for a gateway session (each sticky session is its own
// IP). Direct egress has none: the pod's own address is not shared across
// the fleet.
func (e Endpoint) Exit() string {
	switch {
	case e.URL == "":
		return ""
	case e.SessionID != "":
		return e.Tier + "#s" + e.SessionID
	case e.Index > 0:
		return e.Tier + "#" + strconv.Itoa(e.Index)
	}
	// An endpoint built outside a tier: a hash of its host stands in for
	// the number, so the name still holds no address.
	host := e.URL
	if u, err := url.Parse(e.URL); err == nil && u.Host != "" {
		host = u.Host
	}
	h := sha256.Sum256([]byte(host))
	return e.Tier + "#h" + hex.EncodeToString(h[:4])
}

// ExitTier is the tier of an exit name.
func ExitTier(exit string) string {
	t, _, _ := strings.Cut(exit, "#")
	return t
}

// Tier produces endpoints. Next returns the next endpoint for which allow
// is true, or ErrTierOpen.
type Tier interface {
	Name() string
	Next(allow func(ep Endpoint) bool) (Endpoint, error)
	// Buckets lists the tier's buckets.
	Buckets() []string
	// Static lists a static tier's endpoints (nil for a gateway, whose
	// exits are its sessions, and for direct egress).
	Static() []Endpoint
}

// ErrTierOpen means every bucket of the tier has an open breaker.
var ErrTierOpen = errors.New("proxy: every exit of the tier is cooling down")

// Pool routes query classes to tiers and keeps a circuit breaker per exit
// bucket: one that is blocked too often stops getting new identities. An
// exit (one IP: Endpoint.Exit) can also rest on its own, for a signal one
// response is enough to trust: Instagram's rate limit is per IP, and so is
// a login page served to a fresh visitor.
type Pool struct {
	tiers    map[string]Tier
	routing  map[string][]string
	breakers *breaker.Set
	// rests are the exits resting, keyed by Endpoint.Exit. After a rest an
	// exit is half-open: one identity probes it, and a failure rests it
	// again for twice as long (up to the breaker's max_cooldown).
	rests *breaker.Set
	// shared, when set, spreads rests to every process that uses the same
	// exits (ShareRests).
	shared RestStore
}

// RestStore shares exit rests between processes: a rest one crawler pod
// sees (a rate limit, a checkpoint, a login page) holds for every pod, so
// the others do not each meet the same limit once more before they rest
// the exit too.
type RestStore interface {
	// Publish records that exit rests until until (keeping a later end).
	Publish(ctx context.Context, exit string, until time.Time) error
	// Load returns the rests recorded, ended ones included.
	Load(ctx context.Context) (map[string]time.Time, error)
}

// ShareRests publishes this pool's exit rests to s; SyncRests applies the
// rests other processes published.
func (p *Pool) ShareRests(s RestStore) { p.shared = s }

// SyncRests applies the shared rests every interval until ctx ends.
func (p *Pool) SyncRests(ctx context.Context, every time.Duration) error {
	if p.shared == nil {
		return nil
	}
	if every <= 0 {
		every = 2 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		// Ended rests leave igs_exits_resting here even on a pod that
		// hasn't used the exit since (registry-sync between runs, an idle
		// crawler-retry): the gauge and IgsIgExitsMostlyResting otherwise
		// counted them until the pod next did.
		p.rests.Sweep()
		if err := p.PullRests(ctx); err != nil && ctx.Err() == nil {
			slog.Debug("proxy: shared exit rests", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// PullRests applies the rests other processes published once: an exit
// they rest that this pool does not (or rests for less) rests here too,
// until the same end.
func (p *Pool) PullRests(ctx context.Context) error {
	rests, err := p.shared.Load(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for exit, until := range rests {
		if !until.After(now.Add(time.Second)) {
			continue
		}
		if mine, open := p.rests.OpenUntil(exit); open && !mine.Before(until) {
			continue
		}
		p.rests.Trip(exit, until.Sub(now))
	}
	return nil
}

// publish shares exit's rest, if it rests, without holding up the caller.
func (p *Pool) publish(exit string) {
	if p.shared == nil {
		return
	}
	until, open := p.rests.OpenUntil(exit)
	if !open {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.shared.Publish(ctx, exit, until); err != nil {
			slog.Debug("proxy: publish exit rest", "err", err)
		}
	}()
}

// New builds a pool from config. A "direct" tier always exists.
func New(cfg config.Proxy) (*Pool, error) {
	p := &Pool{tiers: map[string]Tier{"direct": direct{}}, routing: cfg.Routing, breakers: breaker.New(cfg.Breaker),
		rests: breaker.New(cfg.Breaker)}
	p.breakers.OnChange = func(bucket string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("proxy:" + bucket).Set(float64(to))
		slog.Info("proxy: breaker", "bucket", bucket, "from", from.String(), "to", to.String())
	}
	p.rests.OnChange = func(exit string, from, to breaker.State) {
		tier := ExitTier(exit)
		switch {
		case to == breaker.Open && from != breaker.Open:
			metrics.ExitsResting.WithLabelValues(tier).Inc()
		case from == breaker.Open && to != breaker.Open:
			metrics.ExitsResting.WithLabelValues(tier).Dec()
		}
		slog.Info("proxy: exit", "exit", exit, "from", restState(from), "to", restState(to))
	}
	for _, t := range cfg.Tiers {
		switch t.Kind {
		case "static_list":
			if len(t.Endpoints) == 0 {
				return nil, fmt.Errorf("proxy: tier %s has no endpoints", t.Name)
			}
			sl := &staticList{name: t.Name, endpoints: t.Endpoints, country: t.Country, buckets: staticBuckets(t.Name, t.Endpoints)}
			p.tiers[t.Name] = sl
			metrics.Exits.WithLabelValues(t.Name).Set(float64(len(t.Endpoints)))
		case "gateway":
			if t.Gateway == "" {
				return nil, fmt.Errorf("proxy: tier %s has no gateway", t.Name)
			}
			if _, err := url.Parse(t.Gateway); err != nil {
				return nil, fmt.Errorf("proxy: tier %s: %w", t.Name, err)
			}
			p.tiers[t.Name] = &gateway{cfg: t}
		case "direct", "":
			p.tiers[t.Name] = direct{}
		default:
			return nil, fmt.Errorf("proxy: tier %s: unknown kind %q", t.Name, t.Kind)
		}
	}
	if len(p.routing) == 0 {
		p.routing = map[string][]string{"default": {"direct"}}
	}
	for class, tiers := range p.routing {
		for _, t := range tiers {
			if _, ok := p.tiers[t]; !ok {
				return nil, fmt.Errorf("proxy: routing %s references unknown tier %s", class, t)
			}
		}
	}
	return p, nil
}

// TiersFor returns the tier names to try for a query class, cheapest first.
func (p *Pool) TiersFor(class string) []string {
	if t, ok := p.routing[class]; ok && len(t) > 0 {
		return t
	}
	return p.routing["default"]
}

// Next returns a fresh endpoint from tier, skipping buckets whose breaker
// is open. In a half-open bucket the endpoint is the probe.
func (p *Pool) Next(tier string) (Endpoint, error) {
	return p.NextWhere(tier, nil)
}

// ErrNoFreeExit means every exit of the tier that is not cooling down is
// one the caller asked to skip.
var ErrNoFreeExit = errors.New("proxy: no free exit in the tier")

// NextWhere is Next, skipping endpoints for which skip is true (exits
// already busy, or one that just failed). It returns ErrTierOpen when every
// bucket cools down, and ErrNoFreeExit when the rest are all skipped.
// skip is asked before a half-open bucket admits its probe, so a skipped
// endpoint never takes it.
func (p *Pool) NextWhere(tier string, skip func(Endpoint) bool) (Endpoint, error) {
	t, ok := p.tiers[tier]
	if !ok {
		return Endpoint{}, fmt.Errorf("proxy: unknown tier %s", tier)
	}
	skipped := false
	ep, err := t.Next(func(ep Endpoint) bool {
		if p.IsOpen(ep) {
			return false
		}
		if skip != nil && skip(ep) {
			skipped = true
			return false
		}
		if ep.Bucket != "" && !p.breakers.Allow(ep.Bucket) {
			return false
		}
		// A rested exit that is half-open admits one identity at a time.
		return ep.Exit() == "" || p.rests.Allow(ep.Exit())
	})
	if errors.Is(err, ErrTierOpen) && skipped {
		return Endpoint{}, ErrNoFreeExit
	}
	return ep, err
}

// Available reports whether tier has an exit that is not cooling down.
func (p *Pool) Available(tier string) bool {
	t, ok := p.tiers[tier]
	if !ok {
		return false
	}
	if static := t.Static(); len(static) > 0 {
		for _, ep := range static {
			if !p.IsOpen(ep) {
				return true
			}
		}
		return false
	}
	if len(t.Buckets()) == 0 {
		return true // direct egress has no breaker
	}
	for _, b := range t.Buckets() {
		if !p.breakers.IsOpen(b) {
			return true
		}
	}
	return false
}

// IsOpen reports whether ep's bucket cools down or ep's exit rests.
func (p *Pool) IsOpen(ep Endpoint) bool {
	return (ep.Bucket != "" && p.breakers.IsOpen(ep.Bucket)) || (ep.Exit() != "" && p.rests.IsOpen(ep.Exit()))
}

// Rest takes ep's exit out of use for d: when Instagram rate-limits it
// (ig.rate_limit_rest), sends it a checkpoint (ig.checkpoint_rest), or
// serves fresh visitors its login page (ig.page_wall_rest). Other exits of
// its bucket carry on. Direct egress has no exit and cannot rest.
func (p *Pool) Rest(ep Endpoint, d time.Duration) {
	if ep.Exit() != "" && d > 0 {
		p.rests.Trip(ep.Exit(), d)
		p.publish(ep.Exit())
	}
}

// Observe records a response class for ep's bucket, and for its exit
// while that is resting or probed after a rest. Blocks and transport
// failures count against them.
func (p *Pool) Observe(ep Endpoint, c detect.Class) {
	failed := c.Blocking() || c == detect.Transport
	if ep.Bucket != "" {
		p.breakers.Observe(ep.Bucket, failed)
	}
	if x := ep.Exit(); x != "" && p.rests.State(x) != breaker.Closed {
		before := p.rests.State(x)
		p.rests.Observe(x, failed)
		if before == breaker.HalfOpen && p.rests.State(x) == breaker.Open {
			// A failed probe rests the exit again, for longer: for every
			// process.
			p.publish(x)
		}
	}
}

func restState(s breaker.State) string {
	switch s {
	case breaker.Open:
		return "resting"
	case breaker.HalfOpen:
		return "probing"
	}
	return "in use"
}

// staticBuckets names each endpoint's bucket "<tier>/b<n>", numbering the
// distinct groups in list order. A group is the /24 of an IPv4 endpoint (the
// /48 of an IPv6 one), else its host and port: a provider that names its
// exits by port on one host gives each port its own exit IP, so one blocked
// port must not cool down the others.
func staticBuckets(tier string, endpoints []string) []string {
	n := map[string]int{}
	out := make([]string, len(endpoints))
	for i, e := range endpoints {
		g := staticGroup(e)
		if _, ok := n[g]; !ok {
			n[g] = len(n) + 1
		}
		out[i] = tier + "/b" + strconv.Itoa(n[g])
	}
	return out
}

// staticGroup is the address group an endpoint's bucket stands for. It is
// only compared, never shown.
func staticGroup(raw string) string {
	host := raw
	if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
		host = u.Hostname()
		if net.ParseIP(host) == nil && u.Port() != "" {
			return net.JoinHostPort(host, u.Port())
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2])
		}
		return (&net.IPNet{IP: ip.Mask(net.CIDRMask(48, 128)), Mask: net.CIDRMask(48, 128)}).String()
	}
	return host
}

type direct struct{}

func (direct) Name() string       { return "direct" }
func (direct) Buckets() []string  { return nil }
func (direct) Static() []Endpoint { return nil }
func (direct) Next(allow func(Endpoint) bool) (Endpoint, error) {
	ep := Endpoint{Tier: "direct"}
	if !allow(ep) {
		return Endpoint{}, ErrTierOpen
	}
	return ep, nil
}

type staticList struct {
	name      string
	endpoints []string
	buckets   []string // parallel to endpoints
	country   string
	next      atomic.Uint64
}

func (s *staticList) Name() string      { return s.name }
func (s *staticList) Buckets() []string { return s.buckets }

func (s *staticList) Static() []Endpoint {
	out := make([]Endpoint, len(s.endpoints))
	for i, e := range s.endpoints {
		out[i] = Endpoint{Tier: s.name, URL: e, Country: s.country, Index: i + 1, Bucket: s.buckets[i]}
	}
	return out
}

func (s *staticList) Next(allow func(Endpoint) bool) (Endpoint, error) {
	for range s.endpoints {
		i := (s.next.Add(1) - 1) % uint64(len(s.endpoints))
		if ep := (Endpoint{Tier: s.name, URL: s.endpoints[i], Country: s.country, Index: int(i) + 1, Bucket: s.buckets[i]}); allow(ep) {
			return ep, nil
		}
	}
	return Endpoint{}, ErrTierOpen
}

// gateway is a rotating residential gateway: the exit IP is chosen by the
// session id embedded in the proxy username and stays sticky for StickyTTL.
type gateway struct {
	cfg config.ProxyTier
}

func (g *gateway) Name() string { return g.cfg.Name }

// bucket is the gateway's one bucket, "<tier>/gw" with its country: a
// tier has one gateway.
func (g *gateway) bucket() string {
	if g.cfg.Country != "" {
		return g.cfg.Name + "/gw-" + g.cfg.Country
	}
	return g.cfg.Name + "/gw"
}

func (g *gateway) Buckets() []string  { return []string{g.bucket()} }
func (g *gateway) Static() []Endpoint { return nil }

func (g *gateway) Next(allow func(Endpoint) bool) (Endpoint, error) {
	sess := randomSession()
	ttl := g.cfg.StickyTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	user := g.cfg.UsernameTemplate
	if user == "" {
		user = "{user}-session-{session}"
	}
	user = strings.NewReplacer(
		"{user}", g.cfg.Username,
		"{country}", g.cfg.Country,
		"{session}", sess,
		"{ttl_min}", strconv.Itoa(int(ttl.Minutes())),
	).Replace(user)
	u, _ := url.Parse(g.cfg.Gateway)
	u.User = url.UserPassword(user, g.cfg.Password)
	ep := Endpoint{
		Tier:      g.cfg.Name,
		URL:       u.String(),
		SessionID: sess,
		Country:   g.cfg.Country,
		ExpiresAt: time.Now().Add(ttl),
		Bucket:    g.bucket(),
	}
	if !allow(ep) {
		return Endpoint{}, ErrTierOpen
	}
	return ep, nil
}

func randomSession() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
