package session

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
)

// IdentityStore keeps idle identities between processes (Valkey), so a
// restarted or replaced crawler pod resumes with sessions already
// bootstrapped instead of loading a page on every exit again. Entries are
// per exit; Take hands each one to a single caller.
type IdentityStore interface {
	Put(ctx context.Context, exit string, data []byte, ttl time.Duration) error
	// Take returns a stored identity for exit, or nil when there is none.
	Take(ctx context.Context, exit string) ([]byte, error)
}

// parkedIdentity is what a parked identity keeps. Not the proxy URL: its
// credentials stay in the proxies Secret, and the exit names the endpoint.
type parkedIdentity struct {
	Tier        string            `json:"tier"`
	Exit        string            `json:"exit"`
	Profile     string            `json:"profile"`
	Cookies     map[string]string `json:"cookies"`
	Tokens      Tokens            `json:"tokens"`
	SessionID   string            `json:"session_id"`
	Requests    int               `json:"requests"`
	MaxRequests int               `json:"max_requests"`
	Born        time.Time         `json:"born"`
	ParkedAt    time.Time         `json:"parked_at"`
}

// Park stores the pool's idle identities that are bootstrapped, healthy and
// on a static exit, for KeepFor, and closes the pool's idle identities. A
// crawler calls it once its consumers have drained, before it exits.
func (p *Pool) Park(ctx context.Context) int {
	p.mu.Lock()
	var all []*Identity
	for tier, list := range p.idle {
		all = append(all, list...)
		p.live[tier] -= len(list)
		delete(p.idle, tier)
	}
	p.mu.Unlock()
	n := 0
	for _, id := range all {
		if p.park(ctx, id) {
			n++
		}
		id.Doer.Close()
	}
	if n > 0 {
		slog.Info("session: identities parked", "count", n)
	}
	return n
}

func (p *Pool) park(ctx context.Context, id *Identity) bool {
	st := p.cfg.Store
	x := id.Endpoint.Exit()
	if st == nil || p.cfg.KeepFor <= 0 || x == "" || id.Endpoint.SessionID != "" || !id.Bootstrapped() ||
		id.RetireReason() != "" || p.proxies.IsOpen(id.Endpoint) {
		return false
	}
	id.mu.Lock()
	rec := parkedIdentity{Tier: id.Endpoint.Tier, Exit: x, Profile: id.FP.Profile, Tokens: id.tokens, SessionID: id.sessionID,
		Requests: id.requests, MaxRequests: id.maxRequests, Born: id.born, ParkedAt: time.Now()}
	id.mu.Unlock()
	rec.Cookies = id.Doer.Cookies(id.Base)
	b, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	if err := st.Put(ctx, x, b, p.cfg.KeepFor); err != nil {
		slog.Debug("session: park identity", "err", err)
		return false
	}
	metrics.IdentityRetired.WithLabelValues(id.Endpoint.Tier, "parked").Inc()
	return true
}

// unpark rebuilds an identity parked on ep's exit, if one is stored.
func (p *Pool) unpark(ep proxy.Endpoint) *Identity {
	st := p.cfg.Store
	x := ep.Exit()
	if st == nil || p.cfg.KeepFor <= 0 || x == "" || ep.SessionID != "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 3 {
		b, err := st.Take(ctx, x)
		if err != nil || b == nil {
			return nil
		}
		var rec parkedIdentity
		if json.Unmarshal(b, &rec) != nil || rec.Tier != ep.Tier || len(rec.Tokens) == 0 || time.Since(rec.ParkedAt) > p.cfg.KeepFor {
			continue
		}
		fp, ok := p.fps[rec.Profile]
		if !ok {
			continue
		}
		id, err := p.build(ep, fp, rec.MaxRequests)
		if err != nil {
			return nil
		}
		for name, v := range rec.Cookies {
			id.Doer.SetCookie(p.base, name, v)
		}
		id.SetTokens(rec.Tokens)
		id.mu.Lock()
		id.requests, id.born = rec.Requests, rec.Born
		if rec.SessionID != "" {
			id.sessionID = rec.SessionID
		}
		id.mu.Unlock()
		metrics.IdentityUnparked.WithLabelValues(ep.Tier).Inc()
		return id
	}
	return nil
}
