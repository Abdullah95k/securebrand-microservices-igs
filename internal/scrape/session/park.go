package session

import (
	"context"
	"encoding/json"
	"errors"
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

// CheckpointStore keeps a running pool's identities, idle or in use, in
// one snapshot under a lease (Pool.RunCheckpoint). A crawler that dies
// without parking leaves its last snapshot, which another crawler takes
// once the lease ends (Pool.Adopt) and parks exit by exit.
type CheckpointStore interface {
	// Checkpoint replaces owner's snapshot, kept for lease+keep, and renews
	// its lease.
	Checkpoint(ctx context.Context, owner string, data []byte, lease, keep time.Duration) error
	// Release drops owner's snapshot and lease.
	Release(ctx context.Context, owner string) error
	// TakeOrphans removes and returns the snapshots of other owners whose
	// lease has ended, each to one caller.
	TakeOrphans(ctx context.Context, self string) ([][]byte, error)
}

// A running pool checkpoints every CheckpointEvery under a lease of
// checkpointLease and looks for dead pools' snapshots every adoptEvery:
// a crawler that dies leaves identities at most CheckpointEvery old, free
// for the others within checkpointLease plus adoptEvery (fbs 27d2062).
const (
	CheckpointEvery = 2 * time.Minute
	checkpointLease = 5 * time.Minute
	adoptEvery      = time.Minute
)

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
// on a static exit, for KeepFor, closes the pool's idle identities and
// drops the pool's checkpoint. A crawler calls it once its consumers have
// drained, before it exits.
func (p *Pool) Park(ctx context.Context) int {
	p.mu.Lock()
	var all []*Identity
	for tier, list := range p.idle {
		all = append(all, list...)
		p.live[tier] -= len(list)
		for _, id := range list {
			delete(p.all, id)
		}
		delete(p.idle, tier)
		p.gaugesLocked(tier)
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
	// Parked exit by exit now: the checkpoint would only hand them out twice.
	if cs, ok := p.cfg.Store.(CheckpointStore); ok && p.cfg.KeepFor > 0 {
		if err := cs.Release(ctx, p.owner); err != nil {
			slog.Warn("session: checkpoint not released", "err", err)
		}
	}
	return n
}

func (p *Pool) park(ctx context.Context, id *Identity) bool {
	rec, ok := p.record(id, time.Now())
	if !ok {
		return false
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	if err := p.cfg.Store.Put(ctx, rec.Exit, b, p.cfg.KeepFor); err != nil {
		slog.Debug("session: park identity", "err", err)
		return false
	}
	metrics.IdentityRetired.WithLabelValues(id.Endpoint.Tier, "parked").Inc()
	return true
}

// record is what id keeps when parked or checkpointed, if it can be kept:
// bootstrapped, healthy, on a static exit that does not rest.
func (p *Pool) record(id *Identity, at time.Time) (parkedIdentity, bool) {
	x := id.Endpoint.Exit()
	if p.cfg.Store == nil || p.cfg.KeepFor <= 0 || x == "" || id.Endpoint.SessionID != "" || !id.Bootstrapped() ||
		id.RetireReason() != "" || p.proxies.IsOpen(id.Endpoint) {
		return parkedIdentity{}, false
	}
	id.mu.Lock()
	rec := parkedIdentity{Tier: id.Endpoint.Tier, Exit: x, Profile: id.FP.Profile, Tokens: id.tokens, SessionID: id.sessionID,
		Requests: id.requests, MaxRequests: id.maxRequests, Born: id.born, ParkedAt: at}
	id.mu.Unlock()
	rec.Cookies = id.Doer.Cookies(id.Base)
	return rec, true
}

// Checkpoint stores every identity the pool holds, idle or in use, as one
// snapshot under the pool's lease, and returns how many it stored. A
// crawler that dies without parking leaves this snapshot for Adopt.
func (p *Pool) Checkpoint(ctx context.Context) (int, error) {
	cs, ok := p.cfg.Store.(CheckpointStore)
	if !ok || p.cfg.KeepFor <= 0 {
		return 0, nil
	}
	p.mu.Lock()
	ids := make([]*Identity, 0, len(p.all))
	for id := range p.all {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	now := time.Now()
	recs := make([]parkedIdentity, 0, len(ids))
	for _, id := range ids {
		if rec, ok := p.record(id, now); ok {
			recs = append(recs, rec)
		}
	}
	b, err := json.Marshal(recs)
	if err != nil {
		return 0, err
	}
	return len(recs), cs.Checkpoint(ctx, p.owner, b, checkpointLease, p.cfg.KeepFor)
}

// Adopt takes the snapshots of crawlers that died without parking (their
// lease ended) and parks their identities exit by exit, as if those
// crawlers had parked them: the next identity on each exit resumes one
// instead of loading a page. Identities older than KeepFor are dropped. It
// returns how many it parked.
func (p *Pool) Adopt(ctx context.Context) (int, error) {
	cs, ok := p.cfg.Store.(CheckpointStore)
	if !ok || p.cfg.KeepFor <= 0 {
		return 0, nil
	}
	snaps, err := cs.TakeOrphans(ctx, p.owner)
	n := 0
	for _, b := range snaps {
		var recs []parkedIdentity
		if json.Unmarshal(b, &recs) != nil {
			continue
		}
		kept := 0
		var age time.Duration
		for _, rec := range recs {
			age = time.Since(rec.ParkedAt)
			left := p.cfg.KeepFor - age
			if left <= 0 || rec.Exit == "" || len(rec.Tokens) == 0 {
				continue
			}
			one, merr := json.Marshal(rec)
			if merr != nil {
				continue
			}
			if perr := p.cfg.Store.Put(ctx, rec.Exit, one, left); perr != nil {
				err = errors.Join(err, perr)
				continue
			}
			kept++
		}
		n += kept
		metrics.IdentityAdopted.WithLabelValues().Add(float64(kept))
		slog.Info("session: adopted a stopped crawler's identities", "identities", kept, "checkpointed", len(recs),
			"age", age.Round(time.Second).String())
	}
	return n, err
}

// RunCheckpoint checkpoints the pool every CheckpointEvery and adopts dead
// crawlers' identities every adoptEvery, until ctx ends. A failure is
// tried again at the next tick.
func (p *Pool) RunCheckpoint(ctx context.Context) error {
	if _, ok := p.cfg.Store.(CheckpointStore); !ok || p.cfg.KeepFor <= 0 {
		return nil
	}
	if _, err := p.Adopt(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("session: adopting identities failed", "err", err)
	}
	ckpt := time.NewTicker(CheckpointEvery)
	defer ckpt.Stop()
	adopt := time.NewTicker(adoptEvery)
	defer adopt.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ckpt.C:
			if _, err := p.Checkpoint(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("session: checkpoint failed", "err", err)
			}
		case <-adopt.C:
			if _, err := p.Adopt(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("session: adopting identities failed", "err", err)
			}
		}
	}
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
