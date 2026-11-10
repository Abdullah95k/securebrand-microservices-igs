package normalize

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// Volume is the contract's volume rule for counts (README §6): Metrics are
// published when a count changed, and at least once a day for every item
// still refreshed. igs reads posts and comments far more often than their
// counts change, so publishing every read would multiply rows without new
// information.
type Volume interface {
	// Publish reports whether counts hashed as hash for target should be
	// published on day, and remembers them.
	Publish(ctx context.Context, target, hash, day string) (bool, error)
}

// Filter drops the Metrics the volume rule holds back. Everything else
// passes. A Volume that fails lets the Metrics through: a duplicate costs a
// row, a dropped change loses a count.
func Filter(ctx context.Context, v Volume, outs []Out) []Out {
	if v == nil {
		return outs
	}
	kept := outs[:0]
	for _, o := range outs {
		if c, ok := o.Msg.(*discoveryv1.Candidate); ok {
			// A link candidate: once a day per link.
			day := time.Now().UTC().Format(time.DateOnly)
			if t := c.GetFoundAt(); t != nil {
				day = t.AsTime().UTC().Format(time.DateOnly)
			}
			publish, err := v.Publish(ctx, "candidate:"+o.Key, "", day)
			if err != nil {
				slog.Warn("igs normalizer: volume rule unavailable; publishing", "err", err)
				publish = true
			}
			if publish {
				kept = append(kept, o)
			}
			continue
		}
		m, ok := o.Msg.(*contentv1.Metrics)
		if !ok {
			kept = append(kept, o)
			continue
		}
		env := m.GetEnvelope()
		day := time.Now().UTC().Format(time.DateOnly)
		if t := env.GetObservedAt(); t != nil {
			day = t.AsTime().UTC().Format(time.DateOnly)
		}
		publish, err := v.Publish(ctx, env.GetId(), env.GetContentHash(), day)
		if err != nil {
			slog.Warn("igs normalizer: volume rule unavailable; publishing", "err", err)
			publish = true
		}
		if publish {
			kept = append(kept, o)
		}
	}
	return kept
}

// volumeTTL outlasts a day, so the first counts of a new day still compare
// with the last of the one before.
const volumeTTL = 48 * time.Hour

// RedisVolume keeps the last published counts per target in Valkey, so
// every normalizer pod applies the rule the same way.
type RedisVolume struct{ C redis.UniversalClient }

// Publish swaps in "<hash>|<day>" and publishes when the old value differs.
func (r RedisVolume) Publish(ctx context.Context, target, hash, day string) (bool, error) {
	old, err := r.C.SetArgs(ctx, redisx.KeyPrefix+"content:metrics:"+target, hash+"|"+day,
		redis.SetArgs{TTL: volumeTTL, Get: true}).Result()
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return old != hash+"|"+day, nil
}

// MemVolume is the volume rule in memory, for one process and tests.
type MemVolume struct {
	mu   sync.Mutex
	last map[string]string
}

// Publish reports whether target's counts changed since their last
// publication, or the day did.
func (m *MemVolume) Publish(_ context.Context, target, hash, day string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == nil {
		m.last = map[string]string{}
	}
	v := hash + "|" + day
	if m.last[target] == v {
		return false, nil
	}
	m.last[target] = v
	return true, nil
}
