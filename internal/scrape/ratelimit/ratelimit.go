// Package ratelimit provides the fleet-wide limiters (Redis GCRA) that keep
// the request rate to the platform bounded no matter how many pods run.
package ratelimit

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// Limiter blocks until a request under key is allowed at perSecond.
type Limiter interface {
	Wait(ctx context.Context, key string, perSecond int) error
}

// Redis is a distributed GCRA limiter. It fails closed: while Valkey does
// not answer, a request waits and the check is tried again (backing off up
// to StoreRetryMax), so no request goes out without the fleet-wide limit.
// Each identity's own pace alone would let several pods use one exit faster
// than its limit.
type Redis struct {
	l *redis_rate.Limiter
}

// StoreRetryMin and StoreRetryMax bound the wait between tries while Valkey
// does not answer.
var (
	StoreRetryMin = 250 * time.Millisecond
	StoreRetryMax = 5 * time.Second
)

func NewRedis(c *redis.Client) *Redis { return &Redis{l: redis_rate.NewLimiter(c)} }

func (r *Redis) Wait(ctx context.Context, key string, perSecond int) error {
	if perSecond <= 0 {
		return nil
	}
	return r.wait(ctx, key, redis_rate.PerSecond(perSecond))
}

// WaitRate is Wait at a rate below one a second (0.25: one request every
// 4 s), fleet-wide, with a burst of one: igs paces each proxy exit this
// way, since Instagram limits per IP.
func (r *Redis) WaitRate(ctx context.Context, key string, perSecond float64) error {
	if perSecond <= 0 {
		return nil
	}
	return r.wait(ctx, key, redis_rate.Limit{Rate: 1, Burst: 1, Period: time.Duration(float64(time.Second) / perSecond)})
}

func (r *Redis) wait(ctx context.Context, key string, limit redis_rate.Limit) error {
	backoff := StoreRetryMin
	waiting := false
	defer func() {
		if waiting {
			metrics.RateLimitWaiting.Dec()
		}
	}()
	for {
		res, err := r.l.Allow(ctx, redisx.KeyPrefix+"rl:"+key, limit)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Fail closed: wait for Valkey, never send unchecked.
			metrics.RateLimitStoreErrors.WithLabelValues(keyKind(key)).Inc()
			if !waiting {
				waiting = true
				metrics.RateLimitWaiting.Inc()
			}
			storeDownLog(key, err)
			if err := sleep(ctx, backoff); err != nil {
				return err
			}
			backoff = min(2*backoff, StoreRetryMax)
			continue
		}
		if waiting {
			waiting = false
			metrics.RateLimitWaiting.Dec()
			backoff = StoreRetryMin
		}
		if res.Allowed > 0 {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// keyKind is a limiter key's kind for the metric label: the part before
// its first colon (exit, page, global).
func keyKind(key string) string {
	k, _, _ := strings.Cut(key, ":")
	return k
}

// lastStoreLog throttles the "Valkey unavailable" warning to one every
// storeLogEvery per process: every waiting request would otherwise log.
var lastStoreLog atomic.Int64

const storeLogEvery = 30 * time.Second

func storeDownLog(key string, err error) {
	now := time.Now().UnixNano()
	last := lastStoreLog.Load()
	if now-last < int64(storeLogEvery) || !lastStoreLog.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("ratelimit: Valkey unavailable; requests wait for the fleet-wide limit", "kind", keyKind(key), "err", err)
}

// Noop never waits.
type Noop struct{}

func (Noop) Wait(context.Context, string, int) error { return nil }

// WaitRate never waits.
func (Noop) WaitRate(context.Context, string, float64) error { return nil }
