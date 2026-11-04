package ratelimit

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// ParkedIdentities keeps idle identities between crawler processes
// (session.Pool.Park): one Valkey list per exit, each entry taken once.
type ParkedIdentities struct {
	rdb *redis.Client
	ns  string
}

// NewParkedIdentities stores identities under ns (the topic prefix).
func NewParkedIdentities(rdb *redis.Client, ns string) *ParkedIdentities {
	return &ParkedIdentities{rdb: rdb, ns: ns}
}

func (p *ParkedIdentities) key(exit string) string {
	return redisx.KeyPrefix + p.ns + "identities:" + exit
}

// Put adds an identity parked on exit; the list expires ttl after the last
// one.
func (p *ParkedIdentities) Put(ctx context.Context, exit string, data []byte, ttl time.Duration) error {
	k := p.key(exit)
	pipe := p.rdb.TxPipeline()
	pipe.RPush(ctx, k, data)
	pipe.LTrim(ctx, k, -64, -1)
	pipe.PExpire(ctx, k, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Take removes and returns one identity parked on exit, or nil.
func (p *ParkedIdentities) Take(ctx context.Context, exit string) ([]byte, error) {
	b, err := p.rdb.LPop(ctx, p.key(exit)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}
