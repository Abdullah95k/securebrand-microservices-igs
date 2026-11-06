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

// Checkpoints (session.CheckpointStore): a running crawler keeps every
// identity it holds in one snapshot under a lease, so one that dies leaves
// them for the others. The keys share a hash tag, for the scripts.
func (p *ParkedIdentities) ckptKey(owner string) string {
	return redisx.KeyPrefix + p.ns + "{identities}:ckpt:" + owner
}

func (p *ParkedIdentities) leaseKey(owner string) string {
	return redisx.KeyPrefix + p.ns + "{identities}:lease:" + owner
}

func (p *ParkedIdentities) ownersKey() string {
	return redisx.KeyPrefix + p.ns + "{identities}:owners"
}

// Checkpoint replaces owner's snapshot (kept keep) and renews its lease.
func (p *ParkedIdentities) Checkpoint(ctx context.Context, owner string, data []byte, lease, keep time.Duration) error {
	pipe := p.rdb.TxPipeline()
	pipe.Set(ctx, p.ckptKey(owner), data, lease+keep)
	pipe.Set(ctx, p.leaseKey(owner), 1, lease)
	pipe.SAdd(ctx, p.ownersKey(), owner)
	pipe.PExpire(ctx, p.ownersKey(), 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// Release drops owner's snapshot and lease: a crawler that stops cleanly
// parks its identities instead.
func (p *ParkedIdentities) Release(ctx context.Context, owner string) error {
	pipe := p.rdb.TxPipeline()
	pipe.Del(ctx, p.ckptKey(owner), p.leaseKey(owner))
	pipe.SRem(ctx, p.ownersKey(), owner)
	_, err := pipe.Exec(ctx)
	return err
}

// takeOrphan takes an owner's snapshot once its lease has ended; nil while
// the lease holds, or if another caller took it.
var takeOrphan = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then return false end
local v = redis.call('GET', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[3], ARGV[1])
return v`)

// TakeOrphans removes and returns the snapshots of owners other than self
// whose lease has ended: crawlers that died without parking. Each goes to
// one caller.
func (p *ParkedIdentities) TakeOrphans(ctx context.Context, self string) ([][]byte, error) {
	owners, err := p.rdb.SMembers(ctx, p.ownersKey()).Result()
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, o := range owners {
		if o == self {
			continue
		}
		v, err := takeOrphan.Run(ctx, p.rdb, []string{p.ckptKey(o), p.leaseKey(o), p.ownersKey()}, o).Text()
		switch {
		case errors.Is(err, redis.Nil):
			continue
		case err != nil:
			return out, err
		case v != "":
			out = append(out, []byte(v))
		}
	}
	return out, nil
}
