package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// ExitRests shares proxy exit rests between processes through one Valkey
// hash (exit -> end, Unix ms): proxy.Pool.ShareRests. Every crawler pod
// that uses the same exits sees a rest within its sync interval.
type ExitRests struct {
	rdb *redis.Client
	key string
}

// NewExitRests stores the rests under ns (the topic prefix, as the other
// fleet-wide keys).
func NewExitRests(rdb *redis.Client, ns string) *ExitRests {
	return &ExitRests{rdb: rdb, key: redisx.KeyPrefix + ns + "exit:rests"}
}

// publishRest keeps the later end of a rest, drops ended ones now and then
// and lets the hash expire a day after the last rest.
var publishRest = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if (not cur) or tonumber(cur) < tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
end
local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
  if tonumber(all[i+1]) < tonumber(ARGV[3]) then redis.call('HDEL', KEYS[1], all[i]) end
end
redis.call('PEXPIRE', KEYS[1], 86400000)
return 1`)

// Publish records that exit rests until until.
func (e *ExitRests) Publish(ctx context.Context, exit string, until time.Time) error {
	return publishRest.Run(ctx, e.rdb, []string{e.key}, exit, until.UnixMilli(), time.Now().UnixMilli()).Err()
}

// Load returns every rest recorded.
func (e *ExitRests) Load(ctx context.Context) (map[string]time.Time, error) {
	m, err := e.rdb.HGetAll(ctx, e.key).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]time.Time, len(m))
	for exit, v := range m {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		out[exit] = time.UnixMilli(ms)
	}
	return out, nil
}
