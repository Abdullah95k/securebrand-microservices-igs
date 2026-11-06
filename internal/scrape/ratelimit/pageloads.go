package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// PageLoads counts page loads per exit and UTC day across the fleet, in
// one Valkey hash a day (exit -> count) kept two days: the count behind
// ig.exit_page_loads_per_day.
type PageLoads struct {
	rdb *redis.Client
	key string
}

// NewPageLoads keeps the counts under ns (the topic prefix, as the other
// fleet-wide keys).
func NewPageLoads(rdb *redis.Client, ns string) *PageLoads {
	return &PageLoads{rdb: rdb, key: redisx.KeyPrefix + ns + "exit:pageloads:"}
}

var addPageLoad = redis.NewScript(`
local n = redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
redis.call('PEXPIRE', KEYS[1], 172800000)
return n`)

// Add counts one page load on exit on now's UTC day and returns the day's
// count so far.
func (p *PageLoads) Add(ctx context.Context, exit string, now time.Time) (int64, error) {
	return addPageLoad.Run(ctx, p.rdb, []string{p.day(now)}, exit).Int64()
}

// Day returns each exit's page loads on now's UTC day.
func (p *PageLoads) Day(ctx context.Context, now time.Time) (map[string]int64, error) {
	m, err := p.rdb.HGetAll(ctx, p.day(now)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(m))
	for exit, v := range m {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		out[exit] = n
	}
	return out, nil
}

func (p *PageLoads) day(now time.Time) string { return p.key + now.UTC().Format("20060102") }
