package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Claims that overlap get distinct DB indexes, as many as there are, and
// each goes back when its test ends. (The random pick this replaced gave
// 15 stacks 14 indexes: some two always shared one, and the second's
// FLUSHDB wiped the first's state.) The claims here live under their own
// namespace and flush nothing, so the e2e stacks of test binaries running
// meanwhile neither wait for them nor lose their indexes.
func TestRedisClaimsAreExclusive(t *testing.T) {
	addr := os.Getenv("IGS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IGS_TEST_REDIS_ADDR not set")
	}
	prev := claimNamespace
	claimNamespace = fmt.Sprintf("testenv-test-%d:redis-db:", rand.Int64())
	t.Cleanup(func() { claimNamespace = prev })
	ctl := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = ctl.Close() }()
	ctx := context.Background()

	var held []int
	t.Run("all", func(t *testing.T) {
		seen := map[int]bool{}
		for range redisDBs {
			db := claimRedisDB(t, addr)
			if db < 1 || db > redisDBs || seen[db] {
				t.Fatalf("DB %d handed out (held: %v)", db, held)
			}
			seen[db] = true
			held = append(held, db)
		}
		for _, db := range held {
			if n, _ := ctl.Exists(ctx, redisClaim(db)).Result(); n != 1 {
				t.Errorf("DB %d: no claim key while held", db)
			}
		}
	})
	if len(held) != redisDBs {
		t.Fatalf("%d indexes claimed, want %d", len(held), redisDBs)
	}
	for _, db := range held {
		if n, _ := ctl.Exists(ctx, redisClaim(db)).Result(); n != 0 {
			t.Errorf("DB %d still claimed after its test ended", db)
		}
	}
}
