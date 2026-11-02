package testenv

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Stacks that overlap get distinct DB indexes, however many there are (up
// to the indexes there are), and an index goes back when its test ends.
// The random pick this replaced gave 15 stacks 14 indexes: some two always
// shared one, and the second's FLUSHDB wiped the first's state.
func TestRedisIndexesAreExclusive(t *testing.T) {
	addr := os.Getenv("IGS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IGS_TEST_REDIS_ADDR not set")
	}
	ctl := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = ctl.Close() }()
	ctx := context.Background()

	var held []int
	t.Run("all", func(t *testing.T) {
		seen := map[int]bool{}
		for range redisDBs {
			c := Redis(t)
			db := c.Options().DB
			if seen[db] {
				t.Fatalf("DB %d handed out twice", db)
			}
			seen[db] = true
			// The index is this test's alone: a key set now survives.
			if err := c.Set(ctx, "mine", db, 0).Err(); err != nil {
				t.Fatal(err)
			}
			held = append(held, db)
		}
		for _, db := range held {
			c := redis.NewClient(&redis.Options{Addr: addr, DB: db})
			if n, err := c.Get(ctx, "mine").Int(); err != nil || n != db {
				t.Errorf("DB %d: key %d %v; another claim flushed it", db, n, err)
			}
			_ = c.Close()
		}
	})
	for _, db := range held {
		if n, _ := ctl.Exists(ctx, redisClaim(db)).Result(); n != 0 {
			t.Errorf("DB %d still claimed after its test ended", db)
		}
	}
}
