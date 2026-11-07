// Package testenv gives integration tests access to real infrastructure.
// Tests skip unless the IGS_TEST_* variables are set (make test-integration
// starts the containers and sets them).
package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// Postgres creates a fresh, empty database for the test and returns a pool
// on it. Requires IGS_TEST_PG_DSN (a DSN to an admin database).
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("IGS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("IGS_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	admin, err := pg.Open(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("igs_test_%d", rand.Int64N(1<<40))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	testDSN := replaceDB(dsn, name)
	pool, err := pg.Open(ctx, testDSN, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	return pool
}

// PostgresIG creates a fresh database for the test with igs's schema (ig)
// migrated, and returns a pool pinned to schema ig, as igs's binaries use
// it. Requires IGS_TEST_PG_DSN.
func PostgresIG(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pub := Postgres(t)
	ctx := context.Background()
	if err := store.MigrateIGUp(ctx, pub); err != nil {
		t.Fatal(err)
	}
	pool, err := pg.OpenSchema(ctx, pub.Config().ConnString(), 10, store.IGSchema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// The per-component roles exist on the test server (they are the
	// server's, shared by every test database) and get their grants here,
	// as igsctl migrate up gives them in production.
	ensureRoles(t, pool)
	if _, err := store.ApplyRoleGrants(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// ensureRoles creates the per-component roles once per test server. Tests
// run at once in several packages, so a role another test just created is
// fine.
func ensureRoles(t testing.TB, db *pgxpool.Pool) {
	t.Helper()
	for _, role := range store.RoleNames() {
		_, err := db.Exec(context.Background(), `DO $$ BEGIN CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` NOLOGIN;
			EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$`)
		if err != nil {
			t.Fatalf("create role %s: %v", role, err)
		}
	}
}

func replaceDB(dsn, name string) string {
	q := ""
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		dsn, q = dsn[:i], dsn[i:]
	}
	i := strings.LastIndexByte(dsn, '/')
	return dsn[:i+1] + name + q
}

// Redis returns a client on a Valkey DB index no other test holds, flushed.
// Requires IGS_TEST_REDIS_ADDR.
//
// go test runs packages' test binaries at once against the one Valkey
// (internal/e2e and internal/ig/e2e both start stacks), so an index picked
// at random would sometimes be one another binary's stack is using, and the
// FLUSHDB below would wipe its jobs, passes and post caches mid-test. Each
// call claims its index instead (redisClaims), and holds it until the test
// ends.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("IGS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("IGS_TEST_REDIS_ADDR not set")
	}
	db := claimRedisDB(t, addr)
	c := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Valkey's DB indexes 1..redisDBs are handed out; DB 0 holds the claims,
// and scripts/chart-smoke.py uses 14 (igs) and 15 (fbs).
const (
	redisDBs        = 13
	redisClaimTTL   = 30 * time.Second
	redisClaimWait  = 5 * time.Minute
	redisClaimRenew = redisClaimTTL / 3
)

// claimNamespace prefixes the claim keys; the claims' own test swaps it so
// as not to compete with the stacks of test binaries running meanwhile.
var claimNamespace = "testenv:redis-db:"

// redisClaim is the key in DB 0 whose holder owns DB index n.
func redisClaim(n int) string { return claimNamespace + strconv.Itoa(n) }

var (
	renewClaim   = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) end return 0`)
	releaseClaim = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`)
)

// claimRedisDB claims a free DB index for the test: SET NX on its claim
// key, with a lease renewed while the test runs (a test binary that dies
// leaves its index to others within redisClaimTTL), released at cleanup.
// It waits up to redisClaimWait for an index when all are held.
func claimRedisDB(t testing.TB, addr string) int {
	t.Helper()
	ctl := redis.NewClient(&redis.Options{Addr: addr, DB: 0})
	ctx := context.Background()
	token := fmt.Sprintf("%d-%d", os.Getpid(), rand.Int64())
	deadline := time.Now().Add(redisClaimWait)
	db := 0
	for db == 0 {
		for _, i := range rand.Perm(redisDBs) {
			ok, err := ctl.SetNX(ctx, redisClaim(i+1), token, redisClaimTTL).Result()
			if err != nil {
				_ = ctl.Close()
				t.Fatal(err)
			}
			if ok {
				db = i + 1
				break
			}
		}
		if db == 0 {
			if time.Now().After(deadline) {
				_ = ctl.Close()
				t.Fatalf("testenv: all %d Valkey DB indexes held by other tests for %s", redisDBs, redisClaimWait)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(redisClaimRenew)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = renewClaim.Run(ctx, ctl, []string{redisClaim(db)}, token, redisClaimTTL.Milliseconds()).Err()
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-stopped
		_ = releaseClaim.Run(ctx, ctl, []string{redisClaim(db)}, token).Err()
		_ = ctl.Close()
	})
	return db
}

// Kafka returns the broker list. Requires IGS_TEST_KAFKA_BROKERS.
func Kafka(t testing.TB) []string {
	t.Helper()
	b := os.Getenv("IGS_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("IGS_TEST_KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

// TopicPrefix returns a unique prefix so tests sharing a broker never see
// each other's records.
func TopicPrefix() string { return fmt.Sprintf("t%d.", rand.Int64N(1<<40)) }
