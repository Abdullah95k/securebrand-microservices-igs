package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/content/clickhouse"
)

// ClickHouse returns the settings of a fresh database on the test server,
// dropped at cleanup. Requires IGS_TEST_CLICKHOUSE_ADDR (host:9000);
// IGS_TEST_CLICKHOUSE_USER and _PASSWORD when the server needs them.
func ClickHouse(t testing.TB) config.ClickHouse {
	t.Helper()
	addr := os.Getenv("IGS_TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("IGS_TEST_CLICKHOUSE_ADDR not set")
	}
	c := config.ClickHouse{Addr: strings.Split(addr, ","), Database: "default", Username: os.Getenv("IGS_TEST_CLICKHOUSE_USER"),
		Password: os.Getenv("IGS_TEST_CLICKHOUSE_PASSWORD"), BatchSize: 1000, Linger: 100 * time.Millisecond}
	if c.Username == "" {
		c.Username = "default"
	}
	ctx := context.Background()
	admin, err := clickhouse.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	c.Database = fmt.Sprintf("igs_test_%d", rand.Int64N(1<<40))
	if err := admin.Exec(ctx, "CREATE DATABASE "+c.Database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+c.Database)
		_ = admin.Close()
	})
	return c
}
