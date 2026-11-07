package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// Open connects to the store, for `igsctl clickhouse migrate`. igs no longer
// writes to ClickHouse: the platform's writer fills the shared store from
// the contract's content.* topics (REQ-019).
func Open(ctx context.Context, c config.ClickHouse) (driver.Conn, error) {
	o := &clickhouse.Options{Addr: c.Addr, Auth: clickhouse.Auth{Database: c.Database, Username: c.Username, Password: c.Password}}
	if c.Secure {
		o.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	conn, err := clickhouse.Open(o)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return conn, nil
}
