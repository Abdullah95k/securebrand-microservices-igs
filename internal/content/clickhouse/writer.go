package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// Inserter inserts rows into a table: ClickHouse, or a fake in tests.
type Inserter interface {
	Insert(ctx context.Context, table string, cols []string, rows [][]any) error
}

// Write inserts every table's rows, tables in a fixed order.
func Write(ctx context.Context, ins Inserter, r Rows) error {
	tables := make([]string, 0, len(r))
	for t := range r {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		if len(r[t]) == 0 {
			continue
		}
		if err := ins.Insert(ctx, t, columns[t], r[t]); err != nil {
			return fmt.Errorf("clickhouse: %s: %w", t, err)
		}
	}
	return nil
}

// Open connects to the store.
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

// Conn inserts through a ClickHouse connection, one native batch per table.
type Conn struct{ C driver.Conn }

func (c Conn) Insert(ctx context.Context, table string, cols []string, rows [][]any) error {
	b, err := c.C.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(cols, ", ")+")")
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(r...); err != nil {
			_ = b.Abort()
			return err
		}
	}
	return b.Send()
}
