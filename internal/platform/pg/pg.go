// Package pg opens pgx connection pools.
package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open creates a pool and verifies connectivity.
func Open(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// OpenSchema creates a pool whose connections all use schema as their
// search path (igs's tables live in schema "ig", ADR 0005 §4), and checks
// at start that the schema exists and is the current one, so a mistake can
// never point a service at another service's tables of the same name.
func OpenSchema(ctx context.Context, dsn string, maxConns int32, schema string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	var current *string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&current); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	if current == nil || *current != schema {
		pool.Close()
		return nil, fmt.Errorf("pg: schema %q does not exist (current schema %v); run the migrations first", schema, current)
	}
	return pool, nil
}
