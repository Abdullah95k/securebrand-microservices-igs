package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	igmigrations "github.com/abdullah95k/securebrand-microservices-igs/migrations/ig"
)

// IGSchema is the Postgres schema of igs's tables (ADR 0005 §4).
const IGSchema = "ig"

// igVersionTable keeps goose's versions inside the schema, so igs and fbs
// migrate independently even in one database.
const igVersionTable = IGSchema + ".goose_db_version"

// IGLayout is igs's partitioned tables: posts and comments by the post's
// month, profile snapshots by month, metrics by day.
var IGLayout = Layout{
	Monthly:    []string{"posts", "comments", "profile_snapshots"},
	Daily:      []string{"post_metrics"},
	FillFactor: map[string]bool{"posts": true, "comments": true},
	LZ4: map[string][]string{
		"posts":    {"caption", "usertags", "extra"},
		"comments": {"text", "extra"},
	},
}

func igProvider(ctx context.Context, pool *pgxpool.Pool) (*goose.Provider, *sql.DB, error) {
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+IGSchema); err != nil {
		return nil, nil, fmt.Errorf("store: create schema %s: %w", IGSchema, err)
	}
	db := stdlib.OpenDBFromPool(pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, igmigrations.FS, goose.WithTableName(igVersionTable))
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("store: goose: %w", err)
	}
	return p, db, nil
}

// MigrateIGUp creates schema ig if needed and applies igs's pending
// migrations. The pool may have any search path: the migrations set their
// own.
func MigrateIGUp(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := igProvider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate ig up: %w", err)
	}
	return nil
}

// MigrateIGDown rolls back igs's last migration.
func MigrateIGDown(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := igProvider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Down(ctx); err != nil {
		return fmt.Errorf("store: migrate ig down: %w", err)
	}
	return nil
}

// MigrateIGStatus returns "version: applied" lines for igs's migrations.
func MigrateIGStatus(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	p, db, err := igProvider(ctx, pool)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	st, err := p.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(st))
	for _, s := range st {
		out = append(out, fmt.Sprintf("%05d %s %s", s.Source.Version, s.State, s.Source.Path))
	}
	return out, nil
}
