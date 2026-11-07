package igdisc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	igdiscmigrations "github.com/abdullah95k/securebrand-microservices-igs/migrations/igdisc"
)

// Schema is where igdisc's tables live: igs's schema, with names starting
// igdisc_.
const Schema = "ig"

// versionTable keeps igdisc's goose versions apart from igs's
// (ig.goose_db_version), so the two sets of migrations never share numbers.
const versionTable = Schema + ".igdisc_goose_db_version"

func provider(ctx context.Context, pool *pgxpool.Pool) (*goose.Provider, *sql.DB, error) {
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+Schema); err != nil {
		return nil, nil, fmt.Errorf("igdisc: create schema %s: %w", Schema, err)
	}
	db := stdlib.OpenDBFromPool(pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, igdiscmigrations.FS, goose.WithTableName(versionTable))
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("igdisc: goose: %w", err)
	}
	return p, db, nil
}

// MigrateUp applies igdisc's pending migrations. Run it after igs's
// (store.MigrateIGUp). The pool may have any search path: the migrations
// set their own.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("igdisc: migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back igdisc's last migration.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	p, db, err := provider(ctx, pool)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := p.Down(ctx); err != nil {
		return fmt.Errorf("igdisc: migrate down: %w", err)
	}
	return nil
}

// MigrateStatus returns "version: state path" lines for igdisc's
// migrations.
func MigrateStatus(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	p, db, err := provider(ctx, pool)
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
		out = append(out, fmt.Sprintf("igdisc %05d %s %s", s.Source.Version, s.State, s.Source.Path))
	}
	return out, nil
}
