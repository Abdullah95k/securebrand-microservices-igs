// Package igmigrations embeds igs's goose SQL migrations. They create and
// change the tables of the Postgres schema "ig" (ADR 0005 §4); goose keeps
// their version table there too (ig.goose_db_version), so igs and fbs
// migrate independently.
package igmigrations

import "embed"

//go:embed *.sql
var FS embed.FS
