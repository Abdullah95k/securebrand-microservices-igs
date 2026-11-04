// Package chmigrations embeds the ClickHouse schema of the shared content
// store (architecture §6), applied in order by igsctl clickhouse migrate.
package chmigrations

import "embed"

// FS holds the numbered .sql files. Statements end with a semicolon on its
// own line's end; ENGINE clauses are rewritten for a replicated cluster.
//
//go:embed *.sql
var FS embed.FS
