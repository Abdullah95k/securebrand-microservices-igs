// Package igdiscmigrations embeds igdisc's goose SQL migrations: Instagram
// keyword discovery's tables in schema ig, named igdisc_*. goose keeps
// their versions in a table of their own (ig.igdisc_goose_db_version), so
// igdisc's migrations never share numbers with igs's (migrations/ig).
package igdiscmigrations

import "embed"

//go:embed *.sql
var FS embed.FS
