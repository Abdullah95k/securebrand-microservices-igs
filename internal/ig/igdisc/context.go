package igdisc

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeywordContext returns, for each of usernames that has evidence, how
// many different places of the shared list (its groups: Iraq, the
// Kurdistan Region, a governorate) it was met near: discovery's
// keyword_context signal. One place in several spellings or languages
// (Basra, البصرة, بصرة) counts once, so a single mention can't reach the
// signal's cap alone.
func KeywordContext(ctx context.Context, db *pgxpool.Pool, usernames []string) (map[string]int, error) {
	out := map[string]int{}
	if len(usernames) == 0 {
		return out, nil
	}
	rows, err := db.Query(ctx, `SELECT username, count(DISTINCT grp)::int FROM igdisc_evidence
		WHERE username = ANY($1) GROUP BY username`, usernames)
	if err != nil {
		return nil, err
	}
	type row struct {
		username string
		n        int
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.username, &x.n)
	})
	if err != nil {
		return nil, err
	}
	for _, x := range list {
		out[x.username] = x.n
	}
	return out, nil
}

// KeywordContextFunc binds KeywordContext to db, in the form discovery's
// vetter takes.
func KeywordContextFunc(db *pgxpool.Pool) func(context.Context, []string) (map[string]int, error) {
	return func(ctx context.Context, usernames []string) (map[string]int, error) {
		return KeywordContext(ctx, db, usernames)
	}
}
