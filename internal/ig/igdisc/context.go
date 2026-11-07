package igdisc

import (
	"context"
	"time"

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

// grownLookback: KeywordsGrown looks at accounts with evidence found this
// recently. The vetter asks every tick (30 seconds), so older evidence was
// already seen; the evidence table, kept forever, is not scanned whole.
const grownLookback = 24 * time.Hour

// KeywordsGrown returns candidates decided below the threshold whose
// keyword_context signal would now be higher than the one in their stored
// score, because evidence arrived after their decision: discovery's vetter
// scores them again from their stored read, with no new read of Instagram.
// weight and limit are the signal's (discovery.KeywordWeight and
// KeywordCap). Once rescored, a candidate's stored signal matches, so it
// isn't returned again until more evidence arrives.
func KeywordsGrown(ctx context.Context, db *pgxpool.Pool, weight, limit float64) ([]string, error) {
	rows, err := db.Query(ctx, `WITH recent AS (
			SELECT DISTINCT username FROM igdisc_evidence WHERE found_at > now() - $3::int * interval '1 second'
		), k AS (
			SELECT e.username, count(DISTINCT e.grp) AS n FROM igdisc_evidence e JOIN recent r ON r.username = e.username
			GROUP BY e.username
		)
		SELECT c.username FROM discovery_candidates c JOIN k ON k.username = c.username
		WHERE c.status = 'below' AND c.vet IS NOT NULL
			AND coalesce((c.signals->>'keyword_context')::float8, 0) < least($1::float8 * k.n, $2::float8)
		LIMIT 1000`, weight, limit, int(grownLookback.Seconds()))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// KeywordsGrownFunc binds KeywordsGrown to db and the signal's weights, in
// the form discovery's vetter takes.
func KeywordsGrownFunc(db *pgxpool.Pool, weight, limit float64) func(context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		return KeywordsGrown(ctx, db, weight, limit)
	}
}
