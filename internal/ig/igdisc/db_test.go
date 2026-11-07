package igdisc

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// db returns a fresh database with igs's and igdisc's migrations applied,
// pinned to schema ig. Requires IGS_TEST_PG_DSN.
func db(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testenv.PostgresIG(t)
	if err := MigrateUp(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// igdisc's migrations keep their own goose versions, beside igs's, and go
// down and up again cleanly.
func TestMigrations(t *testing.T) {
	pool := db(t)
	ctx := context.Background()
	st, err := MigrateStatus(ctx, pool)
	if err != nil || len(st) == 0 || !strings.Contains(st[0], "igdisc 00001 applied") {
		t.Fatalf("status %v %v", st, err)
	}
	if n := count(t, pool, `SELECT count(*) FROM igdisc_goose_db_version WHERE version_id = 1`); n != 1 {
		t.Fatalf("igdisc's version table holds %d rows for 00001", n)
	}
	if err := MigrateDown(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM information_schema.tables WHERE table_name = 'igdisc_evidence'`); n != 0 {
		t.Fatal("igdisc_evidence survived down")
	}
	if err := MigrateUp(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// igs's own migrations are untouched by igdisc's.
	if n := count(t, pool, `SELECT count(*) FROM goose_db_version WHERE version_id > 0`); n == 0 {
		t.Fatal("igs's version table is empty")
	}
}

// A flush writes new evidence for accounts not on the schedule and not
// rejected, creates or updates their candidates (origin keyword, priority
// raised), and is idempotent: the same evidence again changes nothing.
func TestFlush(t *testing.T) {
	pool := db(t)
	ctx := context.Background()
	exec(t, pool, `INSERT INTO profile_schedule (profile_ref, user_pk, next_run_at) VALUES ('u:scheduled_one', 801, now()),
		('u:by_pk_only', 802, now())`)
	exec(t, pool, `INSERT INTO discovery_candidates (username, status) VALUES ('rejected_one', 'rejected')`)
	exec(t, pool, `INSERT INTO discovery_candidates (username, tags, priority, origins) VALUES ('existing_one', 1, 3, '{network}')`)

	r, err := NewRecorder(pool, Config{Window: 6 * time.Hour, Commenters: true})
	if err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return now }
	p := post("Baghdad nights", "", time.Hour)
	p.Usertags = nil
	p.Coauthors = nil
	p.Mentions = []string{"new_one", "existing_one", "scheduled_one", "rejected_one"}
	r.Post(p)
	r.Comments(501, []*igv1.Comment{comment(9, "renamed_scheduled", 802, "من البصرة", time.Hour)})
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	rows, _ := pool.Query(ctx, `SELECT username || ' ' || keyword_id || ' ' || via FROM igdisc_evidence ORDER BY 1`)
	got, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"existing_one baghdad:en:baghdad caption", "new_one baghdad:en:baghdad caption"}
	if !slices.Equal(got, want) {
		t.Fatalf("evidence %v, want %v", got, want)
	}
	type cand struct {
		priority float64
		origins  []string
		status   string
	}
	read := func(u string) cand {
		var c cand
		if err := pool.QueryRow(ctx, `SELECT priority, origins, status FROM discovery_candidates WHERE username = $1`, u).
			Scan(&c.priority, &c.origins, &c.status); err != nil {
			t.Fatalf("%s: %v", u, err)
		}
		return c
	}
	if c := read("new_one"); c.priority != 2 || !slices.Equal(c.origins, []string{"keyword"}) || c.status != "new" {
		t.Errorf("new_one %+v", c)
	}
	if c := read("existing_one"); c.priority != 5 || !slices.Equal(c.origins, []string{"network", "keyword"}) {
		t.Errorf("existing_one %+v", c)
	}
	if n := count(t, pool, `SELECT count(*) FROM discovery_candidates WHERE username IN ('scheduled_one', 'renamed_scheduled')`); n != 0 {
		t.Errorf("%d scheduled accounts became candidates", n)
	}
	if c := read("rejected_one"); c.priority != 0 || len(c.origins) != 0 {
		t.Errorf("rejected_one changed: %+v", c)
	}

	// The same content again: nothing new.
	r.Post(p)
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM igdisc_evidence`); n != 2 {
		t.Errorf("%d rows after a repeat", n)
	}
	if c := read("existing_one"); c.priority != 5 || !slices.Equal(c.origins, []string{"network", "keyword"}) {
		t.Errorf("existing_one after a repeat %+v", c)
	}

	// A later read that knows the user pk fills it in, without raising the
	// priority again.
	p2 := post("Baghdad nights", "", time.Hour)
	p2.Coauthors, p2.Mentions = nil, nil
	p2.Usertags = []*igv1.UserTag{{UserPk: 901, Username: "new_one"}}
	r.Post(p2)
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM igdisc_evidence WHERE username = 'new_one' AND user_pk = 901`); n != 1 {
		t.Errorf("new_one's user pk not filled in")
	}
	if c := read("new_one"); c.priority != 2 {
		t.Errorf("new_one's priority %v after a repeat", c.priority)
	}
}

// KeywordContext counts the places each account was met near: one place
// in several spellings counts once.
func TestKeywordContext(t *testing.T) {
	pool := db(t)
	ctx := context.Background()
	r, err := NewRecorder(pool, Config{Window: 6 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return now }
	one := post("جلسة تصوير في البصرة", "", time.Hour) // البصرة and بصرة: one place
	one.Usertags, one.Coauthors, one.Mentions = nil, nil, []string{"one_place"}
	r.Post(one)
	two := post("من بغداد الى اربيل", "", time.Hour)
	two.MediaPk = 3000000000000000002
	two.Usertags, two.Coauthors, two.Mentions = nil, nil, []string{"two_places"}
	r.Post(two)
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := KeywordContextFunc(pool)(ctx, []string{"one_place", "two_places", "nobody"})
	if err != nil {
		t.Fatal(err)
	}
	if got["one_place"] != 1 || got["two_places"] != 2 || len(got) != 2 {
		t.Fatalf("%v", got)
	}
	if empty, err := KeywordContext(ctx, pool, nil); err != nil || len(empty) != 0 {
		t.Fatalf("%v %v", empty, err)
	}
}

// Erasure deletes the evidence about an erased account, and the evidence
// found on an erased profile's posts; an erased comment author's too.
func TestErase(t *testing.T) {
	pool := db(t)
	ctx := context.Background()
	exec(t, pool, `INSERT INTO igdisc_evidence (username, user_pk, keyword_id, keyword, normalized, language, grp, content_id, content_time, via, source_pk)
		VALUES ('by_pk', 11, 'baghdad:en:baghdad', 'Baghdad', 'baghdad', 'en', 'baghdad', 'ig:post:1', now(), 'caption', 501),
		('by_name', NULL, 'baghdad:en:baghdad', 'Baghdad', 'baghdad', 'en', 'baghdad', 'ig:post:1', now(), 'caption', 501),
		('on_its_post', 12, 'baghdad:en:baghdad', 'Baghdad', 'baghdad', 'en', 'baghdad', 'ig:post:2', now(), 'caption', 22),
		('author', 33, 'basra:ar:البصره', 'البصرة', 'البصره', 'ar', 'basra', 'ig:comment:3', now(), 'comment', 501),
		('kept', 44, 'baghdad:en:baghdad', 'Baghdad', 'baghdad', 'en', 'baghdad', 'ig:post:1', now(), 'caption', 501)`)
	exec(t, pool, `INSERT INTO discovery_candidates (username, user_pk) VALUES ('by_name', 21)`)
	exec(t, pool, `INSERT INTO comment_authors (author_key, author_pk, first_seen_at, last_seen_at) VALUES ('\x0a0b', 33, now(), now())`)
	erase := func(f func(pgx.Tx) (int64, error)) int64 {
		var n int64
		if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var err error
			n, err = f(tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := erase(func(tx pgx.Tx) (int64, error) { return EraseProfile(ctx, tx, 11) }); n != 1 {
		t.Errorf("by user pk: %d rows", n)
	}
	if n := erase(func(tx pgx.Tx) (int64, error) { return EraseProfile(ctx, tx, 21) }); n != 1 {
		t.Errorf("by its candidate's username: %d rows", n)
	}
	if n := erase(func(tx pgx.Tx) (int64, error) { return EraseProfile(ctx, tx, 22) }); n != 1 {
		t.Errorf("on its posts: %d rows", n)
	}
	if n := erase(func(tx pgx.Tx) (int64, error) { return EraseAuthor(ctx, tx, []byte{0x0a, 0x0b}) }); n != 1 {
		t.Errorf("author: %d rows", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM igdisc_evidence`); n != 1 {
		t.Errorf("%d rows left, want only 'kept'", n)
	}
}

// KeywordsGrown finds the candidates decided below the threshold whose
// keyword signal would now be higher: evidence that arrived after their
// decision. Once their stored signal matches, they drop out.
func TestKeywordsGrown(t *testing.T) {
	pool := db(t)
	ctx := context.Background()
	exec(t, pool, `INSERT INTO discovery_candidates (username, status, vet, signals) VALUES
		('grew', 'below', '{}', '{"keyword_context": 0.5}'),
		('same', 'below', '{}', '{"keyword_context": 0.5}'),
		('never_scored', 'below', '{}', '{}'),
		('old_evidence', 'below', '{}', '{}'),
		('registered_one', 'registered', '{}', '{}'),
		('vetting_one', 'vetting', NULL, NULL)`)
	ev := func(user, grp string, found string) {
		exec(t, pool, `INSERT INTO igdisc_evidence (username, keyword_id, keyword, normalized, language, grp, content_id, content_time,
			via, source_pk, found_at) VALUES ($1, $2 || ':en:x', 'x', 'x', 'en', $2, 'ig:post:' || $2, now(), 'caption', 1, now() - $3::interval)`,
			user, grp, found)
	}
	ev("grew", "baghdad", "0 hours")
	ev("grew", "basra", "0 hours")
	ev("same", "erbil", "0 hours")
	ev("never_scored", "mosul", "1 hour")
	ev("old_evidence", "kirkuk", "48 hours")
	ev("registered_one", "baghdad", "0 hours")
	ev("vetting_one", "baghdad", "0 hours")
	got, err := KeywordsGrownFunc(pool, 0.5, 1)(ctx)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if want := []string{"grew", "never_scored"}; !slices.Equal(got, want) {
		t.Fatalf("grown %v, want %v", got, want)
	}
	exec(t, pool, `UPDATE discovery_candidates SET signals = '{"keyword_context": 1}' WHERE username = 'grew'`)
	exec(t, pool, `UPDATE discovery_candidates SET signals = '{"keyword_context": 0.5}' WHERE username = 'never_scored'`)
	if got, err := KeywordsGrown(ctx, pool, 0.5, 1); err != nil || len(got) != 0 {
		t.Fatalf("after rescoring: %v %v", got, err)
	}
}
