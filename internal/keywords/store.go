package keywords

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is what the store needs of a pool pinned to the schema that holds
// discovery_keywords (igs: ig).
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Keyword is one row of discovery_keywords.
type Keyword struct {
	ID         int64     `json:"id"`
	Term       string    `json:"term"`
	Normalized string    `json:"normalized"`
	Language   string    `json:"language"`
	Group      string    `json:"group"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ErrInvalid is a keyword the store refuses; ErrDuplicate one whose
// spelling folds to a keyword of the same language already listed.
var (
	ErrInvalid   = errors.New("keywords: invalid keyword")
	ErrDuplicate = errors.New("keywords: a keyword with this spelling already exists in this language")
	ErrNotFound  = errors.New("keywords: no such keyword")
)

const columns = `id, term, normalized, language, grp, enabled, created_at, updated_at`

func scan(row pgx.Row) (Keyword, error) {
	var k Keyword
	err := row.Scan(&k.ID, &k.Term, &k.Normalized, &k.Language, &k.Group, &k.Enabled, &k.CreatedAt, &k.UpdatedAt)
	return k, err
}

// maxTermLen bounds a keyword: a place name, not a sentence.
const maxTermLen = 100

func checkTerm(term, lang string) (string, error) {
	if !Languages[lang] {
		return "", fmt.Errorf("%w: language %q must be en, ar, ckb or kmr", ErrInvalid, lang)
	}
	n := Normalize(term)
	if n == "" || len([]rune(term)) > maxTermLen {
		return "", fmt.Errorf("%w: term must be 1 to %d characters", ErrInvalid, maxTermLen)
	}
	return n, nil
}

// Add adds a keyword. A spelling that folds to a keyword already listed in
// the language returns that keyword, created false, unchanged.
func Add(ctx context.Context, db DB, term, lang, group string, enabled bool) (Keyword, bool, error) {
	term = strings.TrimSpace(term)
	n, err := checkTerm(term, lang)
	if err != nil {
		return Keyword{}, false, err
	}
	k, err := scan(db.QueryRow(ctx, `INSERT INTO discovery_keywords (term, normalized, language, grp, enabled)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (normalized, language) DO NOTHING RETURNING `+columns,
		term, n, lang, strings.TrimSpace(group), enabled))
	if err == nil {
		return k, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Keyword{}, false, err
	}
	k, err = scan(db.QueryRow(ctx, `SELECT `+columns+` FROM discovery_keywords WHERE normalized = $1 AND language = $2`, n, lang))
	return k, false, err
}

// Filter narrows List.
type Filter struct {
	Language string
	Group    string
	Enabled  *bool
	AfterID  int64
	Limit    int // default 1000
}

// List lists keywords by id.
func List(ctx context.Context, db DB, f Filter) ([]Keyword, error) {
	q := `SELECT ` + columns + ` FROM discovery_keywords WHERE id > $1`
	args := []any{f.AfterID}
	if f.Language != "" {
		args = append(args, f.Language)
		q += ` AND language = $` + strconv.Itoa(len(args))
	}
	if f.Group != "" {
		args = append(args, f.Group)
		q += ` AND grp = $` + strconv.Itoa(len(args))
	}
	if f.Enabled != nil {
		args = append(args, *f.Enabled)
		q += ` AND enabled = $` + strconv.Itoa(len(args))
	}
	if f.Limit <= 0 {
		f.Limit = 1000
	}
	args = append(args, f.Limit)
	q += ` ORDER BY id LIMIT $` + strconv.Itoa(len(args))
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Keyword, error) { return scan(r) })
	if out == nil {
		out = []Keyword{}
	}
	return out, err
}

// Update changes a keyword; nil fields stay.
type Update struct {
	Term    *string
	Group   *string
	Enabled *bool
}

// Set applies u to keyword id.
func Set(ctx context.Context, db DB, id int64, u Update) (Keyword, error) {
	var term, norm *string
	if u.Term != nil {
		var lang string
		if err := db.QueryRow(ctx, `SELECT language FROM discovery_keywords WHERE id = $1`, id).Scan(&lang); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return Keyword{}, ErrNotFound
			}
			return Keyword{}, err
		}
		t := strings.TrimSpace(*u.Term)
		n, err := checkTerm(t, lang)
		if err != nil {
			return Keyword{}, err
		}
		term, norm = &t, &n
	}
	var group *string
	if u.Group != nil {
		g := strings.TrimSpace(*u.Group)
		group = &g
	}
	k, err := scan(db.QueryRow(ctx, `UPDATE discovery_keywords SET term = COALESCE($2, term), normalized = COALESCE($3, normalized),
		grp = COALESCE($4, grp), enabled = COALESCE($5, enabled), updated_at = now() WHERE id = $1 RETURNING `+columns,
		id, term, norm, group, u.Enabled))
	var pe *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Keyword{}, ErrNotFound
	case errors.As(err, &pe) && pe.Code == "23505":
		return Keyword{}, ErrDuplicate
	}
	return k, err
}

// Delete removes keyword id.
func Delete(ctx context.Context, db DB, id int64) error {
	tag, err := db.Exec(ctx, `DELETE FROM discovery_keywords WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ImportSeed adds the seed's keywords that are not listed yet. Keywords
// already listed keep their state: a keyword someone disabled stays so.
func ImportSeed(ctx context.Context, db DB, seed []SeedKeyword) (added int, err error) {
	for _, s := range seed {
		_, created, err := Add(ctx, db, s.Term, s.Language, s.Group, s.Enabled)
		if err != nil {
			return added, fmt.Errorf("%s %q: %w", s.Language, s.Term, err)
		}
		if created {
			added++
		}
	}
	return added, nil
}

// Load builds a matcher from the table's enabled keywords. ok is false
// when the table is empty (the seed was never imported), so a caller keeps
// the embedded seed's matcher instead of matching nothing.
func Load(ctx context.Context, db DB) (m *Matcher, ok bool, err error) {
	rows, err := db.Query(ctx, `SELECT normalized, grp, enabled FROM discovery_keywords`)
	if err != nil {
		return nil, false, err
	}
	var terms []Term
	n := 0
	for rows.Next() {
		var t Term
		var enabled bool
		if err := rows.Scan(&t.Normalized, &t.Group, &enabled); err != nil {
			rows.Close()
			return nil, false, err
		}
		n++
		if enabled {
			terms = append(terms, t)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if n == 0 {
		return nil, false, nil
	}
	return NewMatcher(terms), true, nil
}
