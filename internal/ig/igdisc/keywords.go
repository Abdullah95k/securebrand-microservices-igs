package igdisc

import (
	"context"
	"log/slog"
	"slices"
	"time"

	contracts "github.com/abdullah95k/securebrand-contracts"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
)

// Keyword is one keyword of the shared list, as the table stores it
// (ig.discovery_keywords).
type Keyword struct {
	// ID is the keyword's ID in every service:
	// contracts.KeywordID(Group, Language, Term), computed from the stored
	// spelling, never from the text that matched.
	ID         string
	Term       string
	Normalized string
	Language   string
	Group      string
}

// Index matches the shared list's enabled keywords in text and says which
// keywords matched. It matches the way igs's discovery does (whole words;
// Arabic prefixes and nisba endings: keywords.Matcher), giving each folded
// spelling a matcher group of its own so the matcher's answer names it. A
// spelling listed in two languages ("Iraq": en and kmr) matches as both.
type Index struct {
	m  *keywords.Matcher
	by map[string][]Keyword // matcher group -> the keywords it stands for
}

// NewIndex returns an index of list. Keywords without a folded spelling
// are folded here; empty ones are skipped.
func NewIndex(list []Keyword) *Index {
	x := &Index{by: map[string][]Keyword{}}
	var terms []keywords.Term
	for _, k := range list {
		if k.Normalized == "" {
			k.Normalized = keywords.Normalize(k.Term)
		}
		if k.Normalized == "" {
			continue
		}
		if k.ID == "" {
			k.ID = contracts.KeywordID(k.Group, k.Language, k.Term)
		}
		key := k.Group + "\x00" + k.Normalized
		if _, ok := x.by[key]; !ok {
			terms = append(terms, keywords.Term{Normalized: k.Normalized, Group: key})
		}
		if !slices.ContainsFunc(x.by[key], func(o Keyword) bool { return o.ID == k.ID }) {
			x.by[key] = append(x.by[key], k)
		}
	}
	x.m = keywords.NewMatcher(terms)
	return x
}

// Len is the number of keywords indexed.
func (x *Index) Len() int {
	if x == nil {
		return 0
	}
	n := 0
	for _, ks := range x.by {
		n += len(ks)
	}
	return n
}

// Match returns the keywords text names, ordered by ID.
func (x *Index) Match(text string) []Keyword {
	if x == nil || text == "" {
		return nil
	}
	var out []Keyword
	for key := range x.m.Groups(text) {
		out = append(out, x.by[key]...)
	}
	slices.SortFunc(out, func(a, b Keyword) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return out
}

// SeedIndex indexes the embedded seed's enabled keywords (fbs's Iraq list
// and igs's additions): what igdisc matches until the table is read.
func SeedIndex() (*Index, error) {
	seed, err := keywords.Seed()
	if err != nil {
		return nil, err
	}
	var list []Keyword
	for _, k := range seed {
		if k.Enabled {
			list = append(list, Keyword{Term: k.Term, Language: k.Language, Group: k.Group})
		}
	}
	return NewIndex(list), nil
}

// LoadIndex indexes the table's enabled keywords. ok is false when the
// table is empty (the seed was never imported): a caller then keeps the
// seed's index rather than matching nothing.
func LoadIndex(ctx context.Context, db keywords.DB) (x *Index, ok bool, err error) {
	rows, err := db.Query(ctx, `SELECT term, normalized, language, grp, enabled FROM discovery_keywords ORDER BY id`)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var list []Keyword
	n := 0
	for rows.Next() {
		var k Keyword
		var enabled bool
		if err := rows.Scan(&k.Term, &k.Normalized, &k.Language, &k.Group, &enabled); err != nil {
			return nil, false, err
		}
		n++
		if enabled {
			list = append(list, k)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if n == 0 {
		return nil, false, nil
	}
	return NewIndex(list), true, nil
}

// WatchIndex reads the table's keywords every interval (5 minutes when
// zero, as discovery does) and passes each new index to use, until ctx
// ends. A failed read keeps the last index.
func WatchIndex(ctx context.Context, db keywords.DB, every time.Duration, use func(*Index)) error {
	if every <= 0 {
		every = 5 * time.Minute
	}
	load := func() {
		x, ok, err := LoadIndex(ctx, db)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				slog.Warn("igdisc: keyword list read failed; keeping the last", "err", err)
			}
		case ok:
			use(x)
		}
	}
	load()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			load()
		}
	}
}
