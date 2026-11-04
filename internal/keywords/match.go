package keywords

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Term is a keyword to match: its folded spelling and its group.
type Term struct {
	Normalized string
	Group      string
}

// Matcher finds which groups (iraq, kurdistan_region, a governorate) a
// text names. Text and terms are folded the same way (Normalize) and
// compared word by word, as igs scores names, bios and captions, where a
// place name inside a longer word would score wrongly (عراقة "heritage"
// holds عراق, رحلة "trip" holds حلة, "Kalara" holds Kalar). fbs's searches
// match anywhere in a post (discovery.Mentions); igs does not.
//
//   - Latin script: the term's words, whole.
//   - Arabic script: the term's words, the first with an attached prefix
//     allowed (و ب ل ف ک and the article: وبغداد, بالبصرة, والعراق) and
//     the last with a nisba ending (ي ية يين يون: بغدادي, العراقية,
//     عراقيين).
type Matcher struct {
	terms []matchTerm
}

type matchTerm struct {
	Term
	words  []string
	arabic bool
}

// NewMatcher returns a matcher of terms (normalized; empty ones skipped).
func NewMatcher(terms []Term) *Matcher {
	m := &Matcher{}
	seen := map[Term]bool{}
	for _, t := range terms {
		w := words(t.Normalized)
		if len(w) == 0 || seen[t] {
			continue
		}
		seen[t] = true
		m.terms = append(m.terms, matchTerm{Term: t, words: w,
			arabic: strings.IndexFunc(t.Normalized, func(r rune) bool { return unicode.Is(unicode.Arabic, r) }) >= 0})
	}
	return m
}

// SeedMatcher is a matcher of the seed's enabled keywords.
func SeedMatcher(seed []SeedKeyword) *Matcher {
	var terms []Term
	for _, k := range seed {
		if k.Enabled {
			terms = append(terms, Term{Normalized: Normalize(k.Term), Group: k.Group})
		}
	}
	return NewMatcher(terms)
}

// Len is the number of terms.
func (m *Matcher) Len() int { return len(m.terms) }

// Groups returns the groups text names, each once.
func (m *Matcher) Groups(text string) map[string]bool {
	out := map[string]bool{}
	if m == nil || text == "" {
		return out
	}
	w := words(Normalize(text))
	for _, t := range m.terms {
		if !out[t.Group] && t.in(w) {
			out[t.Group] = true
		}
	}
	return out
}

func (t matchTerm) in(text []string) bool {
	k := len(t.words)
	for i := 0; i+k <= len(text); i++ {
		ok := true
		for j, tw := range t.words {
			w := text[i+j]
			switch {
			case w == tw:
			case !t.arabic:
				ok = false
			case j == 0 && j == k-1:
				ok = prefixed(w, tw, true)
			case j == 0:
				ok = prefixed(w, tw, false)
			case j == k-1:
				ok = suffixed(w, tw)
			default:
				ok = false
			}
			if !ok {
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// Arabic prefixes a word may carry (folded: ک for ك), longest first, and
// nisba endings (ی for ي, ه for ة).
var (
	arabicPrefixes = []string{"وبال", "وال", "بال", "فال", "کال", "لل", "ال", "و", "ب", "ل", "ف", "ک"}
	nisba          = []string{"ی", "یه", "یین", "یون"}
)

// prefixed reports whether w is term with a prefix attached (and, when
// last, a nisba ending), or term with a nisba ending.
func prefixed(w, term string, last bool) bool {
	if last && suffixed(w, term) {
		return true
	}
	for _, p := range arabicPrefixes {
		rest, ok := strings.CutPrefix(w, p)
		if !ok || utf8.RuneCountInString(rest) < 2 {
			continue
		}
		if rest == term || last && suffixed(rest, term) {
			return true
		}
	}
	return false
}

func suffixed(w, term string) bool {
	rest, ok := strings.CutPrefix(w, term)
	return ok && slices.Contains(nisba, rest)
}

// words splits folded text into words of letters, digits and marks.
func words(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.Is(unicode.Mn, r)
	})
}

// Watch loads the table's keywords every interval and passes each new
// matcher to use, until ctx ends. A failed load keeps the last matcher.
func Watch(ctx context.Context, db DB, every time.Duration, use func(*Matcher)) error {
	if every <= 0 {
		every = 5 * time.Minute
	}
	load := func() {
		m, ok, err := Load(ctx, db)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				slog.Warn("keywords: load failed; keeping the last list", "err", err)
			}
		case ok:
			use(m)
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
