package keywords_test

import (
	"context"
	"errors"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// The seed loads once; loading it again adds nothing and leaves an
// operator's choices alone; the matcher follows the table.
func TestTableFollowsTheOperator(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	if _, ok, err := keywords.Load(ctx, db); err != nil || ok {
		t.Fatalf("empty table: ok=%v err=%v (a caller keeps the embedded seed)", ok, err)
	}
	seed, err := keywords.Seed()
	if err != nil {
		t.Fatal(err)
	}
	n, err := keywords.ImportSeed(ctx, db, seed)
	if err != nil || n != len(seed) {
		t.Fatalf("import: %d of %d, %v", n, len(seed), err)
	}
	m, ok, err := keywords.Load(ctx, db)
	if err != nil || !ok || !m.Groups("Mosul")["nineveh"] || m.Groups("Hit the gym")["anbar"] {
		t.Fatalf("loaded matcher: ok=%v err=%v", ok, err)
	}

	// Turn Mosul off and Hit on: the matcher follows; a second import
	// changes neither.
	off, on := false, true
	list := func(term string) keywords.Keyword {
		t.Helper()
		ks, err := keywords.List(ctx, db, keywords.Filter{Language: "en", Limit: 10000})
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range ks {
			if k.Term == term {
				return k
			}
		}
		t.Fatalf("%q not listed", term)
		return keywords.Keyword{}
	}
	if _, err := keywords.Set(ctx, db, list("Mosul").ID, keywords.Update{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	if _, err := keywords.Set(ctx, db, list("Hit").ID, keywords.Update{Enabled: &on}); err != nil {
		t.Fatal(err)
	}
	if n, err := keywords.ImportSeed(ctx, db, seed); err != nil || n != 0 {
		t.Fatalf("second import added %d, %v", n, err)
	}
	m, _, _ = keywords.Load(ctx, db)
	if m.Groups("Mosul")["nineveh"] || !m.Groups("Hit the gym")["anbar"] {
		t.Fatal("the matcher does not follow the table")
	}
	if !list("Hit").Enabled || list("Mosul").Enabled {
		t.Fatal("a second import undid the operator's changes")
	}

	// A new spelling; one that folds to a listed one is not added twice.
	k, created, err := keywords.Add(ctx, db, "Al Mawsil", "en", "nineveh", true)
	if err != nil || !created {
		t.Fatalf("add: %v %v", created, err)
	}
	if _, created, _ := keywords.Add(ctx, db, "al-mawsil", "en", "nineveh", true); created {
		t.Fatal("a folded duplicate was added")
	}
	if _, err := keywords.Set(ctx, db, k.ID, keywords.Update{Term: ptr("Mosul")}); !errors.Is(err, keywords.ErrDuplicate) {
		t.Fatalf("renaming onto a listed spelling: %v", err)
	}
	if _, _, err := keywords.Add(ctx, db, "x", "fr", "", true); !errors.Is(err, keywords.ErrInvalid) {
		t.Fatalf("unknown language: %v", err)
	}
	if err := keywords.Delete(ctx, db, k.ID); err != nil {
		t.Fatal(err)
	}
	if err := keywords.Delete(ctx, db, k.ID); !errors.Is(err, keywords.ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func ptr[T any](v T) *T { return &v }
