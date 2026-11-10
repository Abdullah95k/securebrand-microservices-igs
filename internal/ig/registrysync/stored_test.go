package registrysync

import (
	"context"
	"slices"
	"testing"
)

// The checks read canaries in order, so the first is the account with the
// most of what the queries need, the next the one that adds what is still
// missing; spare pages go to the richest accounts left. Accounts with
// neither posts nor highlights are never chosen.
func TestPickCanaries(t *testing.T) {
	list := []storedAccount{
		{username: "none"},
		{username: "posts", has: hasPosts},
		{username: "posts_comments", has: hasPosts | hasComments},
		{username: "highlights_only", has: hasHighlights},
		{username: "reels", has: hasPosts | hasComments | hasReels},
		{username: "comments_only", has: hasComments},
	}
	for _, c := range []struct {
		n    int
		want []string
	}{
		{0, nil},
		{1, []string{"reels"}},
		{2, []string{"reels", "highlights_only"}},
		{3, []string{"reels", "highlights_only", "posts_comments"}},
		{9, []string{"reels", "highlights_only", "posts_comments", "posts"}},
	} {
		if got := pickCanaries(list, c.n); !slices.Equal(got, c.want) {
			t.Errorf("n=%d: %v, want %v", c.n, got, c.want)
		}
	}
	// Equal accounts keep the sample's random order.
	if got := pickCanaries([]storedAccount{{username: "b", has: hasPosts}, {username: "a", has: hasPosts}}, 2); !slices.Equal(got, []string{"b", "a"}) {
		t.Errorf("ties: %v", got)
	}
}

// With store_canaries and no canary_pages, a run takes its canaries from
// the store: active public accounts with posts or highlights only, the one
// with highlights first, and heals the registry on them. canary_pages fill
// what the store leaves, without repeats.
func TestStoreCanaries(t *testing.T) {
	s, env, db := setupDB(t, true)
	ctx := context.Background()
	if _, err := db.Exec(ctx, `INSERT INTO profiles (user_pk, username, author_key, media_count, highlight_count, status) VALUES
		(1, 'profile_01', '\x01', 12, 2, 'active'), (2, 'profile_02', '\x02', 12, 0, 'active'),
		(3, 'private_one', '\x03', 12, 2, 'private'), (4, 'empty_one', '\x04', 0, 0, 'active'),
		(5, 'erased_one', '\x05', 12, 2, 'suppressed')`); err != nil {
		t.Fatal(err)
	}
	s.Cfg.CanaryPages, s.Cfg.StoreCanaries = nil, 3
	rep, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("sync: %v (%+v)", err, rep)
	}
	var pages []string
	for _, p := range rep.Pages {
		pages = append(pages, p.Page)
	}
	if !slices.Equal(pages, []string{"u:profile_01", "u:profile_02"}) {
		t.Fatalf("pages %+v", rep.Pages)
	}
	if rep.Status != StatusOK || len(rep.Promoted) != len(env.Seed.Queries) {
		t.Fatalf("sync: status %s, promoted %v, failing %v, unchecked %v", rep.Status, rep.Promoted, rep.Failing, rep.Unchecked)
	}

	s.Cfg.CanaryPages, s.Cfg.StoreCanaries = []string{"@PROFILE_01", "profile_02", "profile_03"}, 1
	if got := s.canaryPages(ctx); !slices.Equal(got, []string{"profile_01", "profile_02", "profile_03"}) {
		t.Fatalf("store first, then canary_pages without repeats: %v", got)
	}
	s.Cfg.CanaryPages, s.Cfg.StoreCanaries = []string{"profile_02"}, 0
	if got := s.canaryPages(ctx); !slices.Equal(got, []string{"profile_02"}) {
		t.Fatalf("store_canaries 0: %v", got)
	}
}
