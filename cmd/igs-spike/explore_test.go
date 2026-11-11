package main

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/explore"
)

func runExploreAgainst(t *testing.T, mode string, o explore.Options) explore.Report {
	t.Helper()
	w := fakeig.NewWorld(fakeig.DefaultSpec())
	hs := httptest.NewServer(fakeig.NewServer(w, fakeig.Options{Faults: fakeig.Faults{Explore: mode}}))
	defer hs.Close()
	st, err := newStack(&config.Config{}, hs.URL, "chrome_146", 1000, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	o.BaseURL, o.Tier, o.Pool, o.Loader = hs.URL, "direct", st.pool, st.loader
	return explore.Run(context.Background(), o)
}

// Open pages: each loads, and its distinct posts and accounts are
// counted (fakeig embeds the same 9 posts, one from each profile in turn,
// on every page).
func TestExploreOpenPages(t *testing.T) {
	rep := runExploreAgainst(t, fakeig.ExploreOpen, explore.Options{Hashtags: []string{"#Iraq", "بغداد"}, Locations: []string{"1000001"}})
	if rep.Requests != 3 || rep.Stopped != "" {
		t.Fatalf("%d requests, stopped %q", rep.Requests, rep.Stopped)
	}
	h, l := rep.Hashtag, rep.Location
	if h.Pages != 2 || h.Loaded != 2 || h.WithPosts != 2 || h.Posts != 9 || l.Pages != 1 || l.Loaded != 1 || l.Posts != 9 {
		t.Fatalf("hashtag %+v, location %+v", h, l)
	}
	n := len(fakeig.NewWorld(fakeig.DefaultSpec()).Profiles())
	if want := min(n, 9); h.Accounts != want || l.Accounts != want {
		t.Fatalf("accounts: hashtag %d, location %d, want %d each", h.Accounts, l.Accounts, want)
	}
	if rep.Pages[0].Kind != explore.KindHashtag || rep.Pages[1].Kind != explore.KindLocation {
		t.Fatalf("pages do not alternate: %+v", rep.Pages)
	}
}

// Walled pages send the visitor to log in: counted as such, nothing read.
func TestExploreWalledPages(t *testing.T) {
	rep := runExploreAgainst(t, fakeig.ExploreWalled, explore.Options{Hashtags: []string{"iraq"}, Locations: []string{"1000001"}})
	if h, l := rep.Hashtag, rep.Location; h.LoginRedirect+h.LoginWall != 1 || l.LoginRedirect+l.LoginWall != 1 || h.Loaded+l.Loaded != 0 ||
		h.Posts+l.Posts != 0 {
		t.Fatalf("hashtag %+v, location %+v", h, l)
	}
}

// A run never makes more than MaxPagesLimit page loads.
func TestExploreCap(t *testing.T) {
	tags := make([]string, 30)
	for i := range tags {
		tags[i] = "tag" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	rep := runExploreAgainst(t, fakeig.ExploreOpen, explore.Options{Hashtags: tags, MaxPages: 100})
	if rep.Requests != explore.MaxPagesLimit {
		t.Fatalf("%d requests, want %d", rep.Requests, explore.MaxPagesLimit)
	}
}

// The default hashtags are Iraq's places from the keyword list: one-word
// terms, the country first.
func TestPlaceHashtags(t *testing.T) {
	tags, err := placeHashtags(6)
	if err != nil || len(tags) != 6 || tags[0] != "iraq" || tags[1] != "العراق" {
		t.Fatalf("%v %v", tags, err)
	}
	for _, tg := range tags {
		for _, r := range tg {
			if r == ' ' {
				t.Fatalf("a hashtag with a space: %q", tg)
			}
		}
	}
}

func TestExploreDryRun(t *testing.T) {
	if code := runExplore([]string{"--dry-run", "--out-dir", t.TempDir()}); code != 0 {
		t.Fatalf("exit %d", code)
	}
}
