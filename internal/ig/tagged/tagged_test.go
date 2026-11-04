package tagged_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/tagged"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
)

func run(t *testing.T, mode string) (tagged.Report, string) {
	t.Helper()
	env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{TaggedTab: mode}})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	dir := t.TempDir()
	rep := tagged.Run(context.Background(), tagged.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct",
		Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Seed: env.Seed, MaxWait: time.Second,
		Scanner: &docids.Scanner{Fetch: docids.HTTPFetcher(&http.Client{Timeout: 5 * time.Second}), MaxBundles: 5},
		RawDir:  filepath.Join(dir, "raw")})
	if err := tagged.Write(dir, rep); err != nil {
		t.Fatal(err)
	}
	return rep, dir
}

// An open tab: the page carries the first posts, the bundle declares the
// operation, and a shape pages on; later profiles try that shape first.
func TestTaggedTabOpen(t *testing.T) {
	rep, dir := run(t, fakeig.TaggedOpen)
	s := rep.Summary
	if s.Profiles != 3 || s.TaggedPageOK != 3 || s.EmbeddedPosts != 3 {
		t.Fatalf("summary %+v", s)
	}
	if rep.Operations[fakeig.OpTagged] != fakeig.TaggedDocID || s.OperationsSeen != 1 {
		t.Fatalf("operations %v", rep.Operations)
	}
	if s.Worked[fakeig.OpTagged+" user_id_pk_count"] != 3 {
		t.Fatalf("worked %v", s.Worked)
	}
	for _, r := range rep.Results {
		if len(r.Tries) != 1 || r.Tries[0].Posts == 0 {
			t.Fatalf("profile %d tries %+v", r.Index, r.Tries)
		}
		if len(r.Markers) == 0 {
			t.Fatalf("profile %d: no markers", r.Index)
		}
	}
	if got := s.Verdict(); got != "the tagged tab is readable logged out: an operation answered with posts" {
		t.Fatalf("verdict %q", got)
	}
	for _, f := range []string{"report.json", "raw/001/tagged-page.html"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
}

// A walled tab: every visit to it goes to the login page, and the
// operation is neither declared nor answered.
func TestTaggedTabWalled(t *testing.T) {
	rep, _ := run(t, fakeig.TaggedWalled)
	s := rep.Summary
	if s.TaggedLogin != 3 || s.TaggedPageOK != 0 || s.EmbeddedPosts != 0 || len(s.Worked) != 0 {
		t.Fatalf("summary %+v", s)
	}
	if got := s.Verdict(); got != "the tagged tab needs a login: Instagram sent every visit to the login page" {
		t.Fatalf("verdict %q", got)
	}
}

// Off (as Instagram may answer, with no tab): the page is not found and
// nothing is tried.
func TestTaggedTabMissing(t *testing.T) {
	rep, _ := run(t, "")
	s := rep.Summary
	if s.TaggedOther != 3 || s.OperationsSeen != 0 || len(s.Worked) != 0 {
		t.Fatalf("summary %+v", s)
	}
	if got := s.Verdict(); got != "the tagged tab gave no posts logged out" {
		t.Fatalf("verdict %q", got)
	}
}
