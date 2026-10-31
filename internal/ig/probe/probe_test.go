package probe_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

func run(t *testing.T, f fakeig.Faults, extra ...string) (probe.Report, *igtest.Env) {
	return runImages(t, f, "", extra...)
}

func runImages(t *testing.T, f fakeig.Faults, imageDir string, extra ...string) (probe.Report, *igtest.Env) {
	t.Helper()
	env := igtest.New(t, igtest.Options{Faults: f})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	names = append(names, extra...)
	rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct",
		MaxGridPages: 10, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Fingerprint: "chrome_146", Version: "test", ImageDir: imageDir})
	return rep, env
}

// No faults: every question gets its "works" answer, and the report holds
// what fakeig served: profile details, every post, the newest post's first
// comments and the profile picture.
func TestProbeAgainstFakeig(t *testing.T) {
	dir := t.TempDir()
	rep, env := runImages(t, fakeig.Faults{}, filepath.Join(dir, "images"), "no_such_profile")
	s := rep.Summary
	n := len(env.World.Profiles())
	posts := len(env.World.Profiles()[0].Posts)
	if s.Profiles != n+1 || s.Bootstrapped != n || s.GridEnds != n || s.PagedPast24 != n || s.MaxPosts != posts {
		t.Fatalf("summary %+v", s)
	}
	if s.CursorPortable != n || s.CursorNotPortable != 0 || s.GridByPKWorks != n || s.GridByPKFails != 0 {
		t.Fatalf("U5/U9: %+v", s)
	}
	if s.CSRFCookie != n || s.Cookies["csrftoken"] != n || s.Cookies["mid"] != n || s.Cookies["ig_did"] != n {
		t.Fatalf("U8: %+v", s)
	}
	if s.QueriesWithoutClientState["ok"] == 0 || len(s.QueriesWithoutClientState) != 1 {
		t.Fatalf("U3: %v", s.QueriesWithoutClientState)
	}
	for _, k := range []string{"lsd", "csrf_token", "app_id", "jazoest"} {
		if s.TokensFound[k] != n {
			t.Errorf("token %s found %d times", k, s.TokensFound[k])
		}
	}
	for i, p := range rep.Profiles[:n] {
		want := env.World.Profiles()[i]
		if p.CommentsRead == nil || p.CommentsRead.Class != "ok" || p.Highlights == nil || p.Highlights.Items == 0 || p.Grid.Posts != posts {
			t.Fatalf("profile %d: %+v", p.Index, p)
		}
		d := p.Data
		if p.Username != want.Username || p.URL != env.HTTP.URL+"/"+want.Username+"/" || d == nil ||
			d.FullName != want.FullName || d.Followers != want.Followers || d.PK != strconv.FormatInt(want.PK, 10) ||
			d.Verified != want.Verified || len(d.BioLinks) != 1 || len(d.Highlights) != len(want.Highlights) || d.Highlights[0].Items == 0 {
			t.Fatalf("profile %d data: %+v", p.Index, d)
		}
		if len(p.Posts) != len(want.Posts) || p.Posts[0].Shortcode != want.Posts[0].Code || p.Posts[0].Caption != want.Posts[0].Caption ||
			!strings.HasSuffix(p.Posts[0].URL, "/"+want.Posts[0].Code+"/") || p.Posts[0].MediaType == "" || p.Posts[0].ThumbnailURL == "" {
			t.Fatalf("profile %d posts: %d, first %+v", p.Index, len(p.Posts), p.Posts[0])
		}
		newest := want.Posts[0]
		if len(p.Comments) != min(len(newest.Comments), fakeig.CommentPageSize) {
			t.Fatalf("profile %d comments: %d of %d", p.Index, len(p.Comments), len(newest.Comments))
		}
		if len(p.Comments) > 0 && (p.Comments[0].Text != newest.Comments[0].Text || p.Comments[0].Username != newest.Comments[0].Username ||
			p.Comments[0].PostShortcode != newest.Code || !p.Comments[0].CreatedAt.Equal(newest.Comments[0].CreatedAt)) {
			t.Fatalf("profile %d first comment: %+v", p.Index, p.Comments[0])
		}
		if b, err := os.ReadFile(filepath.Join(dir, d.ProfilePicFile)); err != nil || len(b) == 0 || d.ProfilePicFile != filepath.Join("images", want.Username+".jpg") {
			t.Fatalf("profile %d picture %q: %v", p.Index, d.ProfilePicFile, err)
		}
	}
	// The unknown profile's page is a 404 and its profile query finds no
	// user; it is reported by its line number only.
	if last := rep.Profiles[n]; last.Index != n+1 || last.Bootstrap.Class == "ok" || last.Profile == nil ||
		last.Profile.Reason != "no user in the response" || last.Grid != nil {
		t.Fatalf("unknown profile: %+v", last)
	}
	if s.Requests["page"].N != n+1 || s.Requests["grid"].N == 0 || s.Requests["grid"].MeanBytes == 0 {
		t.Fatalf("costs %+v", s.Requests)
	}
	// The files: the report, three spreadsheets and the bundle, all 0600.
	if err := probe.Write(dir, rep); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "report.json"))
	var back probe.Report
	if err != nil || json.Unmarshal(b, &back) != nil || len(back.Profiles) != n+1 {
		t.Fatalf("report.json: %v", err)
	}
	for name, rows := range map[string]int{"profiles.csv": n + 2, "posts.csv": n*posts + 1} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		recs, err := csv.NewReader(f).ReadAll()
		_ = f.Close()
		if err != nil || len(recs) != rows {
			t.Fatalf("%s: %d rows, want %d (%v)", name, len(recs), rows, err)
		}
		if fi, _ := os.Stat(filepath.Join(dir, name)); fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %v", name, fi.Mode().Perm())
		}
	}
	bundle, err := probe.Bundle(dir)
	if fi, serr := os.Stat(bundle); err != nil || serr != nil || fi.Size() == 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("bundle %s: %v %v", bundle, err, serr)
	}
}

// Each fault shows in its question's answer.
func TestProbeFindsTheWalls(t *testing.T) {
	t.Run("grid wall at 24 (U1)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{GridWallAfter: 24})
		n := len(env.World.Profiles())
		s := rep.Summary
		if s.GridWalls["login_wall"] != n || s.PagedPast24 != 0 || s.MaxPosts != 24 || len(s.WalledAtPosts) != n || s.WalledAtPosts[0] != 24 {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("cursor bound to its identity (U5)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{CursorBound: true})
		if s := rep.Summary; s.CursorNotPortable != len(env.World.Profiles()) || s.CursorPortable != 0 || s.GridEnds != len(env.World.Profiles()) {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("no grid by pk alone (U9)", func(t *testing.T) {
		rep, env := run(t, fakeig.Faults{NoPKGrid: true})
		if s := rep.Summary; s.GridByPKFails != len(env.World.Profiles()) || s.GridByPKWorks != 0 {
			t.Fatalf("summary %+v", s)
		}
	})
	t.Run("client state required (U3)", func(t *testing.T) {
		rep, _ := run(t, fakeig.Faults{RequireClientState: true})
		if s := rep.Summary; s.QueriesWithoutClientState["ok"] != 0 || s.QueriesWithoutClientState["soft_error"] == 0 {
			t.Fatalf("summary %+v", s.QueriesWithoutClientState)
		}
	})
	t.Run("grid depth cap", func(t *testing.T) {
		env := igtest.New(t, igtest.Options{Spec: fakeig.Spec{Seed: 2, Profiles: 1, PostsPerProfile: 100, MaxComments: 3, Highlights: 0}})
		rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: []string{env.World.Profiles()[0].Username},
			Tier: "direct", MaxGridPages: 3, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL})
		if g := rep.Profiles[0].Grid; g.Stop != "cap" || g.Posts != 48 || !rep.Profiles[0].NoHighlights {
			t.Fatalf("grid %+v", g)
		}
	})
}
