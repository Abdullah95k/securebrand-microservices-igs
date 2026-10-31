package probe_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

func run(t *testing.T, f fakeig.Faults, extra ...string) (probe.Report, *igtest.Env) {
	t.Helper()
	env := igtest.New(t, igtest.Options{Faults: f})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	names = append(names, extra...)
	rep := probe.Run(context.Background(), probe.Options{BaseURL: env.HTTP.URL, Profiles: names, Tier: "direct",
		MaxGridPages: 10, Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, Fingerprint: "chrome_146", Version: "test"})
	return rep, env
}

// No faults: every question gets its "works" answer, and the report names
// no profile, id or text.
func TestProbeAgainstFakeig(t *testing.T) {
	rep, env := run(t, fakeig.Faults{}, "no_such_profile")
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
	for _, p := range rep.Profiles[:n] {
		if p.Comments == nil || p.Comments.Class != "ok" || p.Highlights == nil || p.Highlights.Items == 0 || p.Grid.Posts != posts {
			t.Fatalf("profile %d: %+v", p.Index, p)
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
	b, _ := json.Marshal(rep)
	for _, p := range env.World.Profiles() {
		for _, leak := range []string{p.Username, strconv.FormatInt(p.PK, 10), p.FullName, p.Posts[0].Code, p.Posts[0].Caption} {
			if strings.Contains(string(b), leak) {
				t.Fatalf("the report holds %q", leak)
			}
		}
	}
	if strings.Contains(string(b), "no_such_profile") {
		t.Fatal("the report names the unknown profile")
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
