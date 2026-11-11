package registrysync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

var igQueries = registry.In(registry.IGTables)

func setup(t *testing.T, promote bool, faults ...fakeig.Faults) (*Sync, *igtest.Env) {
	t.Helper()
	s, env, _ := setupDB(t, promote, faults...)
	return s, env
}

// setupDB is setup that also returns the test database's admin pool, for
// rows the sync's own role may only read.
func setupDB(t *testing.T, promote bool, faults ...fakeig.Faults) (*Sync, *igtest.Env, *pgxpool.Pool) {
	t.Helper()
	db := testenv.PostgresIG(t)
	sp := fakeig.DefaultSpec()
	sp.Profiles, sp.Highlights = 2, 2
	var f fakeig.Faults
	if len(faults) > 0 {
		f = faults[0]
	}
	env := igtest.New(t, igtest.Options{Spec: sp, RotateDocIDs: true, Faults: f})
	if err := igQueries.ApplySeed(context.Background(), db, env.Seed); err != nil {
		t.Fatal(err)
	}
	s := &Sync{DB: testenv.AsComponent(t, db, "registry-sync"), Pool: env.Pool, Loader: env.Loader, Rules: igdetect.Rules(), AcceptLanguage: "en-US",
		Fetch: docids.HTTPFetcher(http.DefaultClient),
		Cfg: config.RegistrySync{CanaryPages: []string{"profile_01", "@profile_02"}, Tier: "direct", MaxBundles: 10, Promote: promote,
			KeepFallbacks: 2, RetireAfter: 3, RecheckRetired: time.Hour, Timeout: time.Minute}}
	return s, env, db
}

// fakeig accepts only rotated doc_ids, which its page's bundle declares:
// one sync records them as candidates, finds every seed doc_id stale and
// the candidates working on a canary profile, and promotes them, keeping
// the seed's as fallbacks; a client over the registry then reads again.
// The next sync has nothing to do.
func TestStaleQueriesHealThemselves(t *testing.T) {
	s, env := setup(t, true)
	ctx := context.Background()
	reg, err := igQueries.NewDB(ctx, s.DB, env.Seed)
	if err != nil {
		t.Fatal(err)
	}
	gql := &graphql.Client{Registry: reg, Rules: igdetect.Rules(), Builder: igclient.Builder{AcceptLanguage: "en-US"}}
	id, err := env.Pool.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	defer env.Pool.Release(id)
	if _, err := env.Loader.Load(ctx, id, env.HTTP.URL+"/profile_01/"); err != nil {
		t.Fatal(err)
	}
	profile := func() detect.Class {
		resp, err := gql.Do(ctx, id, graphql.Request{Query: "profile", Vars: map[string]any{"username": "profile_01"}})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Class
	}
	if c := profile(); c == detect.OK {
		t.Fatal("the seed's doc_id should be stale against rotated fakeig")
	}

	rep, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("sync: %v (%+v)", err, rep)
	}
	if rep.Status != StatusOK || len(rep.Promoted) != len(env.Seed.Queries) {
		t.Fatalf("sync: status %s, promoted %v, failing %v, unchecked %v, checks %+v", rep.Status, rep.Promoted, rep.Failing, rep.Unchecked, rep.Checks)
	}
	qs, err := igQueries.List(ctx, s.DB)
	if err != nil {
		t.Fatal(err)
	}
	active, fallback := 0, 0
	for _, q := range qs {
		switch q.Status {
		case registry.StatusActive:
			active++
			if q.DocID[0] != '9' {
				t.Errorf("%s: active %s is not the rotated doc_id", q.Name, q.DocID)
			}
		case registry.StatusFallback:
			fallback++
		}
	}
	if active != len(env.Seed.Queries) || fallback != len(env.Seed.Queries) {
		t.Fatalf("%d active, %d fallbacks", active, fallback)
	}
	if err := reg.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if c := profile(); c != detect.OK {
		t.Fatalf("after the sync the profile query answers %s", c)
	}

	again, err := s.Run(ctx)
	if err != nil || again.Status != StatusOK || len(again.Promoted) != 0 || len(again.Candidates) != 0 {
		t.Fatalf("second sync: %v %+v", err, again)
	}
	var runs int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM registry_sync_runs`).Scan(&runs); err != nil || runs != 2 {
		t.Fatalf("runs recorded %d %v", runs, err)
	}
}

// With promotion off (a dry run) the sync only proposes.
func TestDryRunProposes(t *testing.T) {
	s, env := setup(t, false)
	rep, err := s.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Promoted) != 0 || len(rep.Proposed) != len(env.Seed.Queries) {
		t.Fatalf("dry run: promoted %v, proposed %v", rep.Promoted, rep.Proposed)
	}
}

// When the bundles declare no doc_id (U6 answered no), a rendered canary
// page's own requests supply them, POST and GET alike; the queries no
// request showed stay failing and the run is degraded.
func TestRenderSuppliesDocIDs(t *testing.T) {
	if _, err := browser.FindBinary(""); err != nil {
		t.Skip(err)
	}
	s, env := setup(t, true, fakeig.Faults{NoBundleDocIDs: true, JSOnlyTokens: true})
	b, err := browser.NewBootstrapper(config.Browser{MaxConcurrent: 1, MaxShare: 0.5, Timeout: time.Minute}, "en-US", igclient.Hooks())
	if err != nil {
		t.Fatal(err)
	}
	b.Rules, b.CaptureFilter = igdetect.Rules(), browser.IsInstagramGraphQL
	env.Loader.Browser, s.Render = b, b.Capture
	rep, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if rep.Rendered[fakeig.OpProfile] == "" || rep.Rendered[fakeig.OpHighlights] == "" {
		t.Fatalf("rendered %v", rep.Rendered)
	}
	promoted := map[string]bool{}
	for _, p := range rep.Promoted {
		promoted[p.Query] = true
	}
	if !promoted["profile"] || !promoted["highlights"] || rep.Status != StatusDegraded {
		t.Fatalf("promoted %v, status %s, failing %v", rep.Promoted, rep.Status, rep.Failing)
	}
}

// A kind the crawlers don't read (ig.limits.include_highlights off) is
// neither probed nor unchecked: no highlights request goes out, the
// report lists the query as skipped, and the run counts as checked.
func TestSkippedKindsAreNotProbed(t *testing.T) {
	s, env := setup(t, true)
	s.Skip = map[string]bool{"highlights": true}
	rep, err := s.Run(context.Background())
	if err != nil {
		t.Fatalf("sync: %v (%+v)", err, rep)
	}
	if n := env.Fake.QueryCount(fakeig.OpHighlights); n != 0 {
		t.Errorf("%d highlights requests with highlights skipped", n)
	}
	for _, c := range rep.Checks {
		if c.Query == "highlights" {
			t.Errorf("highlights checked: %+v", c)
		}
	}
	if len(rep.Skipped) != 1 || rep.Skipped[0] != "highlights" || len(rep.Unchecked) != 0 || len(rep.Failing) != 0 {
		t.Fatalf("skipped %v, unchecked %v, failing %v", rep.Skipped, rep.Unchecked, rep.Failing)
	}
	if !rep.Checked() || rep.Status != StatusOK {
		t.Fatalf("a run with a skipped kind counts as checked: status %s", rep.Status)
	}
}
