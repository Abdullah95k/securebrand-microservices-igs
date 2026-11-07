package phase0

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

func kit(t *testing.T, faults fakeig.Faults) (*Kit, *igtest.Env) {
	t.Helper()
	sp := fakeig.DefaultSpec()
	sp.Profiles, sp.PostsPerProfile = 4, 30
	env := igtest.New(t, igtest.Options{Spec: sp, Faults: faults, RotateDocIDs: true})
	var names []string
	for _, p := range env.World.Profiles() {
		names = append(names, p.Username)
	}
	k := &Kit{
		Stack: Stack{BaseURL: env.HTTP.URL, Pool: env.Pool, Loader: env.Loader, HTTP: http.DefaultClient,
			NewGQL: func(s registry.Seed) *graphql.Client {
				return &graphql.Client{Registry: registry.NewStatic(s), Rules: igdetect.Rules(), Builder: igclient.Builder{AcceptLanguage: "en-US"}}
			}},
		Cfg: KitConfig{Out: t.TempDir(), Tiers: []string{"direct"}, Profiles: names, BaseSeed: env.Seed,
			Options: Options{RPS: 1000, Parallel: 2, MaxAge: 365 * 24 * time.Hour, MaxGridPages: 20, CommentPosts: 1, CommentPages: 2,
				PostDetails: 1, ReelsPages: 2},
			CostPerGB: map[string]float64{"direct": 0}, Secrets: []string{"s3cr3t-proxy-pass"}, Salt: "test-salt"},
	}
	return k, env
}

// Against fakeig serving rotated doc_ids, every step works: the docids step
// finds the doc_ids in the page's bundle and checks every query, capture
// keeps scrubbed fixtures the parsers read, and the report says GO. The
// bundle holds only the kit's anonymous files: no username.
func TestKitGoAgainstFakeig(t *testing.T) {
	k, env := kit(t, fakeig.Faults{})
	res, err := k.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !res.Go || res.Recommended != "direct" {
		t.Fatalf("not GO: %+v\n%s", res, Format(res, k.Cfg.Thresholds))
	}
	// The report says when each tier ran, in Iraq time (REQ-008 fix 23).
	if txt := Format(res, k.Cfg.Thresholds); !strings.Contains(txt, "  ran ") || !strings.Contains(txt, " Iraq time\n") {
		t.Errorf("no Iraq-time run line:\n%s", txt)
	}
	var d DocIDs
	if err := readJSON(k.path(FileDocIDs), &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Changed) < 5 || len(d.Failing) > 0 {
		t.Fatalf("docids: %+v", d)
	}
	var checks []FixtureCheck
	if err := readJSON(k.path(FileFixtures), &checks); err != nil || len(checks) == 0 {
		t.Fatalf("fixtures: %v %v", checks, err)
	}
	ts := res.Tiers[0]
	if ts.BootstrapSuccess != 1 || ts.BlockRate != 0 || ts.Complete != 1 || ts.P95QueryMs <= 0 && ts.Queries == 0 {
		t.Fatalf("summary %+v", ts)
	}
	if _, ok := ts.Requests["page"]; !ok {
		t.Fatalf("page loads not counted: %v", ts.Requests)
	}
	// The bundle: only the kit's files, and no profile's name.
	b, err := os.ReadFile(k.path(FileBundle))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	files := 0
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		files++
		for _, p := range env.World.Profiles() {
			if bytes.Contains(body, []byte(p.Username)) {
				t.Fatalf("%s names %s", h.Name, p.Username)
			}
		}
		if strings.Contains(h.Name, dirRaw) {
			t.Fatalf("raw capture bundled: %s", h.Name)
		}
	}
	if files < 6 {
		t.Fatalf("bundle has %d files", files)
	}
	if _, err := os.Stat(filepath.Join(k.Cfg.Out, dirRaw)); !os.IsNotExist(err) {
		t.Fatal("the raw capture was not removed")
	}
}

// A tier that Instagram rate-limits often is NO-GO, with the advice.
func TestKitNoGoOnBlocks(t *testing.T) {
	k, _ := kit(t, fakeig.Faults{RateLimitEvery: 3})
	k.Cfg.From = StepSmoke
	res, err := k.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Go || len(res.Tiers) != 1 || !contains(res.Tiers[0].Failed, CheckBlockRate) {
		t.Fatalf("want NO-GO on the block rate: %+v", res)
	}
	if !strings.Contains(Format(res, k.Cfg.Thresholds), Advice[CheckBlockRate]) {
		t.Fatal("no advice in the report")
	}
}

// A credential in a bundled file stops the bundle.
func TestBundleRefusesSecrets(t *testing.T) {
	k, _ := kit(t, fakeig.Faults{})
	if err := os.WriteFile(k.path(FileMeta), []byte(`{"proxy":"s3cr3t-proxy-pass"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := k.bundle(); !errors.Is(err, ErrSecretInBundle) {
		t.Fatalf("bundle: %v", err)
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}
