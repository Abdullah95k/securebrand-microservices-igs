// Package phase0 is igs's Phase 0 kit (brief M8, checkpoint C3): it
// measures what logged-out Instagram serves through each proxy tier before
// the pipeline is scaled up, and says GO or NO-GO per tier. Its steps are
// fbs's (preflight, docids, capture, smoke, run, report, bundle) on igs's
// probe, with fbs's kit gaps fixed: request latency is reported (p50 and
// p95 per kind), every page load counts (walled ones too), and grids cut by
// the kit's own page cap are reported apart from those Instagram ended.
//
// What the kit writes is anonymous: profiles are numbered by their line in
// the owner's file, never named, and the fixtures it captures are scrubbed
// (internal/ig/fixtures). The bundle holds only those files, and is refused
// when a configured proxy credential shows up in it.
package phase0

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Steps, in order. A run can start at any of them (KitConfig.From).
const (
	StepPreflight = "preflight"
	StepDocIDs    = "docids"
	StepCapture   = "capture"
	StepSmoke     = "smoke"
	StepRun       = "run"
	StepReport    = "report"
	StepBundle    = "bundle"
)

// Steps lists the steps in order.
var Steps = []string{StepPreflight, StepDocIDs, StepCapture, StepSmoke, StepRun, StepReport, StepBundle}

// Files written to the output directory.
const (
	FilePreflight = "preflight.json"
	FileDocIDs    = "docids.json"
	FileRegistry  = "registry.yaml"
	DirFixtures   = "fixtures"
	FileFixtures  = "fixtures.json"
	FileReportTxt = "report.txt"
	FileReport    = "report.json"
	FileMeta      = "meta.json"
	FileBundle    = "igs-phase0-bundle.tar.gz"
	dirRaw        = "raw-capture" // raw answers before scrubbing; never bundled, removed after capture
)

// SmokeFile and RunFile name the per-tier results.
func SmokeFile(tier string) string { return "smoke-" + tier + ".json" }

// RunFile names a tier's measurement run.
func RunFile(tier string) string { return "run-" + tier + ".json" }

// StepError stops the run. NeedsAction: the step worked but found what only
// a person or a code change can resolve (doc_ids no query answers, parser
// drift); the bundle is still written so the findings can be sent back.
type StepError struct {
	Step        string
	NeedsAction bool
	Err         error
}

func (e *StepError) Error() string { return e.Step + ": " + e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

// Stack holds the clients the kit reads with. igs-spike wires the real
// ones; tests wire fakeig.
type Stack struct {
	BaseURL string
	Proxies *proxy.Pool
	Pool    *session.Pool
	Loader  *session.Loader
	// NewGQL returns a GraphQL client over a registry.
	NewGQL func(registry.Seed) *graphql.Client
	// HTTP fetches JS bundles (direct egress).
	HTTP *http.Client
	// EchoClient returns a client through an exit, for the preflight's
	// exit IP check (nil skips it).
	EchoClient func(proxy.Endpoint) *http.Client
}

// Options size the reads.
type Options struct {
	RPS          float64
	Parallel     int
	MaxAge       time.Duration
	MaxGridPages int
	CommentPosts int
	CommentPages int
	PostDetails  int
	ReelsPages   int
	// GridPageSize, CommentPageSize and ReelsPageSize: 0 keeps the probe's
	// defaults.
	GridPageSize    int
	CommentPageSize int
	ReelsPageSize   int
	MaxWait         time.Duration
	PageWallRest    time.Duration
	Fingerprint     string
}

// KitConfig configures a run.
type KitConfig struct {
	Out      string
	Tiers    []string
	Profiles []string
	// BaseSeed is the registry the docids step starts from (the embedded
	// seed); Registry is a registry file to use instead of that step.
	BaseSeed registry.Seed
	Registry string
	From     string // first step (default preflight)
	Options  Options
	// CostPerGB is each tier's proxy price.
	CostPerGB  map[string]float64
	IPEchoURL  string
	Thresholds Thresholds
	// RequestsPerDay is the production volume the monthly cost projection
	// uses (default DefaultRequestsPerDay).
	RequestsPerDay float64
	// Secrets must never appear in the bundle (proxy user names and
	// passwords).
	Secrets []string
	// Salt pseudonymises the captured fixtures; empty draws one (it is
	// never written).
	Salt string
	// Version is the kit's build, for meta.json.
	Version string

	DocIDProfiles   int // default 1
	CaptureProfiles int // default 2
	SmokeProfiles   int // default 3
}

// Kit runs Phase 0.
type Kit struct {
	Stack Stack
	Cfg   KitConfig
	Log   io.Writer
}

func (k *Kit) logf(format string, args ...any) {
	if k.Log != nil {
		_, _ = fmt.Fprintf(k.Log, format+"\n", args...)
	}
}

func (k *Kit) path(name string) string { return filepath.Join(k.Cfg.Out, name) }

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path) //nolint:gosec // the kit's own output directory
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func first(xs []string, n int) []string { return xs[:min(n, len(xs))] }

// Run executes the steps from Cfg.From to the end. It returns the report
// (when the report step ran) and a *StepError when a step stopped the run;
// after a NeedsAction stop the bundle is still written.
func (k *Kit) Run(ctx context.Context) (Result, error) {
	c := &k.Cfg
	if c.DocIDProfiles <= 0 {
		c.DocIDProfiles = 1
	}
	if c.CaptureProfiles <= 0 {
		c.CaptureProfiles = 2
	}
	if c.SmokeProfiles <= 0 {
		c.SmokeProfiles = 3
	}
	if c.Thresholds == (Thresholds{}) {
		c.Thresholds = DefaultThresholds
	}
	if len(c.Profiles) == 0 || len(c.Tiers) == 0 {
		return Result{}, &StepError{Step: StepPreflight, Err: errors.New("no profiles or no tiers")}
	}
	if err := os.MkdirAll(c.Out, 0o750); err != nil {
		return Result{}, err
	}
	start := 0
	if c.From != "" {
		start = slices.Index(Steps, c.From)
		if start < 0 {
			return Result{}, fmt.Errorf("unknown step %q (want one of %v)", c.From, Steps)
		}
	}
	_ = writeJSON(k.path(FileMeta), map[string]any{"version": c.Version, "started_at": time.Now().UTC(), "tiers": c.Tiers,
		"profiles": len(c.Profiles), "options": c.Options, "thresholds": c.Thresholds, "from": Steps[start]})
	var res Result
	for _, step := range Steps[start:] {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		k.logf("== %s", step)
		var err error
		switch step {
		case StepPreflight:
			err = k.preflight(ctx)
		case StepDocIDs:
			if c.Registry != "" {
				k.logf("using the registry %s", c.Registry)
				continue
			}
			err = k.docids(ctx)
		case StepCapture:
			err = k.capture(ctx)
		case StepSmoke:
			err = k.smoke(ctx)
		case StepRun:
			err = k.run(ctx)
		case StepReport:
			res, err = k.report()
		case StepBundle:
			err = k.bundle()
		}
		if err != nil {
			var se *StepError
			if !errors.As(err, &se) {
				se = &StepError{Step: step, Err: err}
			}
			if se.NeedsAction && step != StepBundle {
				if berr := k.bundle(); berr != nil {
					k.logf("bundle: %v", berr)
				}
			}
			return res, se
		}
	}
	return res, nil
}

// registry is the registry the measuring steps use: the file given, the
// docids step's, or the base seed.
func (k *Kit) registry() (registry.Seed, error) {
	path := k.Cfg.Registry
	if path == "" {
		path = k.path(FileRegistry)
	}
	b, err := os.ReadFile(path) //nolint:gosec // the kit's own output or the owner's file
	if errors.Is(err, os.ErrNotExist) && k.Cfg.Registry == "" {
		return k.Cfg.BaseSeed, nil
	}
	if err != nil {
		return registry.Seed{}, err
	}
	return registry.ParseSeed(b)
}
