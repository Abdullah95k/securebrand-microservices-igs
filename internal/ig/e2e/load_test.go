package e2e

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Load tests run igs's in-process pipeline at volume against fakeig. They
// are opt-in (IGS_LOADTEST=1, `make igs-loadtest`) and print a report;
// IGS_LOADTEST_OUT also appends it to a file. They measure this machine,
// one process for every component, so they find the pipeline's own
// ceilings (Kafka, Postgres, the persister, the stages), not Instagram's:
// production's pace is set by ig.identity_rps and the ISP exits.

func loadEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("IGS_LOADTEST") == "" {
		t.Skip("load test: set IGS_LOADTEST=1 (make igs-loadtest)")
	}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

type report struct {
	title string
	rows  [][2]string
}

func (r *report) add(k, format string, args ...any) {
	r.rows = append(r.rows, [2]string{k, fmt.Sprintf(format, args...)})
}

func (r *report) write(t *testing.T) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\n| Measure | Value |\n|---|---|\n", r.title)
	for _, row := range r.rows {
		fmt.Fprintf(&b, "| %s | %s |\n", row[0], row[1])
	}
	t.Log("\n" + b.String())
	if p := os.Getenv("IGS_LOADTEST_OUT"); p != "" {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		_, _ = f.WriteString(b.String() + "\n")
	}
}

func machine() string {
	return fmt.Sprintf("%d CPUs, %s/%s, %s", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// sample sums a metric's children matching labels: a counter's value, or a
// histogram's sample count and sum.
func sample(name string, labels map[string]string) (value float64, count uint64, sum float64) {
	mfs, _ := metrics.Registry.Gather()
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matches(m, labels) {
				value += m.GetCounter().GetValue()
				count += m.GetHistogram().GetSampleCount()
				sum += m.GetHistogram().GetSampleSum()
			}
		}
	}
	return value, count, sum
}

func matches(m *dto.Metric, labels map[string]string) bool {
	for k, v := range labels {
		found := false
		for _, l := range m.GetLabel() {
			if l.GetName() == k && l.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	return xs[min(len(xs)-1, int(p*float64(len(xs))))]
}

func count(t *testing.T, s *Stack, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := s.DB.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// requests is fakeig's GraphQL request count so far.
func requests(f *fakeig.Server) int64 {
	var n int64
	f.Requests.Range(func(_, v any) bool {
		n += v.(*atomic.Int64).Load()
		return true
	})
	return n
}

// TestLoadPipeline reads many profiles in full at once through the
// crawler stages, persister and job accounting, and checks the store
// against fakeig's ground truth: every non-gated post and every top-level
// comment.
//
//	IGS_LOADTEST_PROFILES (30), IGS_LOADTEST_POSTS (40 per profile),
//	IGS_LOADTEST_COMMENTS (60 max per post), IGS_LOADTEST_TIMEOUT (20m)
func TestLoadPipeline(t *testing.T) {
	loadEnabled(t)
	sp := spec()
	sp.Seed, sp.Profiles, sp.PostsPerProfile = 77, envInt("IGS_LOADTEST_PROFILES", 30), envInt("IGS_LOADTEST_POSTS", 40)
	sp.MaxComments = envInt("IGS_LOADTEST_COMMENTS", 60)
	s := Start(t, Options{Spec: sp, Config: func(c *config.Config) {
		c.Crawler.Slots, c.Crawler.LaneMin = 96, 4
		c.Crawler.Lanes = map[string]int{"p0": 96, "p1": 32, "p2": 32}
		c.IG.IdentityRPS, c.IG.IdentityBurst = 1000, 50
		c.IG.ProfileRPS, c.IG.ProfileRPSP0, c.IG.GlobalRPS = 100000, 100000, 100000
		c.IG.AIMD.Enabled = false
	}})
	lim := s.Limits()
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	wantPosts, wantComments := 0, 0
	for _, p := range s.World.Profiles() {
		truth, _ := s.World.Truth(p.Username, since)
		for _, pt := range truth.Posts {
			if !pt.Gated {
				wantPosts++
				wantComments += len(pt.Comments)
			}
		}
	}

	req0 := requests(s.Fake)
	_, bc0, bs0 := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	stopBacklog := watchBacklog(s, 2*time.Second)
	start := time.Now()
	ids := make([]string, 0, sp.Profiles)
	started := map[string]time.Time{}
	for _, p := range s.World.Profiles() {
		id := s.Crawl(t, p.Username, full, lim, nil)
		ids = append(ids, id)
		started[id] = time.Now()
	}
	// A job is done when its tasks are; durations are measured from each
	// job's start to the moment its last task finished.
	timeout := envDuration("IGS_LOADTEST_TIMEOUT", 20*time.Minute)
	var durations []float64
	pending := map[string]bool{}
	for _, id := range ids {
		pending[id] = true
	}
	deadline := time.Now().Add(timeout)
	for len(pending) > 0 && time.Now().Before(deadline) {
		for id := range pending {
			st, err := s.Jobs.Status(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if st.Spawned > 0 && st.Pending == 0 {
				durations = append(durations, time.Since(started[id]).Seconds())
				delete(pending, id)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(pending) > 0 {
		t.Fatalf("%d of %d jobs not done after %s", len(pending), len(ids), timeout)
	}
	s.Wait(t, 5*time.Minute, ids...) // the persister caught up
	elapsed := time.Since(start)
	backlog := stopBacklog()

	gotPosts := count(t, s, `SELECT count(*) FROM posts`)
	gotComments := count(t, s, `SELECT count(*) FROM comments WHERE depth = 0`)
	reqs := requests(s.Fake) - req0
	_, bc, bs := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("igs pipeline: %d profiles × %d posts (up to %d comments each), all read in full at once",
		sp.Profiles, sp.PostsPerProfile, sp.MaxComments)}
	r.add("Machine", "%s", machine())
	r.add("Wall time", "%s", elapsed.Round(time.Millisecond))
	r.add("GraphQL requests (fakeig; post pages are HTML loads on top)", "%d (%.0f/s)", reqs, float64(reqs)/elapsed.Seconds())
	r.add("Posts stored", "%d of %d (%.0f/s)", gotPosts, wantPosts, float64(gotPosts)/elapsed.Seconds())
	r.add("Top-level comments stored", "%d of %d (%.0f/s)", gotComments, wantComments, float64(gotComments)/elapsed.Seconds())
	r.add("Job duration p50 / p95 / max", "%.1fs / %.1fs / %.1fs", pct(durations, 0.5), pct(durations, 0.95), pct(durations, 1))
	if bc > 0 {
		r.add("Persister comment batches", "%d, mean %.1f ms", bc, 1000*bs/float64(bc))
	}
	for _, st := range []string{"profile", "post", "persister"} {
		b := backlog[st]
		r.add("Backlog: "+st, "peak %d, mean %.0f records", b.peak, b.mean)
	}
	r.write(t)
	if gotPosts != wantPosts || gotComments != wantComments {
		t.Fatalf("incomplete: posts %d/%d, comments %d/%d", gotPosts, wantPosts, gotComments, wantComments)
	}
}

type backlogStat struct {
	peak int64
	mean float64
}

// watchBacklog samples, every interval, the records waiting for each crawl
// stage (all lanes) and for the persister: the stage whose backlog grows
// is the one that saturates first. The returned func stops it.
func watchBacklog(s *Stack, every time.Duration) func() map[string]backlogStat {
	adm := kadm.NewClient(s.Producer.Client())
	label := map[string]string{s.Producer.Topic(persist.Group): "persister"}
	for _, st := range []string{"profile", "post"} {
		for _, l := range []string{"p0", "p1", "p2"} {
			label[s.Producer.Topic("igs-ig-crawler-"+st+"-"+l)] = st
		}
	}
	groups := make([]string, 0, len(label))
	for g := range label {
		groups = append(groups, g)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	peak, sum := map[string]int64{}, map[string]int64{}
	n := 0
	go func() {
		defer close(done)
		tk := time.NewTicker(every)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
			}
			lags, err := adm.Lag(ctx, groups...)
			if err != nil {
				continue
			}
			tot := map[string]int64{}
			for g, l := range lags {
				if l.Error() == nil {
					tot[label[g]] += l.Lag.Total()
				}
			}
			for st, v := range tot {
				peak[st] = max(peak[st], v)
				sum[st] += v
			}
			n++
		}
	}()
	return func() map[string]backlogStat {
		cancel()
		<-done
		out := map[string]backlogStat{}
		for st, p := range peak {
			out[st] = backlogStat{peak: p, mean: float64(sum[st]) / float64(max(n, 1))}
		}
		return out
	}
}

// TestLoadPersister measures igs's persister alone: synthetic comments are
// produced to ig.comment.v1 as fast as Kafka takes them, then the same
// records again (the no-op upsert path every re-read takes).
//
//	IGS_LOADTEST_ROWS (200000)
func TestLoadPersister(t *testing.T) {
	loadEnabled(t)
	n := envInt("IGS_LOADTEST_ROWS", 200_000)
	sp := fakeig.DefaultSpec()
	sp.Profiles, sp.PostsPerProfile = 1, 1
	s := Start(t, Options{Spec: sp})
	ctx := context.Background()
	taken := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	const owner = 42
	obs := &commonv1.Observation{ObservedAt: timestamppb.Now(), ContentHash: []byte{1}}
	produce := func() time.Duration {
		start := time.Now()
		const batch = 5000
		for i := 0; i < n; i += batch {
			b := s.Producer.NewBatch()
			for j := i; j < min(i+batch, n); j++ {
				media := int64(1_000_000 + j/200) // 200 comments a post
				c := &igv1.Comment{Obs: obs, CommentPk: int64(10_000_000 + j), MediaPk: media, OwnerPk: owner,
					PostTakenAt: timestamppb.New(taken), Text: "load test comment " + strconv.Itoa(j),
					CreatedAt: timestamppb.New(taken.Add(time.Duration(j) * time.Second)),
					Author:    &igv1.Author{UserPk: int64(60_000 + j%5000), Username: "loadtest_" + strconv.Itoa(j%5000), AuthorKey: []byte("k" + strconv.Itoa(j%5000))}}
				if err := b.AddProto(ctx, topics.Comment, strconv.FormatInt(media, 10), c); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start)
	}
	// processed counts the comment rows the persister handled (written or
	// unchanged).
	processed := func() float64 {
		v, _, _ := sample("igs_persist_rows_total", map[string]string{"entity": "comment"})
		return v
	}
	wait := func(target float64) {
		deadline := time.Now().Add(10 * time.Minute)
		for processed() < target {
			if time.Now().After(deadline) {
				t.Fatalf("the persister handled %.0f of %.0f rows in 10m", processed(), target)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	base := processed()
	_, bc0, bs0 := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	start := time.Now()
	prodNew := produce()
	wait(base + float64(n))
	fresh := time.Since(start)
	if got := count(t, s, `SELECT count(*) FROM comments WHERE owner_pk = $1`, owner); got != n {
		t.Fatalf("comments stored %d, want %d", got, n)
	}
	start = time.Now()
	prodAgain := produce()
	wait(base + 2*float64(n))
	again := time.Since(start)
	_, bc, bs := sample("igs_persist_batch_seconds", map[string]string{"entity": "comment"})
	bc, bs = bc-bc0, bs-bs0

	r := &report{title: fmt.Sprintf("igs persister: %d comments", n)}
	r.add("Machine", "%s", machine())
	r.add("New rows (COPY + insert)", "%s, %.0f rows/s (producing took %s)", fresh.Round(time.Millisecond), float64(n)/fresh.Seconds(), prodNew.Round(time.Millisecond))
	r.add("Same rows again (no-op upsert)", "%s, %.0f rows/s (producing took %s)", again.Round(time.Millisecond), float64(n)/again.Seconds(), prodAgain.Round(time.Millisecond))
	if bc > 0 {
		r.add("Comment batches", "%d, mean %.1f ms, %.0f rows each", bc, 1000*bs/float64(bc), 2*float64(n)/float64(bc))
	}
	r.write(t)
}
