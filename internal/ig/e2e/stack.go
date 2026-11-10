// Package e2e runs igs's pipeline in-process (crawler stages, persister)
// against real Postgres (schema ig), Kafka and Valkey, and fakeig, and
// checks what lands in the store against fakeig's ground truth.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	igapi "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	igapp "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	iggdpr "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/scheduler"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// exitDoer names its exit on every request, for fakeig's per-exit faults.
type exitDoer struct {
	transport.Doer
	exit string
}

func (d exitDoer) Do(ctx context.Context, req *transport.Request) (*transport.Response, error) {
	r := *req
	r.Header = append(slices.Clone(req.Header), [2]string{fakeig.ExitHeader, d.exit})
	return d.Doer.Do(ctx, &r)
}

// Stack is a running igs pipeline.
type Stack struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool // schema ig
	Redis    *redis.Client
	Producer *kafka.Producer
	World    *fakeig.World
	Fake     *fakeig.Server
	Crawler  *igapp.Crawler
	Jobs     *jobs.Tracker
	Dispatch *dispatch.Dispatcher
	API      *httptest.Server
	S3       *minio.Client

	// Each component's own pool (testenv.AsComponent), as in production: the
	// api, the scheduler, the crawler, the persister and media. DB stays
	// the owner's, for the tests' own reads and setup.
	dbs map[string]*pgxpool.Pool

	mu     sync.Mutex
	groups map[string]*group
	copts  igapp.CrawlerOptions
	topics []string // prefixed entity topics, for the persister's lag
}

type group struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Options tweak the stack.
type Options struct {
	Spec   fakeig.Spec
	Faults fakeig.Faults
	Config func(*config.Config)
	// CrawlerDrain overrides how long a stopping crawler lets in-flight
	// tasks run before abandoning them (a short one is a crash).
	CrawlerDrain time.Duration
	// Exits, when set, sends every request through that many ISP exits
	// (igtest.ExitName: exit1.test:10001, ...) of a static_list tier, paced
	// and rested as in production but in seconds. Requests still go straight
	// to fakeig, naming their exit (fakeig.ExitHeader) for its per-exit
	// faults.
	Exits int
	// RawCapture keeps every raw response in the raw bucket
	// (ig.capture_raw: all), as production does.
	RawCapture bool
}

// Start brings up the stack; it stops at test cleanup.
func Start(t *testing.T, o Options) *Stack {
	t.Helper()
	if os.Getenv("IGS_E2E_DEBUG") != "" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	brokers := testenv.Kafka(t)
	db := testenv.PostgresIG(t)
	rdb := testenv.Redis(t)
	ctx := context.Background()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	cfg.Kafka.Brokers, cfg.Kafka.TopicPrefix, cfg.Kafka.ProduceLinger = brokers, prefix, time.Millisecond
	cfg.IG.IdentityRPS, cfg.IG.IdentityBurst = 500, 50
	cfg.IG.IdentityMaxRequestsMin, cfg.IG.IdentityMaxRequestsMax = 100000, 100001
	cfg.IG.ProfileRPS, cfg.IG.ProfileRPSP0, cfg.IG.GlobalRPS = 5000, 5000, 10000
	cfg.IG.RequestTimeout = 10 * time.Second
	cfg.IG.CaptureRaw = "none"
	cfg.IG.AIMD.MinSamples = 10
	cfg.Crawler.YieldPages = 2 // exercise continuations
	cfg.Crawler.Slots, cfg.Crawler.LaneMin = 24, 2
	cfg.Crawler.Lanes = map[string]int{"p0": 24, "p1": 16, "p2": 8}
	cfg.Crawler.SuppressionRefresh = time.Hour
	if o.Exits > 0 {
		var eps []string
		for i := 1; i <= o.Exits; i++ {
			eps = append(eps, "http://"+igtest.ExitName(i))
		}
		cfg.IG.Proxy.Tiers = []config.ProxyTier{{Name: "isp", Kind: "static_list", Endpoints: eps}}
		cfg.IG.Proxy.Routing = map[string][]string{"default": {"isp"}}
		cfg.IG.ExitRPS = 200
		cfg.IG.RateLimitRest, cfg.IG.CheckpointRest, cfg.IG.PageWallRest = time.Second, 2*time.Second, 2*time.Second
		cfg.IG.Proxy.Breaker.Cooldown, cfg.IG.Proxy.Breaker.MaxCooldown = time.Second, 4*time.Second
	}
	if o.Config != nil {
		o.Config(cfg)
	}

	spec := o.Spec
	if spec.Profiles == 0 {
		spec = fakeig.DefaultSpec()
	}
	seedBytes, err := configs.FS.ReadFile(cfg.IG.RegistrySeed)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
	}
	world := fakeig.NewWorld(spec)
	fake := fakeig.NewServer(world, fakeig.Options{Docs: docs, Faults: o.Faults, Admin: true})
	fhs := httptest.NewServer(fake)
	t.Cleanup(fhs.Close)
	cfg.IG.BaseURL = fhs.URL
	fake.SetCDN(fhs.URL + "/fake-cdn")

	// S3 (in memory)
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(s3srv.Close)
	u, _ := url.Parse(s3srv.URL)
	cfg.S3.Endpoint, cfg.S3.UseSSL = u.Host, false
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, false, cfg.S3.MediaBucket, cfg.S3.RawBucket)
	if err != nil {
		t.Fatal(err)
	}

	if err := registry.In(registry.IGTables).ApplySeed(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	// The place list, as igsctl migrate up loads it.
	kseed, err := keywords.Seed()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keywords.ImportSeed(ctx, db, kseed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.IGLayout.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 2}); err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "igs-e2e", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	tb, _ := configs.FS.ReadFile("ig/topics.yaml")
	var doc struct {
		Topics []kafka.TopicSpec `yaml:"topics"`
	}
	if err := yaml.Unmarshal(tb, &doc); err != nil {
		t.Fatal(err)
	}
	for i := range doc.Topics {
		doc.Topics[i].Partitions = min(doc.Topics[i].Partitions, 3)
	}
	if err := kafka.EnsureTopics(ctx, producer.Client(), prefix, doc.Topics, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		names := make([]string, len(doc.Topics))
		for i, tp := range doc.Topics {
			names[i] = prefix + tp.Name
		}
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = kadm.NewClient(producer.Client()).DeleteTopics(cctx, names...)
	})

	s := &Stack{Cfg: cfg, DB: db, Redis: rdb, Producer: producer, World: world, Fake: fake, Jobs: jobs.New(rdb), S3: s3,
		groups: map[string]*group{}, dbs: map[string]*pgxpool.Pool{},
		copts: igapp.CrawlerOptions{DrainTimeout: o.CrawlerDrain, RetryScale: 0.02,
			DoerFactory: func(to transport.Options) (transport.Doer, error) {
				d := transport.NewStd(10 * time.Second)
				if u, err := url.Parse(to.ProxyURL); err == nil && u.Host != "" {
					return exitDoer{Doer: d, exit: u.Host}, nil
				}
				return d, nil
			}}}
	if o.RawCapture {
		cfg.IG.CaptureRaw = "all"
		s.copts.RawS3 = s3
	}
	for _, tp := range topics.Entity {
		s.topics = append(s.topics, prefix+tp)
	}
	t.Cleanup(s.Stop)
	for _, c := range []string{"api", "scheduler", "crawler", "persister", "media"} {
		s.dbs[c] = testenv.AsComponent(t, db, c)
	}
	s.Crawler, err = igapp.NewCrawler(ctx, cfg, s.dbs["crawler"], rdb, producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	s.start("crawler", s.Crawler.Components...)
	s.RestartPersister(t)
	s.Dispatch = &dispatch.Dispatcher{DB: s.dbs["api"], Jobs: s.Jobs, Producer: producer}
	srv := &igapi.Server{Cfg: cfg, DB: s.dbs["api"], Redis: rdb, Dispatch: s.Dispatch}
	s.API = httptest.NewServer(srv.Handler())
	t.Cleanup(s.API.Close)
	return s
}

// StartScheduler runs the planner and the finalizer, as igs-scheduler's
// leader does, with short intervals.
func (s *Stack) StartScheduler() {
	db := s.dbs["scheduler"]
	disp := &dispatch.Dispatcher{DB: db, Jobs: s.Jobs, Producer: s.Producer}
	planner := &scheduler.Planner{DB: db, Dispatch: disp, Cfg: s.Cfg, Redis: s.Redis}
	fin := &scheduler.Finalizer{DB: db, Jobs: s.Jobs, Producer: s.Producer, Cfg: s.Cfg, Timeout: 30 * time.Second}
	loops := []run.Func{
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "planner", 200*time.Millisecond, planner.Tick)
		},
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "finalizer", 200*time.Millisecond, fin.Tick)
		},
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "tiers", 200*time.Millisecond, (&scheduler.Tierer{DB: db, Cfg: s.Cfg}).Tick)
		},
	}
	if s.Cfg.IG.Discovery.Enabled {
		vetter := &discovery.Vetter{DB: db, Dispatch: disp, Cfg: s.Cfg, Candidates: igapp.CandidatePublisher(s.Producer)}
		if s.Cfg.IG.ShareExitRests {
			vetter.Rests = ratelimit.NewExitRests(s.Redis, s.Cfg.Kafka.TopicPrefix)
		}
		loops = append(loops, func(ctx context.Context) error {
			return scheduler.Every(ctx, "discovery", s.Cfg.IG.Discovery.Interval, vetter.Tick)
		})
	}
	s.start("scheduler", loops...)
}

// StartNormalizer runs igs-normalizer (ig.* -> the contract's content.*),
// with the volume rule in memory.
func (s *Stack) StartNormalizer(t *testing.T) {
	t.Helper()
	c, err := igapp.NewNormalizer(s.Cfg, s.Producer, &normalize.MemVolume{})
	if err != nil {
		t.Fatal(err)
	}
	s.start("normalizer", c.Run)
}

// ReadTopic returns every record of a topic (base name) from its start,
// once none has arrived for idle.
func (s *Stack) ReadTopic(t *testing.T, topic string, idle time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(s.Cfg.Kafka.TopicPrefix+topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), idle)
		fs := cl.PollFetches(ctx)
		cancel()
		n := 0
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r); n++ })
		if n == 0 {
			return out
		}
	}
}

// StartMedia runs igs-media's downloader and janitor.
func (s *Stack) StartMedia(t *testing.T) {
	t.Helper()
	comps, err := igapp.NewMedia(s.Cfg, s.dbs["media"], s.S3, s.Producer, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.start("media", comps...)
}

// StartEraser runs the erasure loop, as igs-scheduler's leader does.
func (s *Stack) StartEraser() {
	e := &iggdpr.Eraser{DB: s.dbs["scheduler"], S3: s.S3, Producer: s.Producer, Suppression: s.Crawler.Suppression}
	s.start("eraser", func(ctx context.Context) error { return scheduler.Every(ctx, "erasure", 100*time.Millisecond, e.Tick) })
}

// WaitStatus waits until the job is terminal and returns its status.
func (s *Stack) WaitStatus(t *testing.T, job string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var st string
		if err := s.DB.QueryRow(context.Background(), `SELECT status FROM jobs WHERE job_id = $1`, job).Scan(&st); err != nil {
			t.Fatal(err)
		}
		switch st {
		case "completed", "partial", "failed", "cancelled":
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s still %s after %s", job, st, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Key creates an API client with scopes and returns its key.
func (s *Stack) Key(t *testing.T, scopes ...string) string {
	t.Helper()
	k, _, err := httpapi.CreateClient(context.Background(), s.DB, "e2e", scopes, 0)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Do performs an API request and decodes a JSON answer into out (when not
// nil). It returns the status code.
func (s *Stack) Do(t *testing.T, key, method, path string, body any, out any, header ...string) int {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, s.API.URL+path, rdr)
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && len(b) > 0 && resp.StatusCode < 300 {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s %s: decode %s: %v", method, path, b, err)
		}
	}
	return resp.StatusCode
}

func (s *Stack) start(name string, comps ...run.Func) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	g := &group{cancel: cancel, done: make(chan struct{})}
	s.groups[name] = g
	go func() {
		defer close(g.done)
		if err := run.Group(ctx, comps...); err != nil && ctx.Err() == nil {
			slog.Error("igs e2e: component group failed", "group", name, "err", err)
		}
	}()
}

func (s *Stack) stop(name string) {
	s.mu.Lock()
	g := s.groups[name]
	delete(s.groups, name)
	s.mu.Unlock()
	if g != nil {
		g.cancel()
		<-g.done
	}
}

// RestartCrawler stops the crawler (in-flight tasks get Options.CrawlerDrain
// to finish) and starts a new one, as a pod replacement would.
func (s *Stack) RestartCrawler(t *testing.T) {
	t.Helper()
	s.stop("crawler")
	// As igs-crawler does once its consumers drained.
	s.Crawler.Park(context.Background())
	c, err := igapp.NewCrawler(context.Background(), s.Cfg, s.dbs["crawler"], s.Redis, s.Producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	s.Crawler = c
	s.start("crawler", c.Components...)
}

// RestartPersister replaces the persister consumer.
func (s *Stack) RestartPersister(t *testing.T) {
	t.Helper()
	s.stop("persister")
	pc, err := igapp.NewPersister(s.Cfg, s.dbs["persister"], s.Producer, 2000, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	s.start("persister", pc.Run)
}

// Stop shuts the stack down.
func (s *Stack) Stop() {
	s.mu.Lock()
	names := make([]string, 0, len(s.groups))
	for n := range s.groups {
		names = append(names, n)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.stop(n)
		}()
	}
	wg.Wait()
}

// Limits are the job limits of a default run.
func (s *Stack) Limits() *igstaskv1.Limits { return task.DefaultLimits(s.Cfg.IG.Limits) }

// Crawl starts a crawl of a profile, as the dispatcher will (M7): the job's
// profile task is registered with the tracker and produced. It returns the
// job id.
func (s *Stack) Crawl(t *testing.T, username string, mode commonv1.CrawlMode, lim *igstaskv1.Limits, sc *igstaskv1.ProfileScope) string {
	t.Helper()
	ctx := context.Background()
	job := "job-" + username + "-" + time.Now().Format("150405.000000")
	ref := "u:" + username
	if sc == nil {
		sc = &igstaskv1.ProfileScope{}
	}
	sc.ProfileRef, sc.Username = ref, username
	tk := task.NewProfile(job, igstaskv1.Lane_LANE_P0, mode, ref, sc.UserPk, lim, sc)
	if err := s.Jobs.Spawn(ctx, job, tk.TaskId); err != nil {
		t.Fatal(err)
	}
	topic, _ := topics.TaskTopic(tk.Kind, tk.Lane)
	rec, err := s.Producer.Record(topic, task.ScopeKey(tk), tk)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Producer.ProduceSync(ctx, rec); err != nil {
		t.Fatal(err)
	}
	return job
}

// Wait waits until every task of the jobs finished and the persister has
// written everything published so far, and returns each job's status.
func (s *Stack) Wait(t *testing.T, timeout time.Duration, jobIDs ...string) map[string]jobs.Status {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	out := map[string]jobs.Status{}
	for _, j := range jobIDs {
		for {
			st, err := s.Jobs.Status(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if st.Spawned > 0 && st.Pending == 0 {
				out[j] = st
				break
			}
			if time.Now().After(deadline) {
				prefix := redisx.KeyPrefix + "{job:" + j + "}:"
				missing, _ := s.Redis.SDiff(ctx, prefix+"spawned", prefix+"done").Result()
				t.Fatalf("job %s not done after %s: %+v; unfinished tasks %v", j, timeout, st, missing)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	ends, err := kafka.EndOffsets(ctx, s.Producer.Client(), s.topics...)
	if err != nil {
		t.Fatal(err)
	}
	for {
		ok, err := kafka.GroupCaughtUp(ctx, s.Producer.Client(), s.Producer.Topic(persist.Group), ends)
		if err == nil && ok {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("persister not caught up after %s (err %v)", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
