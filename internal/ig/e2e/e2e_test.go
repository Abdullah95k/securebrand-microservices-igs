package e2e

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	dto "github.com/prometheus/client_model/go"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igapp "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/app"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
)

const (
	full        = commonv1.CrawlMode_CRAWL_MODE_FULL
	incremental = commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
)

// spec is a world with every feature the stages handle: pinned posts,
// gated posts, hidden like counts and comments with replies.
func spec() fakeig.Spec {
	s := fakeig.DefaultSpec()
	s.Pinned, s.GatedEvery, s.HiddenLikesEvery, s.RepliesEvery = 2, 9, 5, 4
	return s
}

// A full crawl of each profile stores exactly what fakeig holds: the
// profile, its highlights, every post in the age window but the gated
// ones (with hidden like counts left empty and each reel's views from the
// reels tab), every top-level comment with its reply count, and the
// media of each post.
func TestFullCrawlEqualsTruth(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	lim := s.Limits()
	var ids []string
	for _, p := range s.World.Profiles() {
		ids = append(ids, s.Crawl(t, p.Username, full, lim, nil))
	}
	st := s.Wait(t, 3*time.Minute, ids...)
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	for i, p := range s.World.Profiles() {
		truth, _ := s.World.Truth(p.Username, since)
		checkTruth(t, s, truth)
		gated := int64(0)
		for _, pt := range truth.Posts {
			if pt.Gated {
				gated++
			}
		}
		c := st[ids[i]].Counters
		if c["posts_gated"] != gated || c["profiles"] != 1 {
			t.Errorf("%s: counters %v, want %d gated", p.Username, c, gated)
		}
	}
}

// Instagram limits each IP. With every exit limited for 2 seconds after 10
// queries and one exit sending fresh visitors to the login page, a full
// crawl of every profile still stores exactly the truth, and no task fails:
// blocked identities fail over to another exit within their task, exits
// rest and are probed again, and tasks park while every exit rests. A
// limited exit rests at its first 429 and identities on it send nothing
// more, so it answers about one 429 a limit; the walled exit gets only
// its probes.
func TestExitFailoverEqualsTruth(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Exits: 4, Faults: fakeig.Faults{ExitRateLimit: 10, ExitRateLimitFor: 2 * time.Second,
		WallExits: []string{igtest.ExitName(4)}}})
	lim := s.Limits()
	var ids []string
	for _, p := range s.World.Profiles() {
		ids = append(ids, s.Crawl(t, p.Username, full, lim, nil))
	}
	st := s.Wait(t, 4*time.Minute, ids...)
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	var hops int64
	for i, p := range s.World.Profiles() {
		truth, _ := s.World.Truth(p.Username, since)
		checkTruth(t, s, truth)
		c := st[ids[i]].Counters
		if c["tasks_failed"] != 0 {
			t.Errorf("%s: %d tasks failed: %v", p.Username, c["tasks_failed"], c)
		}
		hops += c["identity_hops"]
	}
	stats := s.Fake.ExitStats()
	limits, limited := 0, 0
	for i := 1; i <= 3; i++ {
		x := stats[igtest.ExitName(i)]
		if x.Requests == 0 {
			t.Errorf("exit %d was never used: %+v", i, stats)
		}
		limits += x.Limits
		limited += x.Limited
	}
	wall := stats[igtest.ExitName(4)]
	t.Logf("exits %+v; failovers %d", stats, hops)
	if limits == 0 || hops == 0 {
		t.Fatalf("no exit was rate-limited (%d) or no task failed over (%d): the test proves nothing", limits, hops)
	}
	if limited > 2*limits {
		t.Errorf("limited exits served %d requests in %d limits: they must rest at their first 429", limited, limits)
	}
	if wall.Requests > 20 {
		t.Errorf("the walled exit got %d page loads: it must rest after a walled one", wall.Requests)
	}
}

// A crawler pod that stops parks its idle identities, and the pod that
// replaces it resumes them (fakeig accepts their cookies and tokens), so a
// rolling restart does not load a page on every exit again. A rest one
// crawler pod takes holds for every other pod on the same exits.
func TestIdentitiesAndRestsOutliveAPod(t *testing.T) {
	sp := spec()
	sp.Profiles = 2
	s := Start(t, Options{Spec: sp, Exits: 2, Config: func(c *config.Config) { c.IG.ExitRestSync = 50 * time.Millisecond }})
	lim := s.Limits()
	ps := s.World.Profiles()
	s.Wait(t, 2*time.Minute, s.Crawl(t, ps[0].Username, full, lim, nil))
	resumed := func() float64 {
		var m dto.Metric
		_ = metrics.IdentityUnparked.WithLabelValues("isp").Write(&m)
		return m.GetCounter().GetValue()
	}
	before := resumed()
	s.RestartCrawler(t)
	st := s.Wait(t, 2*time.Minute, s.Crawl(t, ps[1].Username, full, lim, nil))
	if resumed() == before {
		t.Fatal("the new crawler resumed no parked identity")
	}
	truth, _ := s.World.Truth(ps[1].Username, time.Now().Add(-time.Duration(lim.MaxAgeSeconds)*time.Second))
	checkTruth(t, s, truth)
	for id, j := range st {
		if j.Counters["tasks_failed"] != 0 {
			t.Errorf("job %s: tasks failed after the restart: %v", id, j.Counters)
		}
	}

	other, err := igapp.NewCrawler(context.Background(), s.Cfg, s.DB, s.Redis, s.Producer, s.copts)
	if err != nil {
		t.Fatal(err)
	}
	s.start("crawler-b", other.Components...)
	ep := proxy.Endpoint{Tier: "isp", URL: "http://" + igtest.ExitName(1)}
	s.Crawler.Deps.Proxies.Rest(ep, time.Minute)
	for deadline := time.Now().Add(5 * time.Second); !other.Deps.Proxies.IsOpen(ep); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a rest one crawler took does not hold in the other")
		}
	}
	if other.Deps.Proxies.IsOpen(proxy.Endpoint{Tier: "isp", URL: "http://" + igtest.ExitName(2)}) {
		t.Fatal("the other exit rests too")
	}
}

// Highlights refused on every identity of several runs in a row, as when
// retries come back before a rate limit ends, still end with their items:
// they keep trying until one stall short of crawler.max_attempts. Before,
// they gave up after two such runs (7 refused queries here);
// TestExitFailoverEqualsTruth met that in CI.
func TestHighlightsOutlastRefusedRuns(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	const refused = 9
	s := Start(t, Options{Spec: sp, Faults: fakeig.Faults{RateLimitFirst: map[string]int{fakeig.OpHighlights: refused}}})
	lim := s.Limits()
	p := s.World.Profiles()[0]
	st := s.Wait(t, 2*time.Minute, s.Crawl(t, p.Username, full, lim, nil))
	truth, _ := s.World.Truth(p.Username, time.Now().Add(-time.Duration(lim.MaxAgeSeconds)*time.Second))
	checkTruth(t, s, truth)
	if n := s.Fake.QueryCount(fakeig.OpHighlights); n != refused+1 {
		t.Errorf("%d highlights queries, want %d (%d refused, then the items)", n, refused+1, refused)
	}
	for id, j := range st {
		if j.Counters["tasks_failed"] != 0 {
			t.Errorf("job %s: %v", id, j.Counters)
		}
	}
}

// With ig.capture_raw all, every response the crawl read is kept in the raw
// bucket: the GraphQL answers (profile, grid, reels, highlights, comments)
// and the post pages, so a parser fix can be applied to past data.
func TestRawResponsesKept(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	s := Start(t, Options{Spec: sp, RawCapture: true})
	p := s.World.Profiles()[0]
	s.Wait(t, 2*time.Minute, s.Crawl(t, p.Username, full, s.Limits(), nil))
	s.stop("crawler") // the sink flushes on stop
	seen := map[string]int{}
	for obj := range s.S3.ListObjects(context.Background(), s.Cfg.S3.RawBucket, minio.ListObjectsOptions{Prefix: "raw/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		q := strings.SplitN(strings.TrimPrefix(obj.Key, "raw/"), "/", 2)[0]
		seen[q]++
	}
	for _, q := range []string{"profile", "comments", "post_page", "reels"} {
		if seen[q] == 0 {
			t.Errorf("no raw %s responses kept: %v", q, seen)
		}
	}
}

// A highlights query that keeps meeting rate limits fails over to other
// identities within the run, then retries: the highlights end with their
// items rather than published without them. (TestExitFailoverEqualsTruth
// met it once in CI: two rate-limited attempts, and the items were gone.)
func TestHighlightsSurviveRateLimits(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	s := Start(t, Options{Spec: sp, Faults: fakeig.Faults{RateLimitFirst: map[string]int{fakeig.OpHighlights: 3}}})
	lim := s.Limits()
	p := s.World.Profiles()[0]
	s.Wait(t, 2*time.Minute, s.Crawl(t, p.Username, full, lim, nil))
	truth, _ := s.World.Truth(p.Username, time.Now().Add(-time.Duration(lim.MaxAgeSeconds)*time.Second))
	checkTruth(t, s, truth)
	if n := s.Fake.QueryCount(fakeig.OpHighlights); n != 4 {
		t.Errorf("%d highlights queries, want 4 (3 refused, then the items)", n)
	}
}

// A profile task that parked while every exit rested still fails over when
// its highlights query is then rate-limited: parks are no failed tries.
// (They used to count: two parks, and a single block made the highlights
// go without their items, as TestExitFailoverEqualsTruth met in CI.)
func TestHighlightsAfterParks(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	s := Start(t, Options{Spec: sp, Exits: 2, Faults: fakeig.Faults{RateLimitFirst: map[string]int{fakeig.OpHighlights: 1}}})
	for i := 1; i <= 2; i++ {
		s.Crawler.Deps.Proxies.Rest(proxy.Endpoint{Tier: "isp", URL: "http://" + igtest.ExitName(i)}, 8*time.Second)
	}
	lim := s.Limits()
	p := s.World.Profiles()[0]
	st := s.Wait(t, 2*time.Minute, s.Crawl(t, p.Username, full, lim, nil))
	truth, _ := s.World.Truth(p.Username, time.Now().Add(-time.Duration(lim.MaxAgeSeconds)*time.Second))
	checkTruth(t, s, truth)
	for _, c := range st {
		if c.Counters["identity_hops"] == 0 {
			t.Errorf("the rate-limited highlights did not fail over: %v", c.Counters)
		}
	}
}

// checkTruth compares the store with a profile's ground truth.
func checkTruth(t *testing.T, s *Stack, truth fakeig.ProfileTruth) {
	t.Helper()
	ctx := context.Background()
	db := s.DB
	var username, fullName string
	var followers int64
	var verified bool
	var hash []byte
	if err := db.QueryRow(ctx, `SELECT username, full_name, follower_count, verified, highlights_hash FROM profiles WHERE user_pk = $1`,
		truth.PK).Scan(&username, &fullName, &followers, &verified, &hash); err != nil {
		t.Fatalf("%s: profile: %v", truth.Username, err)
	}
	if username != truth.Username || fullName != truth.FullName || followers != truth.Followers || verified != truth.Verified || len(hash) == 0 {
		t.Errorf("%s: profile %q %q %d %v hash %x", truth.Username, username, fullName, followers, verified, hash)
	}
	var nh, items int
	if err := db.QueryRow(ctx, `SELECT count(*), coalesce(sum((SELECT count(*) FROM posts i WHERE i.kind = 'highlight' AND i.highlight_id = h.highlight_id AND i.status = 'visible')), 0)
		FROM highlights h WHERE user_pk = $1`, truth.PK).Scan(&nh, &items); err != nil {
		t.Fatal(err)
	}
	wantItems := 0
	for _, h := range truth.Highlights {
		wantItems += h.Items
	}
	if nh != len(truth.Highlights) || items != wantItems {
		t.Errorf("%s: %d highlights with %d items, want %d with %d", truth.Username, nh, items, len(truth.Highlights), wantItems)
	}

	type row struct {
		likes, comments, plays *int64
		pinned                 *bool
		children               *int32
		status                 string
	}
	stored := map[int64]row{}
	rows, err := db.Query(ctx, `SELECT media_pk, like_count, comment_count, play_count, is_pinned, child_count, status FROM posts WHERE owner_pk = $1 AND kind <> 'highlight'`, truth.PK)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var pk int64
		var r row
		if err := rows.Scan(&pk, &r.likes, &r.comments, &r.plays, &r.pinned, &r.children, &r.status); err != nil {
			t.Fatal(err)
		}
		stored[pk] = r
	}
	rows.Close()
	want := 0
	for _, pt := range truth.Posts {
		r, ok := stored[pt.PK]
		if pt.Gated {
			if ok {
				t.Errorf("%s: gated post %d stored", truth.Username, pt.PK)
			}
			continue
		}
		want++
		if !ok {
			t.Errorf("%s: post %d (%s) missing", truth.Username, pt.PK, pt.TakenAt.Format(time.DateOnly))
			continue
		}
		if !eqPtr(r.likes, pt.Likes) || r.comments == nil || *r.comments != pt.CommentCount || !eqPtr(r.plays, pt.Plays) ||
			r.pinned == nil || *r.pinned != pt.Pinned || r.status != "visible" {
			t.Errorf("%s: post %d: likes %v/%v comments %v/%d plays %v/%v pinned %v/%v %s", truth.Username, pt.PK,
				show(r.likes), show(pt.Likes), show(r.comments), pt.CommentCount, show(r.plays), show(pt.Plays), r.pinned, pt.Pinned, r.status)
		}
		if pt.Carousel > 0 && (r.children == nil || int(*r.children) != pt.Carousel) {
			t.Errorf("%s: carousel %d has %v children, want %d", truth.Username, pt.PK, r.children, pt.Carousel)
		}
		checkComments(t, s, truth.Username, pt)
	}
	if len(stored) != want {
		t.Errorf("%s: %d posts stored, want %d", truth.Username, len(stored), want)
	}
	var media int
	if err := db.QueryRow(ctx, `SELECT count(DISTINCT coalesce(parent_media_pk, media_pk)) FROM media WHERE user_pk = $1 AND owner_type = 'post'`,
		truth.PK).Scan(&media); err != nil {
		t.Fatal(err)
	}
	if media != want {
		t.Errorf("%s: media of %d posts, want %d", truth.Username, media, want)
	}
	var state string
	if err := db.QueryRow(ctx, `SELECT state FROM crawl_cursors WHERE scope_type = 'profile_grid' AND scope_id = $1`, truth.PK).Scan(&state); err != nil || state != "complete" {
		t.Errorf("%s: grid cursor %q %v", truth.Username, state, err)
	}
}

func checkComments(t *testing.T, s *Stack, user string, pt fakeig.PostTruth) {
	t.Helper()
	rows, err := s.DB.Query(context.Background(), `SELECT comment_pk, text, coalesce(child_comment_count, 0) FROM comments WHERE media_pk = $1`, pt.PK)
	if err != nil {
		t.Fatal(err)
	}
	type c struct {
		text    string
		replies int64
	}
	got := map[int64]c{}
	for rows.Next() {
		var pk, replies int64
		var text string
		if err := rows.Scan(&pk, &text, &replies); err != nil {
			t.Fatal(err)
		}
		got[pk] = c{text, replies}
	}
	rows.Close()
	if len(got) != len(pt.Comments) {
		t.Errorf("%s: post %d: %d comments stored, want %d", user, pt.PK, len(got), len(pt.Comments))
	}
	for _, w := range pt.Comments {
		g, ok := got[w.PK]
		if !ok || g.text != w.Text || g.replies != int64(w.Replies) {
			t.Errorf("%s: post %d comment %d: %+v (stored %v), want %q with %d replies", user, pt.PK, w.PK, g, ok, w.Text, w.Replies)
			return
		}
	}
}

func eqPtr(a, b *int64) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func show(p *int64) any {
	if p == nil {
		return "nil"
	}
	return *p
}

// A scheduled (incremental) run reads only the posts published since the
// newest stored one, and the reels tab brings every reel's counts up to
// date.
func TestIncrementalReadsNewPosts(t *testing.T) {
	// Pages of 12: the grid and the comments take several pages, and the
	// walks several segments.
	s := Start(t, Options{Spec: spec(), Faults: fakeig.Faults{MaxPageSize: 12}})
	lim := s.Limits()
	p := s.World.Profiles()[0]
	s.Wait(t, 3*time.Minute, s.Crawl(t, p.Username, full, lim, nil))

	added, err := s.World.AddPosts(p.Username, 3, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var reel int64
	for _, x := range p.Reels() {
		if !x.LikesHidden && !x.Gated {
			reel = x.PK
			break
		}
	}
	if err := s.World.SetLikes(reel, 77777); err != nil {
		t.Fatal(err)
	}
	job := s.Crawl(t, p.Username, incremental, lim, nil)
	st := s.Wait(t, 3*time.Minute, job)
	newPosts := int64(0)
	for _, x := range added {
		if !x.Gated {
			newPosts++
		}
	}
	if got := st[job].Counters["posts"]; got != int64(len(added)) {
		t.Errorf("incremental spawned %d post tasks, want %d (counters %v)", got, len(added), st[job].Counters)
	}
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	truth, _ := s.World.Truth(p.Username, since)
	checkTruth(t, s, truth)
	var likes int64
	if err := s.DB.QueryRow(context.Background(), `SELECT like_count FROM posts WHERE media_pk = $1`, reel).Scan(&likes); err != nil || likes != 77777 {
		t.Errorf("reel likes %d %v, want the reels tab's 77777", likes, err)
	}
	var newest time.Time
	if err := s.DB.QueryRow(context.Background(), `SELECT newest_seen_at FROM crawl_cursors WHERE scope_type = 'profile_grid' AND scope_id = $1`,
		p.PK).Scan(&newest); err != nil || !newest.Equal(added[0].TakenAt) {
		t.Errorf("grid watermark %s %v, want the newest post's %s", newest, err, added[0].TakenAt)
	}
	_ = newPosts
}

// Rate limits, login walls after a few queries, walled first page loads
// and crashes of the crawler and the persister mid-run still end with the
// store equal to the truth.
func TestChaosEqualsTruth(t *testing.T) {
	s := Start(t, Options{Spec: spec(), CrawlerDrain: 50 * time.Millisecond,
		Faults: fakeig.Faults{RateLimitEvery: 11, LoginWallAfter: 30, PageWallFirst: 2}})
	lim := s.Limits()
	var ids []string
	for _, p := range s.World.Profiles() {
		ids = append(ids, s.Crawl(t, p.Username, full, lim, nil))
	}
	time.Sleep(700 * time.Millisecond)
	s.RestartCrawler(t)
	time.Sleep(500 * time.Millisecond)
	s.RestartPersister(t)
	time.Sleep(500 * time.Millisecond)
	s.RestartCrawler(t)
	s.Wait(t, 4*time.Minute, ids...)
	since := time.Now().Add(-time.Duration(lim.MaxAgeSeconds) * time.Second)
	names := []string{}
	for _, p := range s.World.Profiles() {
		names = append(names, p.Username)
	}
	sort.Strings(names)
	for _, n := range names {
		truth, _ := s.World.Truth(n, since)
		checkTruth(t, s, truth)
	}
}
