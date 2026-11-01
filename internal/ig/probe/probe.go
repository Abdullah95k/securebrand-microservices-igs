// Package probe answers the open questions the build cannot answer without
// real Instagram traffic (brief §3, checkpoint C2). The owner runs it from
// their own machine, once direct and once per proxy tier:
//
//   - U1: does the grid keep paging past the 24 posts the browser showed,
//     and how deep before a wall?
//   - U3: do queries work without the browser's client-state fields? Every
//     probe query is sent without them.
//   - U5: is a grid cursor bound to the identity that received it?
//   - U8: which cookies does a page load set?
//   - U9: does the grid query answer by user pk alone, with no cursor?
//
// It also reports which page tokens were found, and every request's class,
// latency and wire bytes.
//
// Beside the answers, the report holds what the probe read (owner's
// decision, 2026-10-31): each profile's public details, its posts' links,
// captions and thumbnails, the newest posts' comments (as many posts and
// pages as asked), and the profile picture saved as a file. That is personal data: the command
// writes it 0600 under phase0/, which git ignores, and it is never
// committed.
package probe

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"golang.org/x/time/rate"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	scrapehtml "github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Options configure a run.
type Options struct {
	BaseURL  string   // https://www.instagram.com
	Profiles []string // usernames, in the owner's file order
	Tier     string   // proxy tier ("direct" for none)
	// MaxGridPages caps the U1 walk after the first grid page (12 posts each).
	MaxGridPages int
	// MaxAge, when set, stops the walk once the posts read are older than
	// this (judged by the time their pk encodes): the backfill window.
	MaxAge time.Duration
	Pool   *session.Pool
	Loader *session.Loader
	GQL    *graphql.Client
	// Fingerprint names the profile the pool uses, for the report.
	Fingerprint string
	Version     string
	// ImageDir, when set, receives each profile picture as <username>.jpg.
	ImageDir string
	// RawDir, when set, receives Instagram's raw answers, to check their
	// shape: per profile the first response of each query and every
	// response that did not answer OK or held no items, and the page HTML of
	// the first RawPages profiles.
	RawDir   string
	RawPages int
	// Progress, when set, gets one line per finished profile, with the
	// running totals of posts and comments.
	Progress func(string)
	// OnProfile, when set, gets each profile as soon as it is read, so the
	// caller can save it before the run ends (igs-spike appends the CSVs).
	OnProfile func(Profile)
	// StartAt is the line (1-based, in Profiles) to start at, to continue a
	// run that was stopped; earlier profiles are skipped. Profiles keep
	// their line numbers. 0 or 1 starts at the first.
	StartAt int
	// CommentPosts is how many of each profile's newest posts get their
	// comments read, and CommentPages how many comment pages (10 comments
	// each, newest first) per post. Both default to 1.
	CommentPosts int
	CommentPages int
	// Parallel is how many profiles are read at once, each by its own
	// identity (and so its own exit IP on a static list), each at the same
	// pace. 0 or 1 reads one at a time.
	Parallel int
	// MaxWait is how long a profile waits for an exit when every exit of
	// the tier is cooling down, before it is given up (0: no wait). The
	// full run gave up 274 profiles in a minute without it.
	MaxWait time.Duration
	// PageLoadTries is how many identities a profile's page load may try
	// when it gets the login redirect (some ISP exits are flagged for page
	// loads); 0 means 5. Flagged exits rest (PageWallRest), so each costs
	// one wall, and 5 tries get past a run of them: in the details run three
	// flagged exits in a row failed 27 profiles at 3 tries.
	PageLoadTries int
	// PageWallRest is how long an exit rests (gets no identities) once it
	// looks flagged for page loads: it sent a fresh visitor to the login
	// page and has served no good page load in this run, or did so twice in
	// a row. 0 means 30 minutes; negative, never. Three of the owner's ten
	// ISP exits served only the login page in the details run, and a retry
	// walked from one to the next.
	PageWallRest time.Duration
	// GridPageSize and CommentPageSize ask Instagram for that many posts
	// or comments per page ("first"); 0 keeps the web app's 12 and 10.
	// Whether Instagram serves more is what a test run shows.
	GridPageSize    int
	CommentPageSize int
	// RPS is the pace of each identity's requests; CommentRPS, when higher,
	// is the pace of comment requests only (the pool's identities run at
	// CommentRPS, and the probe holds everything else to RPS).
	RPS        float64
	CommentRPS float64
	// CommentHop, when above 1, sends comment requests through that many
	// extra identities in turn (a post's comment pages each from another
	// exit), bootstrapped once and shared by the workers.
	CommentHop int
	// PostDetails is how many of each profile's newest posts (by time, so
	// pinned posts do not crowd them out) get their own page read for
	// likes, comment count and the rest (PostDetails); 0 means 1. A post
	// page is about 125 KB.
	PostDetails int
	// ReelsPages is how many pages of the profile's reels tab to read
	// (0: none): per reel the view, like and comment counts, about 1 KB a
	// reel; MaxAge stops it too. ReelsPageSize asks that many per page (0:
	// the web app's 4).
	ReelsPages    int
	ReelsPageSize int
	Now           func() time.Time
}

// Step is one request's outcome.
type Step struct {
	Class     string `json:"class"`
	Status    int    `json:"status,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Ms        int64  `json:"ms"`
	WireBytes int64  `json:"wire_bytes"`
	Items     int    `json:"items,omitempty"`
	HasNext   bool   `json:"has_next,omitempty"`
	// Errors are the GraphQL error messages the answer carried beside its
	// data (an answer classed OK can hold some), each once.
	Errors []string `json:"errors,omitempty"`
}

// Grid is the U1 walk of one profile.
type Grid struct {
	Pages int `json:"pages"` // after the first
	Posts int `json:"posts"` // including the first page
	// Stop is "end" (has_next_page false), "cap" (MaxGridPages), "age"
	// (MaxAge reached), "stopped" (the operator stopped the run), or
	// "wall:<class>" for the first page that did not answer.
	Stop     string `json:"stop"`
	PastPage bool   `json:"past_24"` // read more than 24 posts
	// Oldest is when the oldest post read was posted (from its pk).
	Oldest time.Time `json:"oldest,omitzero"`
	Failed *Step     `json:"failed,omitempty"`
}

// ProfileData is what the profile query returned.
type ProfileData struct {
	PK                 string      `json:"pk"`
	FullName           string      `json:"full_name"`
	Biography          string      `json:"biography"`
	BioLinks           []Link      `json:"bio_links,omitempty"`
	Verified           bool        `json:"is_verified"`
	Private            bool        `json:"is_private"`
	Memorialized       bool        `json:"is_memorialized"`
	Followers          int64       `json:"follower_count"`
	Following          int64       `json:"following_count"`
	ProfilePicURL      string      `json:"profile_pic_url"`
	ProfilePicFile     string      `json:"profile_pic_file,omitempty"`
	LinkedFacebookPage string      `json:"linked_facebook_page,omitempty"`
	HasClips           bool        `json:"has_clips"`
	Highlights         []Highlight `json:"highlights,omitempty"`
}

// Link is a bio link.
type Link struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url"`
}

// Highlight is one highlights reel; Items is known for the one the probe read.
type Highlight struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Items int    `json:"items,omitempty"`
}

// Post is one grid item. Grid items carry no time or counts: those need the
// post page (U2).
type Post struct {
	PK        string `json:"pk"`
	Shortcode string `json:"shortcode"`
	URL       string `json:"url"`
	MediaType string `json:"media_type"`
	// PostedAt is when the post was made, from the time its pk encodes
	// (within about a minute of the post page's taken_at).
	PostedAt     time.Time `json:"posted_at,omitzero"`
	ProductType  string    `json:"product_type,omitempty"`
	Caption      string    `json:"caption,omitempty"`
	AltText      string    `json:"alt_text,omitempty"`
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
	// Pinned is inferred: the grid shows pinned posts first, and nothing in
	// its answer marks them, so a post among the first three that is older
	// than a later one is taken as pinned.
	Pinned bool `json:"pinned,omitempty"`
	// Details are what the post's own page adds, for the newest posts
	// (Options.PostDetails).
	Details *PostDetails `json:"details,omitempty"`
	// Plays, Likes and Comments are the post's counts from the best
	// source read: its page (Details) or, for a reel, the reels tab, which
	// alone shows views (Plays).
	Plays    *int64 `json:"plays,omitempty"`
	Likes    *int64 `json:"likes,omitempty"`
	Comments *int64 `json:"comments,omitempty"`
}

// Reel is one item of a profile's reels tab.
type Reel struct {
	PK           string    `json:"pk"`
	Shortcode    string    `json:"shortcode"`
	URL          string    `json:"url"`
	PostedAt     time.Time `json:"posted_at,omitzero"` // from the pk
	Plays        *int64    `json:"plays,omitempty"`
	Likes        *int64    `json:"likes,omitempty"`
	Comments     *int64    `json:"comments,omitempty"`
	CountsHidden bool      `json:"counts_hidden,omitempty"`
	Pinned       bool      `json:"pinned,omitempty"` // pinned in the reels tab
	ThumbnailURL string    `json:"thumbnail_url,omitempty"`
}

// PostDetails is what a post's page shows that the grid does not. Logged
// out, it holds no view, play or share counts (the owner's full run).
type PostDetails struct {
	Class         string    `json:"class"` // the page load's outcome
	TakenAt       time.Time `json:"taken_at,omitzero"`
	Likes         *int64    `json:"likes,omitempty"`
	Comments      *int64    `json:"comments,omitempty"`
	LikesHidden   bool      `json:"likes_hidden,omitempty"`
	CaptionEdited bool      `json:"caption_edited,omitempty"`
	Tagged        []string  `json:"tagged,omitempty"`    // usernames (full names when no username)
	Coauthors     []string  `json:"coauthors,omitempty"` // usernames
	Location      string    `json:"location,omitempty"`
	Music         string    `json:"music,omitempty"`  // a licensed track, or the original audio and its artist
	Topics        []string  `json:"topics,omitempty"` // Instagram's related topic pills
	DurationS     float64   `json:"duration_s,omitempty"`
	Width         int       `json:"width,omitempty"`
	Height        int       `json:"height,omitempty"`
	HasAudio      bool      `json:"has_audio,omitempty"`
	AILabel       string    `json:"ai_label,omitempty"`
	CarouselCount int       `json:"carousel_count,omitempty"`
	VideoURL      string    `json:"video_url,omitempty"` // expires
	ImageURL      string    `json:"image_url,omitempty"` // expires
}

// PostPage is what a post page held.
type PostPage struct {
	Step
	URL              string    `json:"url"`
	TakenAt          time.Time `json:"taken_at,omitzero"`
	LikeCount        *int64    `json:"like_count,omitempty"`
	CommentCount     *int64    `json:"comment_count,omitempty"`
	CommentsEmbedded int       `json:"comments_embedded"`
}

// Comment is one top-level comment of one of the newest posts.
type Comment struct {
	PostShortcode string    `json:"post_shortcode"`
	PK            string    `json:"pk"`
	Username      string    `json:"username"`
	Verified      bool      `json:"is_verified"`
	Text          string    `json:"text"`
	CreatedAt     time.Time `json:"created_at"`
	Likes         int64     `json:"likes"`
}

// Profile is one profile's results; Index is its 1-based line in the list.
type Profile struct {
	Index       int             `json:"index"`
	Username    string          `json:"username"`
	URL         string          `json:"url"`
	Data        *ProfileData    `json:"data,omitempty"`
	Posts       []Post          `json:"posts,omitempty"`
	Comments    []Comment       `json:"comments,omitempty"`
	Bootstrap   Step            `json:"bootstrap"`
	TokensFound map[string]bool `json:"tokens_found,omitempty"`
	Cookies     []string        `json:"cookies,omitempty"`
	CSRFCookie  bool            `json:"csrf_cookie,omitempty"`
	Profile     *Step           `json:"profile,omitempty"`
	GridFirst   *Step           `json:"grid_first,omitempty"`
	Grid        *Grid           `json:"grid,omitempty"`
	OtherCursor *Step           `json:"cursor_other_identity,omitempty"`
	// OtherBootstrap is the page load of the second identity, when this
	// profile was the one that tried to start it.
	OtherBootstrap *Step `json:"other_identity_bootstrap,omitempty"`
	// NewestPost is the newest post's own page: what it holds that the grid
	// does not (U2's baseline), and its embedded first comments.
	NewestPost   *PostPage `json:"newest_post_page,omitempty"`
	GridByPK     *Step     `json:"grid_by_pk_no_cursor,omitempty"`
	CommentsRead *Step     `json:"comments_read,omitempty"`
	Highlights   *Step     `json:"highlights,omitempty"`
	NoHighlights bool      `json:"no_highlights,omitempty"`
	// Stopped marks the profile being read when the operator stopped the
	// run (Ctrl-C): what it holds is partial, and its unanswered steps
	// count for no question.
	Stopped bool `json:"stopped,omitempty"`
	// Identity and Exit say which identity read the profile and through
	// which exit group (a static IP's /24; no credentials), to relate walls
	// to exits.
	Identity string `json:"identity,omitempty"`
	Exit     string `json:"exit,omitempty"`
	// PageLoadWalls counts the identities whose page load got the login
	// redirect before one loaded; WaitedSec how long the profile waited for
	// a cooling tier.
	PageLoadWalls int `json:"page_load_walls,omitempty"`
	WaitedSec     int `json:"waited_s,omitempty"`
	// Reels is the reels tab read (Options.ReelsPages); ReelsFirst its
	// first request, and ReelsStop why the walk ended.
	Reels      []Reel `json:"reels,omitempty"`
	ReelsFirst *Step  `json:"reels_first,omitempty"`
	ReelsStop  string `json:"reels_stop,omitempty"`
}

// Report is the whole run.
type Report struct {
	Version     string    `json:"version"`
	Tier        string    `json:"tier"`
	Fingerprint string    `json:"fingerprint"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	// StartAt is the profile line the run started at, when not the first.
	StartAt int `json:"start_at,omitempty"`
	// Parallel is how many profiles were read at once, when more than one.
	Parallel int `json:"parallel,omitempty"`
	// ContinueAt, after a stopped run, is the first profile line not read
	// completely: run again with StartAt set to it.
	ContinueAt int       `json:"continue_at,omitempty"`
	Profiles   []Profile `json:"profiles"`
	Summary    Summary   `json:"summary"`
}

// Summary answers the questions over all profiles.
type Summary struct {
	Profiles int `json:"profiles"`
	// Posts and Comments are the totals read.
	Posts    int `json:"posts"`
	Comments int `json:"comments"`
	// StoppedEarly: the operator stopped the run (Ctrl-C) before the last
	// profile; the last profile in the report is partial.
	StoppedEarly bool           `json:"stopped_early,omitempty"`
	Bootstrapped int            `json:"bootstrapped"`
	TokensFound  map[string]int `json:"tokens_found"`
	// PageLoadWalls counts page loads that got the login redirect (each
	// retried on another identity); TierWaits the profiles that waited for
	// a cooling tier.
	PageLoadWalls int `json:"page_load_walls"`
	TierWaits     int `json:"tier_waits"`
	// PageWallExits counts the login redirects by exit (every try, the last
	// too); ExitsRested the times an exit was rested for them.
	PageWallExits map[string]int `json:"page_wall_exits,omitempty"`
	ExitsRested   int            `json:"exits_rested,omitempty"`
	// U1
	GridEnds       int            `json:"grid_ends"`
	GridCaps       int            `json:"grid_caps"`
	GridAgeStops   int            `json:"grid_age_stops"`
	GridWalls      map[string]int `json:"grid_walls"`
	PagedPast24    int            `json:"paged_past_24"`
	MaxPosts       int            `json:"max_posts"`
	WalledAtPosts  []int          `json:"walled_at_posts,omitempty"`
	GridFirstAlone int            `json:"grid_first_only"` // a first page with no next page
	// U3: queries sent without client-state fields, and how they answered.
	QueriesWithoutClientState map[string]int `json:"queries_without_client_state"`
	// U5
	CursorPortable    int `json:"cursor_portable"`
	CursorNotPortable int `json:"cursor_not_portable"`
	// U8
	Cookies    map[string]int `json:"cookies"`
	CSRFCookie int            `json:"csrf_cookie"`
	// U9
	GridByPKWorks int `json:"grid_by_pk_works"`
	GridByPKFails int `json:"grid_by_pk_fails"`
	// Cost: per request kind, count, mean latency and mean wire bytes.
	Requests map[string]Cost `json:"requests"`
	// The speed tests: the largest grid and comment pages served, comment
	// requests by answer and their time (summed over workers, so per
	// chain), and the identities comment requests hopped across.
	GridMaxPage          int            `json:"grid_max_page"`
	CommentMaxPage       int            `json:"comment_max_page"`
	CommentRequests      int            `json:"comment_requests"`
	CommentAnswers       map[string]int `json:"comment_answers"`
	CommentMs            int64          `json:"comment_ms"`
	CommentHopIdentities int            `json:"comment_hop_identities,omitempty"`
	// Post pages read for details, and posts inferred pinned.
	PostDetails int `json:"post_details"`
	PinnedPosts int `json:"pinned_posts"`
	// The reels tab: reels read, the largest page served, and whether its
	// first page came by the user's id without a cursor or, failing that,
	// from the reels page's HTML.
	Reels             int `json:"reels"`
	ReelsMaxPage      int `json:"reels_max_page"`
	ReelsByIDWorks    int `json:"reels_by_id_works"`
	ReelsPageFallback int `json:"reels_page_fallback"`
}

// Cost aggregates one request kind.
type Cost struct {
	N         int   `json:"n"`
	MeanMs    int64 `json:"mean_ms"`
	MeanBytes int64 `json:"mean_wire_bytes"`
	sumMs     int64
	sumBytes  int64
}

const (
	// stopped is the class of a step cut short by the operator (Ctrl-C),
	// and the grid's stop when that happens mid-walk.
	stopped = "stopped"
	// progressEvery is how often, in grid pages, a long walk reports
	// progress (about every 30 seconds at the probe's pace).
	progressEvery = 10
)

// Run probes every profile and returns the report.
func Run(ctx context.Context, o Options) Report {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxGridPages <= 0 {
		o.MaxGridPages = 32
	}
	o.CommentPosts, o.CommentPages, o.Parallel = max(o.CommentPosts, 1), max(o.CommentPages, 1), max(o.Parallel, 1)
	if o.PageLoadTries <= 0 {
		o.PageLoadTries = 5
	}
	if o.PageWallRest == 0 {
		o.PageWallRest = 30 * time.Minute
	}
	o.PostDetails = max(o.PostDetails, 1)
	rep := Report{Version: o.Version, Tier: o.Tier, Fingerprint: o.Fingerprint, StartedAt: o.Now().UTC()}
	if o.StartAt > 1 {
		rep.StartAt = o.StartAt
	}
	if o.Parallel > 1 {
		rep.Parallel = o.Parallel
	}
	// Workers share the progress callback (a long walk reports from its
	// worker) and the U5 identity; everything else is per worker, and
	// their summaries are added up at the end.
	var progressMu sync.Mutex
	progress := func(line string) {
		if o.Progress != nil {
			progressMu.Lock()
			defer progressMu.Unlock()
			o.Progress(line)
		}
	}
	wo := o
	wo.Progress = progress
	sh := &shared{}
	type job struct {
		index    int
		username string
	}
	jobs := make(chan job)
	results := make(chan Profile)
	runners := make([]*runner, o.Parallel)
	var wg sync.WaitGroup
	for w := range runners {
		r := newRunner(wo, sh)
		runners[w] = r
		wg.Go(func() {
			for j := range jobs {
				if ctx.Err() != nil {
					continue // stopped: drain without starting another profile
				}
				results <- r.profile(ctx, j.index, j.username)
			}
		})
	}
	go func() {
		defer close(jobs)
		for i, username := range o.Profiles {
			if i+1 < o.StartAt {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			select {
			case jobs <- job{i + 1, username}:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	sum := newSummary()
	for p := range results {
		if ctx.Err() != nil {
			p.Stopped, sum.StoppedEarly = true, true
		}
		rep.Profiles = append(rep.Profiles, p)
		sum.Posts += len(p.Posts)
		sum.Comments += len(p.Comments)
		if o.OnProfile != nil {
			o.OnProfile(p)
		}
		line := fmt.Sprintf("profile %d/%d %s: bootstrap %s, grid %s, %d comments; so far %d posts, %d comments",
			p.Index, len(o.Profiles), p.Username, p.Bootstrap.Class, gridLine(p.Grid), len(p.Comments), sum.Posts, sum.Comments)
		if p.Stopped {
			line = fmt.Sprintf("profile %d/%d %s: stopped (Ctrl-C) after %d posts; the report keeps what was read", p.Index, len(o.Profiles), p.Username, len(p.Posts))
		}
		progress(line)
	}
	if sh.second != nil {
		o.Pool.Release(sh.second)
	}
	for _, h := range sh.hops {
		o.Pool.Release(h)
	}
	for _, r := range runners {
		sum.add(r.sum)
	}
	for k, c := range sum.Requests {
		c.MeanMs, c.MeanBytes = c.sumMs/int64(c.N), c.sumBytes/int64(c.N)
		sum.Requests[k] = c
	}
	slices.SortFunc(rep.Profiles, func(a, b Profile) int { return a.Index - b.Index })
	sum.Profiles = len(rep.Profiles)
	slices.Sort(sum.WalledAtPosts)
	sum.CommentHopIdentities = sh.hopCreated
	if sum.StoppedEarly {
		rep.ContinueAt = continueAt(rep.Profiles, max(o.StartAt, 1), len(o.Profiles))
	}
	rep.Summary = sum
	rep.FinishedAt = o.Now().UTC()
	return rep
}

// waitPoll is how often a profile waiting for a cooling tier asks again.
var waitPoll = 15 * time.Second

// acquire gets an identity, waiting up to maxWait (asking every poll) while
// every exit of the tier is cooling down; onWait is called once when the
// wait starts. It returns how long it waited.
func acquire(ctx context.Context, maxWait, poll time.Duration, get func() (*session.Identity, error), onWait func()) (*session.Identity, time.Duration, error) {
	start := time.Now()
	for waits := 0; ; waits++ {
		id, err := get()
		if !errors.Is(err, proxy.ErrTierOpen) || time.Since(start) >= maxWait {
			var waited time.Duration
			if waits > 0 {
				waited = time.Since(start)
			}
			return id, waited, err
		}
		if waits == 0 {
			onWait()
		}
		select {
		case <-ctx.Done():
			return nil, time.Since(start), ctx.Err()
		case <-time.After(poll):
		}
	}
}

// continueAt is the first line from start that was not read completely.
func continueAt(profiles []Profile, start, n int) int {
	done := map[int]bool{}
	for _, p := range profiles {
		done[p.Index] = !p.Stopped
	}
	for i := start; i <= n; i++ {
		if !done[i] {
			return i
		}
	}
	return 0
}

func newSummary() Summary {
	return Summary{TokensFound: map[string]int{}, GridWalls: map[string]int{},
		QueriesWithoutClientState: map[string]int{}, Cookies: map[string]int{}, Requests: map[string]Cost{},
		CommentAnswers: map[string]int{}, PageWallExits: map[string]int{}}
}

// add adds a worker's counts to s. Profiles, Posts, Comments and
// StoppedEarly are the collector's.
func (s *Summary) add(o Summary) {
	addMap := func(dst, src map[string]int) {
		for k, v := range src {
			dst[k] += v
		}
	}
	s.Bootstrapped += o.Bootstrapped
	addMap(s.TokensFound, o.TokensFound)
	s.PageLoadWalls += o.PageLoadWalls
	s.TierWaits += o.TierWaits
	addMap(s.PageWallExits, o.PageWallExits)
	s.ExitsRested += o.ExitsRested
	s.GridEnds += o.GridEnds
	s.GridCaps += o.GridCaps
	s.GridAgeStops += o.GridAgeStops
	addMap(s.GridWalls, o.GridWalls)
	s.PagedPast24 += o.PagedPast24
	s.MaxPosts = max(s.MaxPosts, o.MaxPosts)
	s.WalledAtPosts = append(s.WalledAtPosts, o.WalledAtPosts...)
	s.GridFirstAlone += o.GridFirstAlone
	addMap(s.QueriesWithoutClientState, o.QueriesWithoutClientState)
	s.CursorPortable += o.CursorPortable
	s.CursorNotPortable += o.CursorNotPortable
	addMap(s.Cookies, o.Cookies)
	s.CSRFCookie += o.CSRFCookie
	s.GridByPKWorks += o.GridByPKWorks
	s.GridByPKFails += o.GridByPKFails
	for k, c := range o.Requests {
		t := s.Requests[k]
		t.N, t.sumMs, t.sumBytes = t.N+c.N, t.sumMs+c.sumMs, t.sumBytes+c.sumBytes
		s.Requests[k] = t
	}
	s.GridMaxPage = max(s.GridMaxPage, o.GridMaxPage)
	s.CommentMaxPage = max(s.CommentMaxPage, o.CommentMaxPage)
	s.CommentRequests += o.CommentRequests
	addMap(s.CommentAnswers, o.CommentAnswers)
	s.CommentMs += o.CommentMs
	s.PostDetails += o.PostDetails
	s.PinnedPosts += o.PinnedPosts
	s.Reels += o.Reels
	s.ReelsMaxPage = max(s.ReelsMaxPage, o.ReelsMaxPage)
	s.ReelsByIDWorks += o.ReelsByIDWorks
	s.ReelsPageFallback += o.ReelsPageFallback
}

// shared is what the workers of a run share.
type shared struct {
	mu    sync.Mutex
	pages int // profiles whose page HTML was saved
	// The U5 identity, bootstrapped once and used by one worker at a time.
	otherMu sync.Mutex
	second  *session.Identity
	// The comment-hop identities (Options.CommentHop), used in turn.
	hopMu      sync.Mutex
	hops       []*session.Identity
	hopNext    int
	hopCreated int
	hopRetryAt time.Time // after a failed bootstrap, wait before another
	// Page loads by exit, for PageWallRest.
	exits map[string]*exitLoads
}

type exitLoads struct {
	ok     int // good page loads in this run
	streak int // login redirects since the last good one
}

// pageLoad records a page load on exit and reports whether the exit should
// rest: it walled and has served no good page load in this run, or walled
// twice in a row. A profile walled for everyone (not an exit) walls good
// exits whose other page loads succeed, so it rests none of them.
func (sh *shared) pageLoad(exit string, walled bool) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.exits == nil {
		sh.exits = map[string]*exitLoads{}
	}
	e := sh.exits[exit]
	if e == nil {
		e = &exitLoads{}
		sh.exits[exit] = e
	}
	if !walled {
		e.ok++
		e.streak = 0
		return false
	}
	e.streak++
	return e.ok == 0 || e.streak >= 2
}

// takePage reports whether this profile may save its page HTML (the first
// limit profiles do).
func (sh *shared) takePage(limit int) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.pages >= limit {
		return false
	}
	sh.pages++
	return true
}

// runner reads one profile at a time; a parallel run has one per worker.
type runner struct {
	o   Options
	sh  *shared
	sum Summary
	// pace holds everything but comment requests to RPS when comment
	// requests run faster (CommentRPS).
	pace *rate.Limiter
	// The profile being read, for the raw answers.
	rawDir    string
	rawSeq    int
	rawSaved  map[string]bool
	savePages bool // this profile saves its page HTML
}

func newRunner(o Options, sh *shared) *runner {
	r := &runner{o: o, sh: sh, sum: newSummary()}
	if o.RPS > 0 && o.CommentRPS > o.RPS {
		r.pace = rate.NewLimiter(rate.Limit(o.RPS), 1)
	}
	return r
}

// paceWait holds a request that is not a comment request to RPS (see pace).
func (r *runner) paceWait(ctx context.Context) {
	if r.pace != nil {
		_ = r.pace.Wait(ctx)
	}
}

// hopIdentity is the next comment-hop identity, creating and bootstrapping
// them (on page) up to CommentHop and replacing retired ones; nil when none
// is available, and the worker's own identity serves.
func (r *runner) hopIdentity(ctx context.Context, page string) *session.Identity {
	sh := r.sh
	sh.hopMu.Lock()
	defer sh.hopMu.Unlock()
	live := sh.hops[:0]
	for _, h := range sh.hops {
		if h.RetireReason() == "" {
			live = append(live, h)
		} else {
			r.o.Pool.Release(h)
		}
	}
	sh.hops = live
	if len(sh.hops) < r.o.CommentHop && time.Now().After(sh.hopRetryAt) {
		if h, err := r.o.Pool.Acquire(ctx, r.o.Tier); err == nil {
			r.paceWait(ctx)
			if _, err := r.o.Loader.Bootstrap(ctx, h, page); err != nil {
				r.o.Pool.Release(h)
				sh.hopRetryAt = time.Now().Add(2 * time.Minute)
			} else {
				sh.hops = append(sh.hops, h)
				sh.hopCreated++
			}
		}
	}
	if len(sh.hops) == 0 {
		return nil
	}
	h := sh.hops[sh.hopNext%len(sh.hops)]
	sh.hopNext++
	return h
}

func (r *runner) profile(ctx context.Context, index int, username string) Profile {
	page := strings.TrimRight(r.o.BaseURL, "/") + "/" + username + "/"
	out := Profile{Index: index, Username: username, URL: page}
	r.rawSeq, r.rawSaved, r.savePages = 0, map[string]bool{}, false
	if r.o.RawDir != "" {
		r.rawDir = filepath.Join(r.o.RawDir, fmt.Sprintf("%03d-%s", index, unsafeName.ReplaceAllString(username, "_")))
	}
	// The page load, retried on another identity when the exit is flagged
	// for page loads (the login redirect; the walled identity is retired).
	var (
		id  *session.Identity
		pl  *session.PageLoad
		err error
	)
	for try := 1; ; try++ {
		var waited time.Duration
		id, waited, err = acquire(ctx, r.o.MaxWait, waitPoll, func() (*session.Identity, error) { return r.o.Pool.Acquire(ctx, r.o.Tier) },
			func() {
				r.o.Progress(fmt.Sprintf("  %s: every exit of the tier is cooling down; waiting up to %s", username, r.o.MaxWait))
			})
		if waited > 0 {
			out.WaitedSec += int(waited.Seconds())
			r.sum.TierWaits++
		}
		if err != nil {
			out.Bootstrap = Step{Class: "no_identity", Reason: err.Error()}
			if ctx.Err() != nil {
				out.Bootstrap.Class, out.Bootstrap.Reason = stopped, "stopped by the operator"
			}
			return out
		}
		out.Identity, out.Exit = id.ID, id.Endpoint.Bucket
		r.paceWait(ctx)
		start := time.Now()
		pl, err = r.o.Loader.Load(ctx, id, page)
		out.Bootstrap = Step{Ms: time.Since(start).Milliseconds()}
		walled := err == nil && pl.Class == detect.LoginWall
		if exit := id.Endpoint.Bucket; err == nil && exit != "" {
			if walled {
				r.sum.PageWallExits[exit]++
			}
			if r.sh.pageLoad(exit, walled) && r.o.PageWallRest > 0 {
				r.o.Pool.RestExit(id, r.o.PageWallRest)
				r.sum.ExitsRested++
				r.o.Progress(fmt.Sprintf("  %s: exit %s sent a fresh visit to the login page; resting it for %s", username, exit, r.o.PageWallRest))
			}
		}
		if !walled || try >= r.o.PageLoadTries {
			break
		}
		out.PageLoadWalls++
		r.sum.PageLoadWalls++
		r.cost("page", Step{Class: pl.Class.String(), Ms: out.Bootstrap.Ms, WireBytes: pl.Bytes})
		r.o.Pool.Retire(id, "page_login_wall")
	}
	defer r.o.Pool.Release(id)
	if err != nil {
		out.Bootstrap.Class, out.Bootstrap.Reason = detect.Transport.String(), redact(err.Error())
		if ctx.Err() != nil {
			out.Bootstrap.Class, out.Bootstrap.Reason = stopped, "stopped by the operator"
		}
		return out
	}
	out.Bootstrap.Class, out.Bootstrap.Status, out.Bootstrap.WireBytes = pl.Class.String(), pl.Status, pl.Bytes
	out.Bootstrap.Reason = pl.Reason
	r.cost("page", out.Bootstrap)
	if r.rawDir != "" && pl.Class == detect.OK && r.sh.takePage(r.o.RawPages) {
		r.savePages = true
		r.saveRaw("page.html", pl.HTML)
	}
	if pl.Class == detect.OK {
		out.TokensFound = htmlx.Found(htmlx.ExtractTokens(pl.HTML))
		for k, ok := range out.TokensFound {
			if ok {
				r.sum.TokensFound[k]++
			}
		}
		// The identity's cookies after a good page load (U8), names only.
		out.Cookies = slices.Sorted(maps.Keys(id.Doer.Cookies(id.Base)))
		out.CSRFCookie = id.Doer.Cookie(id.Base, "csrftoken") != ""
		for _, c := range out.Cookies {
			r.sum.Cookies[c]++
		}
		if out.CSRFCookie {
			r.sum.CSRFCookie++
		}
	}
	// A bootstrap is this page load giving tokens. An identity reused from
	// an earlier profile still holds its tokens and goes on querying, as
	// production identities do.
	if pl.Class == detect.OK && out.TokensFound["lsd"] {
		r.sum.Bootstrapped++
	}
	if !id.Bootstrapped() {
		return out
	}

	// Profile: the user pk and the highlights list.
	resp, s := r.do(ctx, id, "profile", map[string]any{"username": username}, page)
	out.Profile = ptr(s)
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	user := first(resp.Parts, "data.xig_user_by_username")
	if !user.Exists() {
		user = findAll(resp.Parts, "xig_user_by_username")
	}
	pk := user.Get("pk").String()
	if pk == "" {
		r.keepRaw("profile", resp, 0)
		out.Profile.Reason = "no user in the response"
		return out
	}
	r.keepRaw("profile", resp, 1)
	out.Data = profileData(user)
	// The grid's _connection query takes the user object's id (17841...),
	// not its pk: with the pk it answers a null node (second probe run).
	userID := user.Get("id").String()
	if userID == "" {
		userID = pk
	}
	if len(out.Data.Highlights) == 0 {
		// The first run found no highlights on any profile: the list may
		// come in another part of the answer.
		for _, e := range findAll(resp.Parts, "lox_highlights_connection").Get("edges").Array() {
			out.Data.Highlights = append(out.Data.Highlights, Highlight{ID: e.Get("node.id").String(), Title: e.Get("node.title").String()})
		}
	}
	if r.o.ImageDir != "" && out.Data.ProfilePicURL != "" {
		out.Data.ProfilePicFile = r.saveImage(ctx, id, out.Data.ProfilePicURL, username, page)
	}
	var highlight string
	if len(out.Data.Highlights) > 0 {
		highlight = out.Data.Highlights[0].ID
	}

	// First grid page, by username.
	resp, s = r.do(ctx, id, "grid_first", map[string]any{"username": username}, page)
	out.GridFirst = ptr(s)
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	edges, pi := connection(resp.Parts, gridConn)
	out.GridFirst.Items, out.GridFirst.HasNext = len(edges), pi.HasNext
	r.sum.GridMaxPage = max(r.sum.GridMaxPage, len(edges))
	r.keepRaw("grid_first", resp, len(edges))
	cursor := pi.EndCursor
	out.Posts = append(out.Posts, r.posts(edges)...)

	// U5: the first cursor, from another identity (shared by the workers,
	// used by one at a time).
	if cursor != "" {
		r.sh.otherMu.Lock()
		if other := r.otherIdentity(ctx, page, &out); other != nil {
			resp, s := r.do(ctx, other, "grid", map[string]any{"user_id": userID, "cursor": cursor}, page)
			s.Items = gridItems(resp)
			r.keepRaw("grid_other_identity", resp, s.Items)
			out.OtherCursor = ptr(s)
			switch {
			case s.Class == stopped:
			case s.Class == detect.OK.String() && s.Items > 0:
				r.sum.CursorPortable++
			default:
				r.sum.CursorNotPortable++
			}
		}
		r.sh.otherMu.Unlock()
	}

	// U1: page the grid as far as it goes.
	out.Grid = r.walk(ctx, id, userID, cursor, &out, page)
	if ctx.Err() != nil {
		return out // stopped: the remaining steps would only fail
	}
	r.sum.PinnedPosts += markPinned(out.Posts)

	// U9: the grid by the user's id with no cursor (an absent cursor
	// renders as null; "" fails).
	resp, s = r.do(ctx, id, "grid", map[string]any{"user_id": userID}, page)
	s.Items = gridItems(resp)
	r.keepRaw("grid_by_pk", resp, s.Items)
	out.GridByPK = ptr(s)
	switch {
	case s.Class == stopped:
		return out
	case s.Class == detect.OK.String() && s.Items > 0:
		r.sum.GridByPKWorks++
	default:
		r.sum.GridByPKFails++
	}

	// Comments of the newest posts, and the first highlight.
	if len(out.Posts) > 0 {
		if !r.comments(ctx, id, &out, page) {
			return out
		}
	}
	// The newest posts' pages: time, counts and the rest the grid lacks;
	// the newest one's embedded comments stand in when the comments query
	// gave none.
	r.details(ctx, id, &out)
	// The reels tab: view, like and comment counts per reel.
	if r.o.ReelsPages > 0 {
		r.reels(ctx, id, userID, &out)
	}
	mergeCounts(&out)
	if highlight != "" {
		resp, s = r.do(ctx, id, "highlights", map[string]any{"reel_ids": []string{highlight}, "initial_reel_id": highlight}, page)
		s.Items = len(findAll(respParts(resp), "items").Array())
		r.keepRaw("highlights", resp, s.Items)
		out.Highlights = ptr(s)
		out.Data.Highlights[0].Items = s.Items
	} else {
		out.NoHighlights = true
	}
	return out
}

// comments reads up to CommentPages pages of comments (newest first) for
// each of the CommentPosts newest posts. The newest post's first page is
// out.CommentsRead. It reports false when the run was stopped.
func (r *runner) comments(ctx context.Context, id *session.Identity, out *Profile, page string) bool {
	for i, at := range newestByDate(out.Posts, r.o.CommentPosts) {
		post := out.Posts[at]
		if post.PK == "" {
			continue
		}
		cursor := ""
		for pg := range r.o.CommentPages {
			vars := map[string]any{"media_pk": post.PK}
			if cursor != "" {
				vars["cursor"] = cursor
			}
			cid := id
			if r.o.CommentHop > 1 {
				if h := r.hopIdentity(ctx, page); h != nil {
					cid = h
				}
			}
			resp, s := r.do(ctx, cid, "comments", vars, page)
			if s.Class == stopped {
				return false
			}
			edges, pi := connection(respParts(resp), "comments_connection")
			s.Items, s.HasNext = len(edges), pi.HasNext
			r.sum.CommentMaxPage = max(r.sum.CommentMaxPage, len(edges))
			r.keepRaw("comments", resp, s.Items)
			if i == 0 && pg == 0 {
				out.CommentsRead = ptr(s)
			}
			for _, e := range edges {
				n := e.Get("node")
				out.Comments = append(out.Comments, Comment{PostShortcode: post.Shortcode, PK: n.Get("pk").String(),
					Username: n.Get("user.username").String(), Verified: n.Get("user.is_verified").Bool(), Text: n.Get("text").String(),
					CreatedAt: time.Unix(n.Get("created_at").Int(), 0).UTC(), Likes: n.Get("comment_like_count").Int()})
			}
			if resp == nil || resp.Class != detect.OK || !pi.HasNext || pi.EndCursor == "" || pi.EndCursor == cursor {
				break
			}
			cursor = pi.EndCursor
		}
	}
	return true
}

func (r *runner) walk(ctx context.Context, id *session.Identity, userID, cursor string, out *Profile, page string) *Grid {
	firstPage := out.GridFirst
	g := &Grid{Posts: firstPage.Items, Stop: "end"}
	// A first page that names a cursor is followed even when it says no
	// next page, so the answer comes from what the next query returns.
	if !firstPage.HasNext && cursor == "" {
		r.sum.GridEnds++
		r.sum.GridFirstAlone++
		r.sum.MaxPosts = max(r.sum.MaxPosts, g.Posts)
		return g
	}
	for cursor != "" {
		if g.Pages >= r.o.MaxGridPages {
			g.Stop = "cap"
			break
		}
		resp, s := r.do(ctx, id, "grid", map[string]any{"user_id": userID, "cursor": cursor}, page)
		if s.Class == stopped {
			g.Stop = stopped
			break
		}
		if resp == nil || resp.Class != detect.OK {
			g.Stop, g.Failed = "wall:"+s.Class, &s
			break
		}
		edges, pi := connection(resp.Parts, gridConn)
		r.keepRaw("grid", resp, len(edges))
		if len(edges) == 0 {
			break // nothing more, whatever page_info says
		}
		g.Pages++
		g.Posts += len(edges)
		r.sum.GridMaxPage = max(r.sum.GridMaxPage, len(edges))
		out.Posts = append(out.Posts, r.posts(edges)...)
		if r.o.Progress != nil && g.Pages%progressEvery == 0 {
			line := fmt.Sprintf("  %s: %d posts so far", out.Username, g.Posts)
			if t := out.Posts[len(out.Posts)-1].PostedAt; !t.IsZero() {
				line += ", back to " + t.Format("2006-01-02")
			}
			r.o.Progress(line)
		}
		if r.o.MaxAge > 0 {
			if last := out.Posts[len(out.Posts)-1].PostedAt; !last.IsZero() && last.Before(r.o.Now().Add(-r.o.MaxAge)) {
				g.Stop = "age"
				break
			}
		}
		next := ""
		if pi.HasNext || pi.EndCursor != cursor {
			next = pi.EndCursor
		}
		if next == cursor {
			next = ""
		}
		cursor = next
	}
	g.PastPage = g.Posts > 24
	if n := len(out.Posts); n > 0 {
		g.Oldest = out.Posts[n-1].PostedAt
	}
	switch g.Stop {
	case "end":
		r.sum.GridEnds++
	case "cap":
		r.sum.GridCaps++
	case "age":
		r.sum.GridAgeStops++
	case stopped:
	default:
		r.sum.GridWalls[strings.TrimPrefix(g.Stop, "wall:")]++
		r.sum.WalledAtPosts = append(r.sum.WalledAtPosts, g.Posts)
	}
	if g.PastPage {
		r.sum.PagedPast24++
	}
	r.sum.MaxPosts = max(r.sum.MaxPosts, g.Posts)
	return g
}

// otherIdentity is a second identity for U5, bootstrapped on the first
// profile page it is needed for and reused after.
func (r *runner) otherIdentity(ctx context.Context, page string, out *Profile) *session.Identity {
	if r.sh.second != nil {
		return r.sh.second
	}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		out.OtherBootstrap = &Step{Class: "no_identity", Reason: err.Error()}
		return nil
	}
	r.paceWait(ctx)
	start := time.Now()
	pl, err := r.o.Loader.Bootstrap(ctx, id, page)
	st := &Step{Ms: time.Since(start).Milliseconds()}
	if pl != nil {
		st.Class, st.Status, st.WireBytes, st.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	}
	if err != nil {
		if st.Class == "" {
			st.Class = detect.Transport.String()
		}
		st.Reason = redact(err.Error())
		if ctx.Err() != nil {
			st.Class, st.Reason = stopped, "stopped by the operator"
		}
		out.OtherBootstrap = st
		r.o.Pool.Release(id)
		return nil
	}
	out.OtherBootstrap = st
	r.sh.second = id
	return id
}

// details reads the pages of the PostDetails newest posts (by time) into
// their Details; the first is also NewestPost, and its embedded comments
// stand in when the comments query gave none. A page that is walled stops
// the rest for this profile.
func (r *runner) details(ctx context.Context, id *session.Identity, out *Profile) {
	for i, at := range newestByDate(out.Posts, r.o.PostDetails) {
		post := &out.Posts[at]
		pp, d := r.postPage(ctx, id, post, i == 0, out)
		post.Details = d
		if i == 0 {
			out.NewestPost = pp
		}
		if pp.Class != detect.OK.String() {
			return
		}
		r.sum.PostDetails++
	}
}

// postPage loads a post's page and reads what it holds that the grid does
// not (PostDetails); with embed, its embedded first comments fill
// out.Comments when the comments query gave none.
func (r *runner) postPage(ctx context.Context, id *session.Identity, post *Post, embed bool, out *Profile) (*PostPage, *PostDetails) {
	pp := &PostPage{URL: post.URL}
	start := time.Now()
	r.paceWait(ctx)
	pl, err := r.o.Loader.Load(ctx, id, post.URL)
	pp.Ms = time.Since(start).Milliseconds()
	if err != nil {
		pp.Class, pp.Reason = detect.Transport.String(), redact(err.Error())
		if ctx.Err() != nil {
			pp.Class, pp.Reason = stopped, "stopped by the operator"
		}
		r.cost("post_page", pp.Step)
		return pp, &PostDetails{Class: pp.Class}
	}
	pp.Class, pp.Status, pp.WireBytes, pp.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	r.cost("post_page", pp.Step)
	d := &PostDetails{Class: pp.Class}
	if pl.Class != detect.OK {
		return pp, d
	}
	if r.rawDir != "" && r.savePages && embed {
		r.saveRaw("post-page.html", pl.HTML)
	}
	blobs := scrapehtml.JSONBlobs(pl.HTML)
	var media gjson.Result
	for _, b := range blobs {
		if media = mediaByCode(b, post.Shortcode); media.Exists() {
			break
		}
	}
	if media.Exists() {
		parseDetails(media, d)
		pp.TakenAt, pp.LikeCount, pp.CommentCount = d.TakenAt, d.Likes, d.Comments
	}
	edges, _ := connection(blobs, "comments_connection")
	pp.CommentsEmbedded = len(edges)
	pp.Items = len(edges)
	if embed && len(out.Comments) == 0 {
		for _, e := range edges {
			n := e.Get("node")
			out.Comments = append(out.Comments, Comment{PostShortcode: post.Shortcode, PK: n.Get("pk").String(),
				Username: n.Get("user.username").String(), Verified: n.Get("user.is_verified").Bool(), Text: n.Get("text").String(),
				CreatedAt: time.Unix(n.Get("created_at").Int(), 0).UTC(), Likes: n.Get("comment_like_count").Int()})
		}
	}
	return pp, d
}

// reels walks the profile's reels tab by the user's id: the first page
// without a cursor, or, when that fails, from the reels page's HTML (which
// embeds it), then by cursor up to ReelsPages pages.
func (r *runner) reels(ctx context.Context, id *session.Identity, userID string, out *Profile) {
	const conn = "polaris_clips_connection"
	cursor, stop := "", "end"
	for pg := 0; pg < r.o.ReelsPages; pg++ {
		vars := map[string]any{"user_id": userID}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, s := r.do(ctx, id, "reels", vars, out.URL+"reels/")
		if s.Class == stopped {
			stop = stopped
			break
		}
		var edges []gjson.Result
		var pi walk.PageInfo
		if resp != nil && resp.Class == detect.OK {
			edges, pi = connection(resp.Parts, conn)
		}
		s.Items, s.HasNext = len(edges), pi.HasNext
		r.keepRaw("reels", resp, s.Items)
		if pg == 0 {
			out.ReelsFirst = ptr(s)
			if len(edges) > 0 {
				r.sum.ReelsByIDWorks++
			} else {
				// The reels page embeds the first page and its cursor.
				r.paceWait(ctx)
				pl, err := r.o.Loader.Load(ctx, id, out.URL+"reels/")
				if err != nil || pl.Class != detect.OK {
					stop = "wall:" + s.Class
					break
				}
				r.cost("reels_page", Step{Class: pl.Class.String(), WireBytes: pl.Bytes})
				edges, pi = connection(scrapehtml.JSONBlobs(pl.HTML), conn)
				if len(edges) > 0 {
					r.sum.ReelsPageFallback++
				}
			}
		} else if resp == nil || resp.Class != detect.OK {
			stop = "wall:" + s.Class
			break
		}
		if len(edges) == 0 {
			break
		}
		r.sum.ReelsMaxPage = max(r.sum.ReelsMaxPage, len(edges))
		for _, e := range edges {
			out.Reels = append(out.Reels, reelOf(e.Get("node"), r.o.BaseURL))
		}
		// The backfill window (MaxAge) stops the reels as it stops the grid
		// (a pinned reel may be older; the last of the page decides).
		if last := out.Reels[len(out.Reels)-1].PostedAt; r.o.MaxAge > 0 && !last.IsZero() && last.Before(r.o.Now().Add(-r.o.MaxAge)) {
			stop = "age"
			break
		}
		if !pi.HasNext || pi.EndCursor == "" || pi.EndCursor == cursor {
			break
		}
		cursor = pi.EndCursor
		if pg == r.o.ReelsPages-1 {
			stop = "cap"
		}
	}
	out.ReelsStop = stop
	r.sum.Reels += len(out.Reels)
}

func reelOf(n gjson.Result, base string) Reel {
	count := func(path string) *int64 {
		if v := n.Get(path); v.Exists() && v.Type == gjson.Number {
			x := v.Int()
			return &x
		}
		return nil
	}
	code := n.Get("code").String()
	return Reel{PK: n.Get("pk").String(), Shortcode: code, URL: strings.TrimRight(base, "/") + "/reel/" + code + "/",
		PostedAt: PostedAt(n.Get("pk").String()), Plays: count("play_count"), Likes: count("like_count"), Comments: count("comment_count"),
		CountsHidden: n.Get("like_and_view_counts_disabled").Bool(), Pinned: len(n.Get("clips_tab_pinned_user_ids").Array()) > 0,
		ThumbnailURL: n.Get("display_uri").String()}
}

// mergeCounts gives each post its counts from the best source read: its
// page, else the reels tab (which alone has views).
func mergeCounts(out *Profile) {
	reels := map[string]*Reel{}
	for i := range out.Reels {
		reels[out.Reels[i].Shortcode] = &out.Reels[i]
	}
	for i := range out.Posts {
		p := &out.Posts[i]
		if rl := reels[p.Shortcode]; rl != nil {
			p.Plays, p.Likes, p.Comments = rl.Plays, rl.Likes, rl.Comments
		}
		if d := p.Details; d != nil && d.Likes != nil {
			p.Likes, p.Comments = d.Likes, d.Comments
		}
	}
}

// mediaByCode finds the post's own media object in a page's data: the one
// with its shortcode and a taken_at (a post page also carries other posts
// of the profile, without counts).
func mediaByCode(v gjson.Result, code string) gjson.Result {
	switch {
	case v.IsObject():
		if v.Get("code").String() == code && v.Get("taken_at").Exists() {
			return v
		}
		var hit gjson.Result
		v.ForEach(func(_, x gjson.Result) bool {
			if x.IsObject() || x.IsArray() {
				hit = mediaByCode(x, code)
			}
			return !hit.Exists()
		})
		return hit
	case v.IsArray():
		var hit gjson.Result
		v.ForEach(func(_, x gjson.Result) bool {
			hit = mediaByCode(x, code)
			return !hit.Exists()
		})
		return hit
	}
	return gjson.Result{}
}

var dashDuration = regexp.MustCompile(`mediaPresentationDuration="PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?"`)

// parseDetails reads a post page's media object (field names from the
// owner's full run).
func parseDetails(m gjson.Result, d *PostDetails) {
	if t := m.Get("taken_at").Int(); t > 0 {
		d.TakenAt = time.Unix(t, 0).UTC()
	}
	if v := m.Get("like_count"); v.Exists() && v.Type == gjson.Number {
		n := v.Int()
		d.Likes = &n
	}
	if v := m.Get("comment_count"); v.Exists() && v.Type == gjson.Number {
		n := v.Int()
		d.Comments = &n
	}
	d.LikesHidden = m.Get("like_and_view_counts_disabled").Bool()
	d.CaptionEdited = m.Get("caption_is_edited").Bool()
	for _, t := range m.Get("usertags.in").Array() {
		name := t.Get("user.username").String()
		if name == "" {
			name = t.Get("user.full_name").String()
		}
		if name != "" {
			d.Tagged = append(d.Tagged, name)
		}
	}
	for _, c := range m.Get("coauthor_producers").Array() {
		if u := c.Get("username").String(); u != "" {
			d.Coauthors = append(d.Coauthors, u)
		}
	}
	d.Location = m.Get("location.name").String()
	if mi := m.Get("clips_metadata.music_info"); mi.IsObject() {
		title := firstString(mi, "music_asset_info.title", "music_info.title", "title")
		artist := firstString(mi, "music_asset_info.display_artist", "music_info.display_artist", "display_artist")
		d.Music = strings.TrimSpace(strings.Trim(title+" - "+artist, " -"))
	}
	if d.Music == "" {
		if snd := m.Get("clips_metadata.original_sound_info"); snd.IsObject() {
			d.Music = strings.TrimSpace(snd.Get("original_audio_title").String() + " - " + snd.Get("ig_artist.username").String())
			d.Music = strings.Trim(d.Music, " -")
		}
	}
	for _, t := range m.Get("related_topic_pills").Array() {
		if n := t.Get("topic_name").String(); n != "" {
			d.Topics = append(d.Topics, n)
		}
	}
	if v := m.Get("video_duration"); v.Exists() {
		d.DurationS = v.Float()
	} else if g := dashDuration.FindStringSubmatch(m.Get("video_dash_manifest").String()); g != nil {
		h, _ := strconv.ParseFloat(g[1], 64)
		mi, _ := strconv.ParseFloat(g[2], 64)
		sec, _ := strconv.ParseFloat(g[3], 64)
		d.DurationS = math.Round((h*3600+mi*60+sec)*100) / 100
	}
	d.Width, d.Height = int(m.Get("original_width").Int()), int(m.Get("original_height").Int())
	d.HasAudio = m.Get("has_audio").Bool()
	d.AILabel = m.Get("ai_label_info.attribution_label_text").String()
	d.CarouselCount = int(m.Get("carousel_media_count").Int())
	if d.CarouselCount == 0 {
		d.CarouselCount = len(m.Get("carousel_media").Array())
	}
	d.VideoURL = m.Get("video_versions.0.url").String()
	d.ImageURL = firstString(m, "display_uri", "image_versions2.candidates.0.url")
}

func firstString(v gjson.Result, paths ...string) string {
	for _, p := range paths {
		if s := v.Get(p).String(); s != "" {
			return s
		}
	}
	return ""
}

// markPinned marks posts among the first three that are older than a
// later post (the grid shows pinned posts first) and returns how many.
func markPinned(posts []Post) int {
	n := 0
	for i := range min(3, len(posts)) {
		for _, later := range posts[i+1:] {
			if !posts[i].PostedAt.IsZero() && later.PostedAt.After(posts[i].PostedAt) {
				posts[i].Pinned = true
				n++
				break
			}
		}
	}
	return n
}

// newestByDate is the indexes of the n newest posts, by posting time (grid
// order when a time is missing or equal).
func newestByDate(posts []Post, n int) []int {
	idx := make([]int, len(posts))
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int { return posts[b].PostedAt.Compare(posts[a].PostedAt) })
	return idx[:min(n, len(idx))]
}

func (r *runner) do(ctx context.Context, id *session.Identity, query string, vars map[string]any, referer string) (*graphql.Response, Step) {
	switch {
	case (query == "grid" || query == "grid_first") && r.o.GridPageSize > 0:
		vars["first"] = r.o.GridPageSize
	case query == "comments" && r.o.CommentPageSize > 0:
		vars["first"] = r.o.CommentPageSize
	case query == "reels" && r.o.ReelsPageSize > 0:
		vars["first"] = r.o.ReelsPageSize
	}
	if query != "comments" {
		r.paceWait(ctx)
	}
	start := time.Now()
	resp, err := r.o.GQL.Do(ctx, id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	s := Step{Ms: time.Since(start).Milliseconds()}
	if query == "comments" {
		defer func() {
			r.sum.CommentRequests++
			r.sum.CommentAnswers[s.Class]++
			r.sum.CommentMs += s.Ms
		}()
	}
	if err != nil {
		if ctx.Err() != nil {
			// The operator stopped the run: not Instagram's answer, so it
			// counts for no question and no cost.
			s.Class, s.Reason = stopped, "stopped by the operator"
			return nil, s
		}
		s.Class, s.Reason = detect.Transport.String(), "request failed"
		r.cost(query, s)
		return nil, s
	}
	s.Class, s.Status, s.WireBytes, s.Reason = resp.Class.String(), resp.Status, resp.BytesIn+resp.BytesOut, resp.Reason
	for _, e := range resp.Errors {
		if m := e.Message; m != "" && !slices.Contains(s.Errors, m) && len(s.Errors) < 5 {
			s.Errors = append(s.Errors, m)
		}
	}
	r.sum.QueriesWithoutClientState[s.Class]++
	if resp.Class != detect.OK && r.rawDir != "" {
		r.rawSeq++
		r.saveRaw(fmt.Sprintf("%02d-%s-%s.json", r.rawSeq, query, resp.Class), resp.Body)
	}
	r.cost(query, s)
	return resp, s
}

func (r *runner) cost(kind string, s Step) {
	c := r.sum.Requests[kind]
	c.N++
	c.sumMs += s.Ms
	c.sumBytes += s.WireBytes
	r.sum.Requests[kind] = c
}

// ptr copies a step: each report field keeps its own, though the step
// variable is reused for the next request.
func ptr(s Step) *Step { return &s }

func first(parts []gjson.Result, path string) gjson.Result {
	for _, p := range parts {
		if v := p.Get(path); v.Exists() {
			return v
		}
	}
	return gjson.Result{}
}

func respParts(resp *graphql.Response) []gjson.Result {
	if resp == nil || resp.Class != detect.OK {
		return nil
	}
	return resp.Parts
}

func gridItems(resp *graphql.Response) int {
	edges, _ := connection(respParts(resp), gridConn)
	return len(edges)
}

// gridConn names the profile grid's connection in both grid queries.
const gridConn = "polaris_ordered_timeline_connection"

// connection reads the connection named key from every part of a response:
// its edges wherever they stand (inside the connection object, or in parts
// Relay streams separately, whose path ends in the connection's edges), and
// its page_info, including a deferred page_info part (walk.FindPageInfo).
// Edges repeated across parts count once.
func connection(parts []gjson.Result, key string) ([]gjson.Result, walk.PageInfo) {
	var edges []gjson.Result
	seen := map[string]bool{}
	add := func(e gjson.Result) {
		if !e.Get("node").Exists() {
			return
		}
		k := e.Get("node.pk").String() + "|" + e.Get("node.id").String()
		if k != "|" && seen[k] {
			return
		}
		seen[k] = true
		edges = append(edges, e)
	}
	for _, p := range parts {
		for _, c := range walk.FindAll(p, key) {
			for _, e := range c.Get("edges").Array() {
				add(e)
			}
		}
		if path := p.Get("path"); path.IsArray() && strings.Contains(path.Raw, `"`+key+`"`) && strings.Contains(path.Raw, `"edges"`) {
			d := p.Get("data")
			if d.IsArray() {
				for _, e := range d.Array() {
					add(e)
				}
			} else {
				add(d)
			}
		}
	}
	return edges, walk.FindPageInfo(parts, []string{key})
}

// findAll returns the first value stored under key in any part.
func findAll(parts []gjson.Result, key string) gjson.Result {
	for _, p := range parts {
		if v := walk.Find(p, key); v.Exists() {
			return v
		}
	}
	return gjson.Result{}
}

// keepRaw saves an OK response's body when it is the first of its kind for
// this profile, or held no items. do saves every response that was not OK.
func (r *runner) keepRaw(kind string, resp *graphql.Response, items int) {
	if r.rawDir == "" || resp == nil || resp.Class != detect.OK {
		return
	}
	if r.rawSaved[kind] && items > 0 {
		return
	}
	r.rawSaved[kind] = true
	r.rawSeq++
	r.saveRaw(fmt.Sprintf("%02d-%s-%s.json", r.rawSeq, kind, resp.Class), resp.Body)
}

func (r *runner) saveRaw(name string, body []byte) {
	if err := os.MkdirAll(r.rawDir, 0o700); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(r.rawDir, name), body, 0o600)
}

func gridLine(g *Grid) string {
	if g == nil {
		return "not read"
	}
	return fmt.Sprintf("%d posts, stop %s", g.Posts, g.Stop)
}

// profileData reads the profile query's user object. Field names are the
// findings' (har-findings §3); the linked Facebook page's shape was not
// recorded, so two candidate paths are tried.
func profileData(u gjson.Result) *ProfileData {
	d := &ProfileData{PK: u.Get("pk").String(), FullName: u.Get("full_name").String(), Biography: u.Get("biography").String(),
		Verified: u.Get("is_verified").Bool(), Private: u.Get("is_private").Bool(), Memorialized: u.Get("is_memorialized").Bool(),
		Followers: u.Get("follower_count").Int(), Following: u.Get("following_count").Int(), HasClips: u.Get("has_any_clips").Bool()}
	for _, p := range []string{"hd_profile_pic_url_info.url", "profile_pic_url_hd", "profile_pic_url"} {
		if v := u.Get(p).String(); v != "" {
			d.ProfilePicURL = v
			break
		}
	}
	for _, p := range []string{"linked_fb_info.linked_fb_page.name", "linked_fb_info.name"} {
		if v := u.Get(p).String(); v != "" {
			d.LinkedFacebookPage = v
			break
		}
	}
	for _, l := range u.Get("bio_links").Array() {
		if url := l.Get("url").String(); url != "" {
			d.BioLinks = append(d.BioLinks, Link{Title: l.Get("title").String(), URL: url})
		}
	}
	for _, e := range u.Get("lox_highlights_connection.edges").Array() {
		d.Highlights = append(d.Highlights, Highlight{ID: e.Get("node.id").String(), Title: e.Get("node.title").String()})
	}
	return d
}

var mediaTypes = map[int64]string{1: "photo", 2: "video", 8: "carousel"}

// posts reads grid edges.
func (r *runner) posts(edges []gjson.Result) []Post {
	base := strings.TrimRight(r.o.BaseURL, "/")
	var out []Post
	for _, e := range edges {
		n := e.Get("node")
		p := Post{PK: n.Get("pk").String(), Shortcode: n.Get("code").String(), ProductType: n.Get("product_type").String(),
			Caption: n.Get("caption.text").String(), AltText: n.Get("accessibility_caption").String()}
		p.MediaType = mediaTypes[n.Get("media_type").Int()]
		p.PostedAt = PostedAt(p.PK)
		if p.MediaType == "" {
			p.MediaType = n.Get("media_type").String()
		}
		for _, k := range []string{"display_uri", "image_versions2.candidates.0.url", "thumbnail_src"} {
			if v := n.Get(k).String(); v != "" {
				p.ThumbnailURL = v
				break
			}
		}
		kind := "p"
		if p.ProductType == "clips" {
			kind = "reel"
		}
		p.URL = base + "/" + kind + "/" + p.Shortcode + "/"
		out = append(out, p)
	}
	return out
}

// idEpochMs is the epoch of Instagram's ids: a media pk holds the
// milliseconds since it above its 23rd bit. Checked in the owner's probe run
// (2026-10-31): within about a minute of each post page's taken_at.
const idEpochMs = 1314220021721

// PostedAt is when a media pk says its post was made; zero for a pk that
// does not parse or encodes no plausible time.
func PostedAt(pk string) time.Time {
	v, err := strconv.ParseInt(pk, 10, 64)
	if err != nil || v <= 0 {
		return time.Time{}
	}
	t := time.UnixMilli(v>>23 + idEpochMs).UTC()
	// Ids that encode a time began in late 2011; a small number decodes to
	// the epoch itself.
	if t.Year() < 2012 || t.After(time.Now().Add(48*time.Hour)) {
		return time.Time{}
	}
	return t
}

var credentials = regexp.MustCompile(`://[^/@\s]+@`)

// redact removes credentials from URLs in an error text (a proxy URL can
// carry them).
func redact(s string) string { return credentials.ReplaceAllString(s, "://***@") }

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// saveImage downloads a profile picture through the identity (its exit and
// fingerprint) and returns the file name under ImageDir, or "" on failure.
func (r *runner) saveImage(ctx context.Context, id *session.Identity, url, username, referer string) string {
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: url, Header: [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"user-agent", id.FP.UserAgent},
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform}, {"accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"},
		{"sec-fetch-site", "cross-site"}, {"sec-fetch-mode", "no-cors"}, {"sec-fetch-dest", "image"},
		{"referer", referer}, {"accept-encoding", "gzip, deflate, br, zstd"}, {"priority", "i"},
	}})
	if err != nil || resp.Status != 200 || len(resp.Body) == 0 {
		return ""
	}
	r.cost("profile_picture", Step{WireBytes: resp.BytesIn + resp.BytesOut, Ms: resp.Elapsed.Milliseconds()})
	name := unsafeName.ReplaceAllString(username, "_") + ".jpg"
	if err := os.MkdirAll(r.o.ImageDir, 0o700); err != nil {
		return ""
	}
	if err := os.WriteFile(filepath.Join(r.o.ImageDir, name), resp.Body, 0o600); err != nil {
		return ""
	}
	return filepath.Join(filepath.Base(r.o.ImageDir), name)
}
