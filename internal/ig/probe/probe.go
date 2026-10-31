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
// latency and wire bytes. The report holds no usernames, ids, cursors or
// text: a profile is its line number in the owner's list, a cookie or token
// is its name.
package probe

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Options configure a run.
type Options struct {
	BaseURL  string   // https://www.instagram.com
	Profiles []string // usernames, in the owner's file order
	Tier     string   // proxy tier ("direct" for none)
	// MaxGridPages caps the U1 walk after the first grid page (12 posts each).
	MaxGridPages int
	Pool         *session.Pool
	Loader       *session.Loader
	GQL          *graphql.Client
	// Fingerprint names the profile the pool uses, for the report.
	Fingerprint string
	Version     string
	// Progress, when set, gets one line per finished profile (no names).
	Progress func(string)
	Now      func() time.Time
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
}

// Grid is the U1 walk of one profile.
type Grid struct {
	Pages int `json:"pages"` // after the first
	Posts int `json:"posts"` // including the first page
	// Stop is "end" (has_next_page false), "cap" (MaxGridPages), or
	// "wall:<class>" for the first page that did not answer.
	Stop     string `json:"stop"`
	PastPage bool   `json:"past_24"` // read more than 24 posts
	Failed   *Step  `json:"failed,omitempty"`
}

// Profile is one profile's results; Index is its 1-based line in the list.
type Profile struct {
	Index        int             `json:"index"`
	Bootstrap    Step            `json:"bootstrap"`
	TokensFound  map[string]bool `json:"tokens_found,omitempty"`
	Cookies      []string        `json:"cookies,omitempty"`
	CSRFCookie   bool            `json:"csrf_cookie,omitempty"`
	Profile      *Step           `json:"profile,omitempty"`
	GridFirst    *Step           `json:"grid_first,omitempty"`
	Grid         *Grid           `json:"grid,omitempty"`
	OtherCursor  *Step           `json:"cursor_other_identity,omitempty"`
	GridByPK     *Step           `json:"grid_by_pk_no_cursor,omitempty"`
	Comments     *Step           `json:"comments,omitempty"`
	Highlights   *Step           `json:"highlights,omitempty"`
	NoHighlights bool            `json:"no_highlights,omitempty"`
}

// Report is the whole run.
type Report struct {
	Version     string    `json:"version"`
	Tier        string    `json:"tier"`
	Fingerprint string    `json:"fingerprint"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Profiles    []Profile `json:"profiles"`
	Summary     Summary   `json:"summary"`
}

// Summary answers the questions over all profiles.
type Summary struct {
	Profiles     int            `json:"profiles"`
	Bootstrapped int            `json:"bootstrapped"`
	TokensFound  map[string]int `json:"tokens_found"`
	// U1
	GridEnds       int            `json:"grid_ends"`
	GridCaps       int            `json:"grid_caps"`
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
}

// Cost aggregates one request kind.
type Cost struct {
	N         int   `json:"n"`
	MeanMs    int64 `json:"mean_ms"`
	MeanBytes int64 `json:"mean_wire_bytes"`
	sumMs     int64
	sumBytes  int64
}

// Run probes every profile and returns the report.
func Run(ctx context.Context, o Options) Report {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxGridPages <= 0 {
		o.MaxGridPages = 32
	}
	r := &runner{o: o, sum: Summary{TokensFound: map[string]int{}, GridWalls: map[string]int{},
		QueriesWithoutClientState: map[string]int{}, Cookies: map[string]int{}, Requests: map[string]Cost{}}}
	rep := Report{Version: o.Version, Tier: o.Tier, Fingerprint: o.Fingerprint, StartedAt: o.Now().UTC()}
	for i, username := range o.Profiles {
		if ctx.Err() != nil {
			break
		}
		p := r.profile(ctx, i+1, username)
		rep.Profiles = append(rep.Profiles, p)
		if o.Progress != nil {
			o.Progress(fmt.Sprintf("profile %d/%d: bootstrap %s, grid %s", i+1, len(o.Profiles), p.Bootstrap.Class, gridLine(p.Grid)))
		}
	}
	if r.second != nil {
		o.Pool.Release(r.second)
	}
	for k, c := range r.sum.Requests {
		c.MeanMs, c.MeanBytes = c.sumMs/int64(c.N), c.sumBytes/int64(c.N)
		r.sum.Requests[k] = c
	}
	r.sum.Profiles = len(rep.Profiles)
	slices.Sort(r.sum.WalledAtPosts)
	rep.Summary = r.sum
	rep.FinishedAt = o.Now().UTC()
	return rep
}

type runner struct {
	o      Options
	sum    Summary
	second *session.Identity // the U5 identity, bootstrapped once
}

func (r *runner) profile(ctx context.Context, index int, username string) Profile {
	out := Profile{Index: index}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		out.Bootstrap = Step{Class: "no_identity", Reason: err.Error()}
		return out
	}
	defer r.o.Pool.Release(id)
	page := strings.TrimRight(r.o.BaseURL, "/") + "/" + username + "/"
	start := time.Now()
	pl, err := r.o.Loader.Load(ctx, id, page)
	out.Bootstrap = Step{Ms: time.Since(start).Milliseconds()}
	if err != nil {
		out.Bootstrap.Class, out.Bootstrap.Reason = detect.Transport.String(), "page load failed"
		return out
	}
	out.Bootstrap.Class, out.Bootstrap.Status, out.Bootstrap.WireBytes = pl.Class.String(), pl.Status, pl.Bytes
	out.Bootstrap.Reason = r.reason(pl.Reason, username)
	r.cost("page", out.Bootstrap)
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
	resp, s := r.do(ctx, id, "profile", map[string]any{"username": username}, username, page)
	out.Profile = &s
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	user := first(resp.Parts, "data.xig_user_by_username")
	pk := user.Get("pk").String()
	if pk == "" {
		out.Profile.Reason = "no user in the response"
		return out
	}
	var highlight string
	if h := user.Get("lox_highlights_connection.edges.0.node.id"); h.Exists() {
		highlight = h.String()
	}

	// First grid page, by username.
	resp, s = r.do(ctx, id, "grid_first", map[string]any{"username": username}, username, page)
	out.GridFirst = &s
	if resp == nil || resp.Class != detect.OK {
		return out
	}
	conn := first(resp.Parts, "data.xig_user_by_username.polaris_ordered_timeline_connection")
	edges := conn.Get("edges").Array()
	out.GridFirst.Items, out.GridFirst.HasNext = len(edges), conn.Get("page_info.has_next_page").Bool()
	cursor := conn.Get("page_info.end_cursor").String()
	var mediaPK string
	if len(edges) > 0 {
		mediaPK = edges[0].Get("node.pk").String()
	}

	// U5: the first cursor, from another identity.
	if cursor != "" {
		if other := r.otherIdentity(ctx, page); other != nil {
			resp, s := r.do(ctx, other, "grid", map[string]any{"user_pk": pk, "cursor": cursor}, username, page)
			s.Items = gridItems(resp)
			out.OtherCursor = &s
			if s.Class == detect.OK.String() && s.Items > 0 {
				r.sum.CursorPortable++
			} else {
				r.sum.CursorNotPortable++
			}
		}
	}

	// U1: page the grid as far as it goes.
	out.Grid = r.walk(ctx, id, pk, cursor, out.GridFirst, username, page)

	// U9: the grid by pk with no cursor.
	resp, s = r.do(ctx, id, "grid", map[string]any{"user_pk": pk, "cursor": ""}, username, page)
	s.Items = gridItems(resp)
	out.GridByPK = &s
	if s.Class == detect.OK.String() && s.Items > 0 {
		r.sum.GridByPKWorks++
	} else {
		r.sum.GridByPKFails++
	}

	// Comments of the newest post, and the first highlight.
	if mediaPK != "" {
		resp, s = r.do(ctx, id, "comments", map[string]any{"media_pk": mediaPK, "cursor": ""}, username, page)
		cc := first(respParts(resp), "data.xig_polaris_media.comments_connection")
		s.Items, s.HasNext = len(cc.Get("edges").Array()), cc.Get("page_info.has_next_page").Bool()
		out.Comments = &s
	}
	if highlight != "" {
		resp, s = r.do(ctx, id, "highlights", map[string]any{"reel_ids": []string{highlight}, "initial_reel_id": highlight}, username, page)
		s.Items = len(first(respParts(resp), "data.xdt_api__v1__feed__reels_media__connection.edges.0.node.items").Array())
		out.Highlights = &s
	} else {
		out.NoHighlights = true
	}
	return out
}

func (r *runner) walk(ctx context.Context, id *session.Identity, pk, cursor string, firstPage *Step, username, page string) *Grid {
	g := &Grid{Posts: firstPage.Items, Stop: "end"}
	if !firstPage.HasNext {
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
		resp, s := r.do(ctx, id, "grid", map[string]any{"user_pk": pk, "cursor": cursor}, username, page)
		if resp == nil || resp.Class != detect.OK {
			g.Stop, g.Failed = "wall:"+s.Class, &s
			break
		}
		g.Pages++
		conn := first(resp.Parts, "data.node.polaris_ordered_timeline_connection")
		g.Posts += len(conn.Get("edges").Array())
		cursor = ""
		if conn.Get("page_info.has_next_page").Bool() {
			cursor = conn.Get("page_info.end_cursor").String()
		}
	}
	g.PastPage = g.Posts > 24
	switch {
	case g.Stop == "end":
		r.sum.GridEnds++
	case g.Stop == "cap":
		r.sum.GridCaps++
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
func (r *runner) otherIdentity(ctx context.Context, page string) *session.Identity {
	if r.second != nil {
		return r.second
	}
	id, err := r.o.Pool.Acquire(ctx, r.o.Tier)
	if err != nil {
		return nil
	}
	if _, err := r.o.Loader.Bootstrap(ctx, id, page); err != nil {
		r.o.Pool.Release(id)
		return nil
	}
	r.second = id
	return id
}

func (r *runner) do(ctx context.Context, id *session.Identity, query string, vars map[string]any, username, referer string) (*graphql.Response, Step) {
	start := time.Now()
	resp, err := r.o.GQL.Do(ctx, id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	s := Step{Ms: time.Since(start).Milliseconds()}
	if err != nil {
		s.Class, s.Reason = detect.Transport.String(), "request failed"
		r.cost(query, s)
		return nil, s
	}
	s.Class, s.Status, s.WireBytes, s.Reason = resp.Class.String(), resp.Status, resp.BytesIn+resp.BytesOut, r.reason(resp.Reason, username)
	r.sum.QueriesWithoutClientState[s.Class]++
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

// reason keeps a classifier reason out of the report if it names the
// profile (a redirect path can carry the username).
func (r *runner) reason(reason, username string) string {
	if username != "" && strings.Contains(strings.ToLower(reason), strings.ToLower(username)) {
		return "(withheld: names the profile)"
	}
	return reason
}

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
	return len(first(respParts(resp), "data.node.polaris_ordered_timeline_connection.edges").Array())
}

func gridLine(g *Grid) string {
	if g == nil {
		return "not read"
	}
	return fmt.Sprintf("%d posts, stop %s", g.Posts, g.Stop)
}
