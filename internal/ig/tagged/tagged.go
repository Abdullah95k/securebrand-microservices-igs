// Package tagged tests a profile's tagged tab logged out (igs-spike
// tagged): whether Instagram serves the tab's page to a visitor who is not
// logged in, whether the page carries tagged posts, and which GraphQL
// operation pages through them. No recording has shown the tab yet, so the
// operation is looked for in the page's script bundles (names with
// "Tagged" or "Usertag") and tried with the variable shapes other
// logged-out queries use. What it learns goes in the query seed; the raw
// answers it saves feed the parser's fixtures (igs-spike fixtures).
package tagged

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/docids"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Options configure a run.
type Options struct {
	BaseURL  string
	Profiles []string
	Tier     string
	Pool     *session.Pool
	Loader   *session.Loader
	// GQL is the probe's client; the run copies it with the seed plus the
	// operations it tries.
	GQL  *graphql.Client
	Seed registry.Seed
	// Scanner finds the operations in the pages' bundles.
	Scanner *docids.Scanner
	// MaxOps caps the operations tried per profile (default 3).
	MaxOps int
	// MaxWait is how long a profile waits for an identity while every
	// exit of the tier rests (default 10 minutes).
	MaxWait time.Duration
	// RawDir, when set, keeps the tagged pages' HTML and every answer.
	RawDir   string
	Progress func(string)
}

// Shape is one way of passing the user and the page size.
type Shape struct {
	Name string `json:"name"`
	// User is which of the user's numbers goes in: "pk" or "id".
	User string `json:"user"`
	// UserKey and SizeKey are the variable names.
	UserKey string `json:"user_key"`
	SizeKey string `json:"size_key"`
}

// Shapes are tried in order until one answers with posts; the first that
// works is tried first for the next profile. The v1 usertags feed takes
// the pk and a count; the logged-out connection queries take the user
// object's id and "first".
var Shapes = []Shape{
	{Name: "user_id_pk_count", User: "pk", UserKey: "user_id", SizeKey: "count"},
	{Name: "id_first", User: "id", UserKey: "id", SizeKey: "first"},
	{Name: "user_id_id_count", User: "id", UserKey: "user_id", SizeKey: "count"},
	{Name: "id_pk_first", User: "pk", UserKey: "id", SizeKey: "first"},
}

// PageSize is the posts asked for per page: the grid's 12.
const PageSize = 12

// Step is one request's answer.
type Step struct {
	Class  string   `json:"class"`
	Status int      `json:"status,omitempty"`
	Bytes  int64    `json:"bytes,omitempty"`
	Ms     int64    `json:"ms"`
	Reason string   `json:"reason,omitempty"`
	Errors []string `json:"errors,omitempty"`
}

// Try is one operation tried with one shape.
type Try struct {
	Operation string `json:"operation"`
	DocID     string `json:"doc_id"`
	Shape     string `json:"shape"`
	Step
	// Posts is the largest list of posts the answer held.
	Posts int `json:"posts"`
	// HasNext is whether the answer offered another page.
	HasNext bool `json:"has_next"`
}

// Result is one profile's test. It names no account: the index is the
// line in the profiles file.
type Result struct {
	Index int    `json:"index"`
	Exit  string `json:"exit,omitempty"`
	// Profile is the profile page's load, which gives the identity its
	// tokens; ProfileQuery gives the user's pk and id.
	Profile      Step `json:"profile_page"`
	ProfileQuery Step `json:"profile_query"`
	// Tagged is the tagged tab's page load. LoginPage is set when it
	// sent the visitor to the login page.
	Tagged    Step `json:"tagged_page"`
	LoginPage bool `json:"login_page"`
	// Embedded is the most tagged posts the tab's page carried, and
	// Markers the tagged-looking keys its data held.
	Embedded int      `json:"embedded_posts"`
	Markers  []string `json:"markers,omitempty"`
	Tries    []Try    `json:"tries,omitempty"`
}

// Report is a run's results.
type Report struct {
	StartedAt time.Time `json:"started_at"`
	Results   []Result  `json:"results"`
	// Operations are the tagged-looking operations the bundles declared.
	Operations map[string]string `json:"operations"`
	Summary    Summary           `json:"summary"`
}

// Summary is what to send back: no account appears in it.
type Summary struct {
	Profiles      int `json:"profiles"`
	ProfilePageOK int `json:"profile_page_ok"`
	TaggedPageOK  int `json:"tagged_page_ok"`
	// TaggedLogin counts tagged pages that sent the visitor to log in.
	TaggedLogin    int `json:"tagged_login_page"`
	TaggedOther    int `json:"tagged_other"`
	EmbeddedPosts  int `json:"profiles_with_embedded_posts"`
	OperationsSeen int `json:"operations_seen"`
	// Worked counts, per "operation shape", the tries that answered with
	// posts; Answers counts every try's class per operation.
	Worked  map[string]int            `json:"worked"`
	Answers map[string]map[string]int `json:"answers"`
}

// Verdict is the summary in one line.
func (s Summary) Verdict() string {
	switch {
	case s.Profiles == 0:
		return "no profiles were read"
	case len(s.Worked) > 0:
		return "the tagged tab is readable logged out: an operation answered with posts"
	case s.EmbeddedPosts > 0:
		return "the tagged tab's page carries posts logged out, but no operation paged on; send raw/ for a recording-based fix"
	case s.TaggedPageOK == 0 && s.TaggedLogin > 0:
		return "the tagged tab needs a login: Instagram sent every visit to the login page"
	default:
		return "the tagged tab gave no posts logged out"
	}
}

var tagName = regexp.MustCompile(`(?i)tagged|usertag`)

// Run tests each profile in turn, one identity at a time.
func Run(ctx context.Context, o Options) Report {
	if o.MaxOps <= 0 {
		o.MaxOps = 3
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	rep := Report{StartedAt: time.Now().UTC(), Operations: map[string]string{},
		Summary: Summary{Worked: map[string]int{}, Answers: map[string]map[string]int{}}}
	t := &tester{o: o, rep: &rep, gql: map[string]*graphql.Client{}}
	for i, u := range o.Profiles {
		if ctx.Err() != nil {
			break
		}
		r := t.profile(ctx, i+1, u)
		rep.Results = append(rep.Results, r)
		rep.Summary.add(r)
		o.Progress(fmt.Sprintf("profile %d of %d: page %s, tagged page %s, %d embedded posts, %d tries", i+1, len(o.Profiles),
			r.Profile.Class, r.Tagged.Class, r.Embedded, len(r.Tries)))
	}
	rep.Summary.OperationsSeen = len(rep.Operations)
	return rep
}

func (s *Summary) add(r Result) {
	s.Profiles++
	if r.Profile.Class == detect.OK.String() {
		s.ProfilePageOK++
	}
	switch {
	case r.Tagged.Class == "":
	case r.LoginPage:
		s.TaggedLogin++
	case r.Tagged.Class == detect.OK.String():
		s.TaggedPageOK++
	default:
		s.TaggedOther++
	}
	if r.Embedded > 0 {
		s.EmbeddedPosts++
	}
	for _, t := range r.Tries {
		if s.Answers[t.Operation] == nil {
			s.Answers[t.Operation] = map[string]int{}
		}
		s.Answers[t.Operation][t.Class]++
		if t.Posts > 0 {
			s.Worked[t.Operation+" "+t.Shape]++
		}
	}
}

type tester struct {
	o   Options
	rep *Report
	// best is the shape that last worked, per operation.
	best map[string]string
	gql  map[string]*graphql.Client
	raw  string
	seq  int
}

func (t *tester) profile(ctx context.Context, index int, username string) Result {
	r := Result{Index: index}
	t.raw, t.seq = "", 0
	if t.o.RawDir != "" {
		t.raw = filepath.Join(t.o.RawDir, fmt.Sprintf("%03d", index))
	}
	id, err := t.acquire(ctx)
	if err != nil {
		r.Profile = Step{Class: "no_identity", Reason: err.Error()}
		return r
	}
	defer t.o.Pool.Release(id)
	r.Exit = id.Endpoint.Bucket
	base := strings.TrimRight(t.o.BaseURL, "/") + "/" + username + "/"

	// The profile page: tokens, and the bundles that may declare the
	// tagged tab's operation.
	pl, step := t.load(ctx, id, base)
	r.Profile = step
	if pl == nil || pl.Class != detect.OK || !id.Bootstrapped() {
		return r
	}
	t.scan(ctx, pl.HTML, base)
	resp, step := t.do(ctx, id, t.client("profile", nil), "profile", map[string]any{"username": username}, base)
	r.ProfileQuery = step
	var pk, uid string
	if resp != nil && resp.Class == detect.OK {
		for _, p := range resp.Parts {
			if u := p.Get("data.xig_user_by_username"); u.Exists() {
				pk, uid = u.Get("pk").String(), u.Get("id").String()
			}
		}
	}

	// The tagged tab, as a visitor clicking it would load it.
	page := base + "tagged/"
	pl, step = t.load(ctx, id, page)
	r.Tagged = step
	if pl != nil {
		r.LoginPage = pl.Class == detect.LoginWall || strings.Contains(pl.URL, "/accounts/login")
		if len(pl.HTML) > 0 {
			t.save("tagged-page.html", pl.HTML)
		}
		if pl.Class == detect.OK {
			r.Embedded, r.Markers = embedded(pl.HTML)
			t.scan(ctx, pl.HTML, page)
		}
	}
	if pk == "" && uid == "" {
		return r
	}
	// The operations, each with the shape that last worked first, until
	// one answers with posts.
	for _, op := range t.ops() {
		for _, sh := range t.order(op) {
			user := pk
			if sh.User == "id" {
				user = uid
			}
			if user == "" {
				continue
			}
			name := "tagged_" + sh.Name
			c := t.client(name, &registry.Query{Name: name, FriendlyName: op, DocID: t.rep.Operations[op],
				Variables: json.RawMessage(fmt.Sprintf(`{%q:"{{user}}",%q:"{{size=%d}}"}`, sh.UserKey, sh.SizeKey, PageSize)),
				Parser:    "none", ParserVersion: 1, Status: registry.StatusActive, Priority: 10})
			resp, step := t.do(ctx, id, c, name, map[string]any{"user": user}, page)
			try := Try{Operation: op, DocID: t.rep.Operations[op], Shape: sh.Name, Step: step}
			if resp != nil {
				try.Posts, try.HasNext = posts(resp)
				t.save(fmt.Sprintf("%02d-%s-%s.json", t.seq, sh.Name, try.Class), resp.Body)
			}
			r.Tries = append(r.Tries, try)
			if try.Posts > 0 {
				t.best[op] = sh.Name
				break
			}
			if resp != nil && (resp.Class == detect.LoginWall || resp.Class == detect.RateLimited || resp.Class == detect.Checkpoint) {
				return r // this identity is done; the next profile gets another
			}
		}
	}
	return r
}

// acquire takes an identity, waiting while every exit of the tier rests.
func (t *tester) acquire(ctx context.Context) (*session.Identity, error) {
	wait := t.o.MaxWait
	if wait <= 0 {
		wait = 10 * time.Minute
	}
	deadline := time.Now().Add(wait)
	for {
		id, err := t.o.Pool.Acquire(ctx, t.o.Tier)
		if err == nil || ctx.Err() != nil || time.Now().After(deadline) {
			return id, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(15*time.Second, wait)):
		}
	}
}

// ops are the tagged-looking operations found so far, at most MaxOps.
func (t *tester) ops() []string {
	var out []string
	for name := range t.rep.Operations {
		out = append(out, name)
	}
	sort.Strings(out)
	if len(out) > t.o.MaxOps {
		out = out[:t.o.MaxOps]
	}
	return out
}

func (t *tester) order(op string) []Shape {
	out := slices.Clone(Shapes)
	if best := t.best[op]; best != "" {
		slices.SortStableFunc(out, func(a, b Shape) int {
			switch {
			case a.Name == best:
				return -1
			case b.Name == best:
				return 1
			}
			return 0
		})
	}
	return out
}

// scan adds the tagged-looking operations page and its bundles declare.
func (t *tester) scan(ctx context.Context, html []byte, page string) {
	if t.o.Scanner == nil {
		return
	}
	pu, err := url.Parse(page)
	if err != nil {
		return
	}
	found := map[string]string{}
	t.o.Scanner.Page(ctx, html, pu, found)
	for name, doc := range found {
		if tagName.MatchString(name) {
			t.rep.Operations[name] = doc
		}
	}
}

// client is the probe's client with the seed and, when q is set, q.
func (t *tester) client(name string, q *registry.Query) *graphql.Client {
	if t.best == nil {
		t.best = map[string]string{}
	}
	key := name
	if q != nil {
		key += "|" + q.FriendlyName + "|" + q.DocID
	}
	if c, ok := t.gql[key]; ok {
		return c
	}
	seed := t.o.Seed
	seed.Queries = slices.Clone(seed.Queries)
	if q != nil {
		seed.Queries = append(seed.Queries, *q)
	}
	c := &graphql.Client{Registry: registry.NewStatic(seed), Rules: t.o.GQL.Rules, Builder: t.o.GQL.Builder}
	t.gql[key] = c
	return c
}

func (t *tester) load(ctx context.Context, id *session.Identity, page string) (*session.PageLoad, Step) {
	start := time.Now()
	pl, err := t.o.Loader.Load(ctx, id, page)
	s := Step{Ms: time.Since(start).Milliseconds()}
	if err != nil {
		s.Class, s.Reason = detect.Transport.String(), "page load failed"
		return nil, s
	}
	s.Class, s.Status, s.Bytes, s.Reason = pl.Class.String(), pl.Status, pl.Bytes, pl.Reason
	return pl, s
}

func (t *tester) do(ctx context.Context, id *session.Identity, c *graphql.Client, query string, vars map[string]any, referer string) (*graphql.Response, Step) {
	start := time.Now()
	resp, err := c.Do(ctx, id, graphql.Request{Query: query, Vars: vars, Referer: referer})
	s := Step{Ms: time.Since(start).Milliseconds()}
	t.seq++
	if err != nil {
		s.Class, s.Reason = detect.Transport.String(), "request failed"
		return nil, s
	}
	s.Class, s.Status, s.Bytes, s.Reason = resp.Class.String(), resp.Status, resp.BytesIn+resp.BytesOut, resp.Reason
	for _, e := range resp.Errors {
		if m := e.Message; m != "" && !slices.Contains(s.Errors, m) && len(s.Errors) < 5 {
			s.Errors = append(s.Errors, m)
		}
	}
	return resp, s
}

func (t *tester) save(name string, body []byte) {
	if t.raw == "" || len(body) == 0 {
		return
	}
	if err := os.MkdirAll(t.raw, 0o700); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(t.raw, name), body, 0o600)
}

// posts is the largest list of posts in an answer, and whether it offered
// another page.
func posts(resp *graphql.Response) (int, bool) {
	n, next := 0, false
	for _, p := range resp.Parts {
		var v any
		if err := json.Unmarshal([]byte(p.Raw), &v); err != nil {
			continue
		}
		c, h := largestList(v, false)
		n, next = max(n, c), next || h
	}
	return n, next
}

// largestList walks v for "edges" or "items" lists (under a tagged-looking
// key when tagged), returning the longest and whether a page_info beside
// one offers another page.
func largestList(v any, tagged bool) (int, bool) {
	n, next := 0, false
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			inside := tagged || tagName.MatchString(k)
			if (k == "edges" || k == "items") && inside {
				if l, ok := c.([]any); ok {
					n = max(n, len(l))
				}
			}
			if k == "page_info" && inside {
				if pi, ok := c.(map[string]any); ok && pi["has_next_page"] == true {
					next = true
				}
			}
			if k == "more_available" && inside && c == true {
				next = true
			}
			cn, ch := largestList(c, inside)
			n, next = max(n, cn), next || ch
		}
	case []any:
		for _, c := range x {
			cn, ch := largestList(c, tagged)
			n, next = max(n, cn), next || ch
		}
	}
	return n, next
}

var scriptJSON = regexp.MustCompile(`(?s)<script type="application/json"[^>]*>(.*?)</script>`)

// embedded is the most tagged posts the page's data holds, and the
// tagged-looking keys it has.
func embedded(html []byte) (int, []string) {
	n := 0
	keys := map[string]bool{}
	for _, m := range scriptJSON.FindAllSubmatch(html, -1) {
		if !tagName.Match(m[1]) {
			continue
		}
		var v any
		dec := json.NewDecoder(bytes.NewReader(m[1]))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			continue
		}
		c, _ := largestList(v, false)
		n = max(n, c)
		markers(v, keys)
	}
	return n, slices.Sorted(func(yield func(string) bool) {
		for k := range keys {
			if !yield(k) {
				return
			}
		}
	})
}

func markers(v any, out map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			if tagName.MatchString(k) && len(out) < 20 {
				out[k] = true
			}
			markers(c, out)
		}
	case []any:
		for _, c := range x {
			markers(c, out)
		}
	}
}

// Write saves report.json in dir.
func Write(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.json"), b, 0o600)
}
