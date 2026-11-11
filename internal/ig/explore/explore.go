// Package explore is the read-only probe of Instagram's hashtag and
// location pages (owner's D-055 point 4). Logged out, on one exit, with a
// few page loads (MaxPagesLimit at most), it measures whether each page
// loads, shows a login wall or sends the visitor to log in, and how many
// posts and distinct accounts it shows.
//
// It keeps counts only: no names, no posts, no accounts. Nothing is stored
// or published, and no discovery change follows from it. A rate limit or
// a checkpoint ends the run at once: the probe never pushes past a bot
// check.
package explore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// MaxPagesLimit is the most page loads one run makes, whatever is asked.
const MaxPagesLimit = 20

// Page kinds.
const (
	KindHashtag  = "hashtag"
	KindLocation = "location"
)

// Options configure a run.
type Options struct {
	BaseURL string
	// Hashtags (without #) and Locations (location ids) to load. The run
	// alternates between them.
	Hashtags  []string
	Locations []string
	// Tier is the proxy tier; the run keeps one identity, so one exit.
	Tier   string
	Pool   *session.Pool
	Loader *session.Loader
	// MaxPages caps the page loads (MaxPagesLimit at most; 0 means it).
	MaxPages int
	// MaxWait is how long to wait for an identity (default 10 minutes).
	MaxWait  time.Duration
	Progress func(string)
}

// Page is one page load's result. It names no hashtag or place: N is
// its place in the run.
type Page struct {
	N        int    `json:"n"`
	Kind     string `json:"kind"`
	Class    string `json:"class"`
	Status   int    `json:"status,omitempty"`
	Login    bool   `json:"login_redirect,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
	Ms       int64  `json:"ms"`
	Posts    int    `json:"posts"`
	Accounts int    `json:"accounts"`
}

// Totals add up one kind's pages. Posts and Accounts are distinct across
// all of that kind's pages.
type Totals struct {
	Pages         int `json:"pages"`
	Loaded        int `json:"loaded"`
	LoginRedirect int `json:"login_redirect"`
	LoginWall     int `json:"login_wall"`
	Other         int `json:"other"`
	WithPosts     int `json:"with_posts"`
	Posts         int `json:"posts"`
	Accounts      int `json:"accounts"`
}

// Report is a run's result: counts only, safe to send back.
type Report struct {
	StartedAt time.Time `json:"started_at"`
	Tier      string    `json:"tier"`
	Requests  int       `json:"requests"`
	// Stopped says why the run ended early: rate_limited, checkpoint,
	// no_identity or cancelled.
	Stopped  string `json:"stopped,omitempty"`
	Pages    []Page `json:"pages"`
	Hashtag  Totals `json:"hashtag"`
	Location Totals `json:"location"`
}

type target struct{ kind, path string }

// targets alternates hashtag and location pages, max at most.
func targets(o Options, max int) []target {
	var out []target
	for i := 0; len(out) < max && (i < len(o.Hashtags) || i < len(o.Locations)); i++ {
		if i < len(o.Hashtags) {
			if h := strings.TrimPrefix(strings.TrimSpace(o.Hashtags[i]), "#"); h != "" {
				out = append(out, target{KindHashtag, "/explore/tags/" + url.PathEscape(strings.ToLower(h)) + "/"})
			}
		}
		if len(out) < max && i < len(o.Locations) {
			if l := strings.TrimSpace(o.Locations[i]); l != "" {
				out = append(out, target{KindLocation, "/explore/locations/" + url.PathEscape(l) + "/"})
			}
		}
	}
	return out
}

// Run loads each page in turn on one identity.
func Run(ctx context.Context, o Options) Report {
	rep := Report{StartedAt: time.Now().UTC(), Tier: o.Tier}
	max := o.MaxPages
	if max <= 0 || max > MaxPagesLimit {
		max = MaxPagesLimit
	}
	list := targets(o, max)
	if len(list) == 0 {
		return rep
	}
	id, err := acquire(ctx, o)
	if err != nil {
		rep.Stopped = "no_identity"
		return rep
	}
	defer o.Pool.Release(id)
	seen := map[string]map[string]bool{KindHashtag: {}, KindLocation: {}}
	seenPosts := map[string]map[string]bool{KindHashtag: {}, KindLocation: {}}
	base := strings.TrimRight(o.BaseURL, "/")
	for i, tg := range list {
		if ctx.Err() != nil {
			rep.Stopped = "cancelled"
			break
		}
		p := Page{N: i + 1, Kind: tg.kind}
		start := time.Now()
		pl, err := o.Loader.Load(ctx, id, base+tg.path)
		rep.Requests++
		p.Ms = time.Since(start).Milliseconds()
		if err != nil {
			p.Class = detect.Transport.String()
		} else {
			p.Class, p.Status, p.Bytes = pl.Class.String(), pl.Status, pl.Bytes
			p.Login = strings.Contains(pl.URL, "/accounts/login")
			if pl.Class == detect.OK && !p.Login {
				posts, accounts := count(pl.HTML)
				p.Posts, p.Accounts = len(posts), len(accounts)
				for a := range accounts {
					seen[tg.kind][a] = true
				}
				for c := range posts {
					seenPosts[tg.kind][c] = true
				}
			}
		}
		rep.Pages = append(rep.Pages, p)
		if o.Progress != nil {
			o.Progress(fmt.Sprintf("page %d of %d (%s): %s%s, %d posts, %d accounts", p.N, len(list), p.Kind, p.Class,
				map[bool]string{true: " (sent to log in)"}[p.Login], p.Posts, p.Accounts))
		}
		if err == nil && (pl.Class == detect.RateLimited || pl.Class == detect.Checkpoint) {
			rep.Stopped = pl.Class.String()
			break
		}
	}
	for _, p := range rep.Pages {
		t := &rep.Hashtag
		if p.Kind == KindLocation {
			t = &rep.Location
		}
		t.Pages++
		switch {
		case p.Login:
			t.LoginRedirect++
		case p.Class == detect.LoginWall.String():
			t.LoginWall++
		case p.Class == detect.OK.String():
			t.Loaded++
		default:
			t.Other++
		}
		if p.Posts > 0 {
			t.WithPosts++
		}
	}
	rep.Hashtag.Accounts, rep.Location.Accounts = len(seen[KindHashtag]), len(seen[KindLocation])
	rep.Hashtag.Posts, rep.Location.Posts = len(seenPosts[KindHashtag]), len(seenPosts[KindLocation])
	return rep
}

func acquire(ctx context.Context, o Options) (*session.Identity, error) {
	wait := o.MaxWait
	if wait <= 0 {
		wait = 10 * time.Minute
	}
	deadline := time.Now().Add(wait)
	for {
		id, err := o.Pool.Acquire(ctx, o.Tier)
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

var scriptJSON = regexp.MustCompile(`(?s)<script type="application/json"[^>]*>(.*?)</script>`)

// count finds the distinct posts (an object with a code and a pk or id)
// and distinct accounts (a post's user or owner, by id, pk or username)
// in the page's embedded data. The sets live only for the count.
func count(html []byte) (posts, accounts map[string]bool) {
	posts, accounts = map[string]bool{}, map[string]bool{}
	for _, m := range scriptJSON.FindAllSubmatch(html, -1) {
		var v any
		dec := json.NewDecoder(bytes.NewReader(m[1]))
		dec.UseNumber()
		if dec.Decode(&v) != nil {
			continue
		}
		walk(v, posts, accounts)
	}
	return posts, accounts
}

func walk(v any, posts, accounts map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		code, _ := x["code"].(string)
		if code == "" {
			code, _ = x["shortcode"].(string)
		}
		if code != "" && (x["pk"] != nil || x["id"] != nil) {
			posts[code] = true
			for _, k := range []string{"user", "owner"} {
				if u, ok := x[k].(map[string]any); ok {
					if a := accountKey(u); a != "" {
						accounts[a] = true
					}
				}
			}
		}
		for _, c := range x {
			walk(c, posts, accounts)
		}
	case []any:
		for _, c := range x {
			walk(c, posts, accounts)
		}
	}
}

func accountKey(u map[string]any) string {
	for _, k := range []string{"id", "pk"} {
		if s := fmt.Sprint(u[k]); u[k] != nil && s != "" {
			return "id:" + s
		}
	}
	if s, _ := u["username"].(string); s != "" {
		return "u:" + strings.ToLower(s)
	}
	return ""
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
