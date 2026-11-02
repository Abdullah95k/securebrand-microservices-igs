package parse

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

var update = flag.Bool("update", false, "rewrite the golden files from the parsers' output")

// fixture is one testdata/ig/<query>/<case>/ directory: a response scrubbed
// from the owner's probe runs (internal/ig/fixtures), its meta.json and the
// parser's expected output (golden.json).
type fixture struct {
	query, name, dir string
	body             []byte
	meta             map[string]any
}

func fixtures(t *testing.T) []fixture {
	t.Helper()
	dirs, _ := filepath.Glob("../../../testdata/ig/*/*")
	var out []fixture
	for _, d := range dirs {
		f := fixture{query: filepath.Base(filepath.Dir(d)), name: filepath.Base(d), dir: d, meta: map[string]any{}}
		for _, n := range []string{"response.ndjson", "response.html"} {
			if b, err := os.ReadFile(filepath.Join(d, n)); err == nil {
				f.body = b
			}
		}
		if b, err := os.ReadFile(filepath.Join(d, "meta.json")); err == nil {
			_ = json.Unmarshal(b, &f.meta)
		}
		if f.body == nil {
			t.Fatalf("%s: no response", d)
		}
		out = append(out, f)
	}
	if len(out) < 10 {
		t.Fatalf("only %d fixtures", len(out))
	}
	return out
}

// run parses a fixture with its query's parser and returns a JSON-able
// summary of everything the parser produced, and its report.
func run(t *testing.T, f fixture) (map[string]any, walk.Report) {
	t.Helper()
	parts := graphql.SplitParts(f.body)
	out := map[string]any{}
	switch f.query {
	case "profile":
		r, rep := Profile(parts)
		out["profile"], out["highlights"], out["media"] = msg(r.Profile), msgs(r.Highlights), msgs(r.Media)
		out["user_id"], out["not_found"], out["has_clips"], out["highlights_hash"] = r.UserID, r.NotFound, r.HasClips, len(r.HighlightsHash) > 0
		return out, rep
	case "grid":
		r, rep := Grid(parts)
		out["items"], out["page_info"], out["user_id"] = r.Items, r.PageInfo, r.UserID
		return out, rep
	case "comments":
		r, rep := Comments(parts, CommentContext{MediaPK: 1, OwnerPK: 2})
		out["comments"], out["page_info"], out["node_null"] = msgs(r.Comments), r.PageInfo, r.NodeNull
		return out, rep
	case "reels":
		r, rep := Reels(parts)
		out["reels"], out["page_info"] = r.Reels, r.PageInfo
		return out, rep
	case "highlights":
		hs, media, rep := HighlightItems(parts, 9)
		out["highlights"], out["media"] = msgs(hs), msgs(media)
		return out, rep
	case "post_page":
		code, _ := f.meta["shortcode"].(string)
		r, rep := PostPage(f.body, code, "https://www.instagram.com")
		out["post"], out["media"], out["gated"] = msg(r.Post), msgs(r.Media), r.Gated
		out["comments"], out["comments_page_info"] = msgs(r.Comments.Comments), r.Comments.PageInfo
		return out, rep
	}
	t.Fatalf("%s: no parser for query %q", f.dir, f.query)
	return nil, walk.Report{}
}

func msg(m proto.Message) any {
	if m == nil {
		return nil
	}
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		panic(err)
	}
	var v any
	_ = json.Unmarshal(b, &v)
	return v
}

func msgs[T proto.Message](ms []T) []any {
	out := make([]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, msg(m))
	}
	return out
}

// TestGolden parses every fixture and compares the output with golden.json
// (go test ./internal/ig/parse -update rewrites them). protojson varies its
// spacing between builds on purpose, so the output is normalised through
// encoding/json, which sorts keys.
func TestGolden(t *testing.T) {
	for _, f := range fixtures(t) {
		t.Run(f.query+"/"+f.name, func(t *testing.T) {
			out, rep := run(t, f)
			out["report"] = map[string]any{"missing": sorted(rep.Missing), "optional": sorted(rep.Optional)}
			got, err := json.MarshalIndent(out, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join(f.dir, "golden.json")
			if *update {
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update to create it)", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("output differs from %s (run with -update after checking the change)\n%s", path, firstDiff(string(want), string(got)))
			}
		})
	}
}

func sorted(xs []string) []string {
	out := append([]string{}, xs...)
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

func firstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return "line " + itoa(i+1) + ":\n- " + al[i] + "\n+ " + bl[i]
		}
	}
	return "lengths differ"
}

func itoa(i int) string { return strings.TrimSpace(strings.Repeat(" ", 0) + jsonNum(i)) }

func jsonNum(i int) string { b, _ := json.Marshal(i); return string(b) }

// What each real fixture must show, beyond matching its golden file: the
// meaning, so a careless -update cannot hide a regression.
func TestFixtureSemantics(t *testing.T) {
	byName := map[string]fixture{}
	for _, f := range fixtures(t) {
		byName[f.query+"/"+f.name] = f
	}
	get := func(name string) fixture {
		f, ok := byName[name]
		if !ok {
			t.Fatalf("fixture %s missing", name)
		}
		return f
	}
	parts := func(name string) []gjson.Result { return graphql.SplitParts(get(name).body) }

	full, rep := Profile(parts("profile/profile_full"))
	if !rep.OK() || full.Profile.UserPk == 0 || full.UserID == "" || len(full.Highlights) < 3 || len(full.Profile.BioLinks) == 0 ||
		full.Profile.FollowerCount == nil || full.Profile.LinkedFacebookPage == "" || len(full.HighlightsHash) == 0 || len(full.Media) != 1 {
		t.Errorf("profile_full: %+v %v", full, rep)
	}
	if p, _ := Profile(parts("profile/profile_private")); !p.Profile.IsPrivate || p.Profile.Status.String() != "PROFILE_STATUS_PRIVATE" {
		t.Errorf("profile_private: %+v", p.Profile)
	}
	if p, rep := Profile(parts("profile/profile_missing")); !p.NotFound || !rep.OK() {
		t.Errorf("profile_missing: %+v %v", p, rep)
	}
	g, rep := Grid(parts("grid/grid_first_mixed"))
	types := map[int32]bool{}
	for _, it := range g.Items {
		types[int32(it.MediaType)] = true
		if it.TakenAt == nil || it.Shortcode == "" || it.MediaPK == 0 {
			t.Errorf("grid item without time or ids: %+v", it)
		}
	}
	if !rep.OK() || !types[1] || !types[2] || !types[8] || !g.PageInfo.HasNext || g.PageInfo.EndCursor == "" || g.UserID == "" {
		t.Errorf("grid_first_mixed: %d items, types %v, %+v %v", len(g.Items), types, g.PageInfo, rep)
	}
	if g, _ := Grid(parts("grid/grid_end")); g.PageInfo.HasNext || len(g.Items) == 0 {
		t.Errorf("grid_end: %+v, %d items", g.PageInfo, len(g.Items))
	}
	if g, _ := Grid(parts("grid/grid_page")); !g.PageInfo.HasNext || len(g.Items) < 12 {
		t.Errorf("grid_page: %+v, %d items", g.PageInfo, len(g.Items))
	}
	c, rep := Comments(parts("comments/comments_page"), CommentContext{MediaPK: 1})
	if !rep.OK() || len(c.Comments) < 8 || !c.PageInfo.HasNext {
		t.Errorf("comments_page: %d %+v %v", len(c.Comments), c.PageInfo, rep)
	}
	for _, cm := range c.Comments {
		if cm.CreatedAt == nil || cm.Author.GetUserPk() == 0 || cm.Depth != 0 || cm.ParentCommentPk != nil {
			t.Errorf("comment: %+v", cm)
		}
	}
	if c, _ := Comments(parts("comments/comments_end"), CommentContext{}); c.PageInfo.HasNext || len(c.Comments) == 0 {
		t.Errorf("comments_end: %+v", c.PageInfo)
	}
	r, rep := Reels(parts("reels/reels_hidden_pinned"))
	hidden, pinned, plays := 0, 0, 0
	for _, x := range r.Reels {
		if x.CountsHidden {
			hidden++
			if x.LikeCount != nil {
				t.Errorf("hidden likes must be nil, got %d", *x.LikeCount)
			}
		}
		if x.Pinned {
			pinned++
		}
		if x.PlayCount != nil {
			plays++
		}
	}
	if !rep.OK() || hidden == 0 || pinned == 0 || plays != len(r.Reels) || len(r.Reels) < 12 {
		t.Errorf("reels: %d reels, %d hidden, %d pinned, %d with plays %v", len(r.Reels), hidden, pinned, plays, rep)
	}
	for _, name := range []string{"post_page/post_page_photo", "post_page/post_page_reel", "post_page/post_page_carousel"} {
		f := get(name)
		code, _ := f.meta["shortcode"].(string)
		res, rep := PostPage(f.body, code, "https://www.instagram.com")
		if !rep.OK() || res.Post.TakenAt == nil || res.Post.Shortcode != code || res.Post.CommentCount == nil || len(res.Media) == 0 {
			t.Errorf("%s: %+v %v", name, res.Post, rep)
			continue
		}
		switch name {
		case "post_page/post_page_reel":
			if res.Post.ProductType != "clips" || res.Post.VideoDurationMs == 0 || !strings.Contains(res.Post.Permalink, "/reel/") {
				t.Errorf("reel: %+v", res.Post)
			}
		case "post_page/post_page_carousel":
			if len(res.Post.Children) < 2 || res.Post.MediaType != 8 {
				t.Errorf("carousel: %d children, type %v", len(res.Post.Children), res.Post.MediaType)
			}
		}
		// The page's pk-encoded time agrees with its taken_at (U10).
		if pt := TakenAtFromPK(itoa64(res.Post.MediaPk)); pt.IsZero() || abs(pt.Sub(res.Post.TakenAt.AsTime()).Minutes()) > 60 {
			t.Errorf("%s: pk time %v vs taken_at %v", name, pt, res.Post.TakenAt.AsTime())
		}
	}
}

func itoa64(n int64) string { b, _ := json.Marshal(n); return string(b) }

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// Highlight items (handcrafted: the probe's answers carried none): each
// item with its renditions, a video with its length.
func TestHighlightItems(t *testing.T) {
	var f fixture
	for _, x := range fixtures(t) {
		if x.query == "highlights" && x.name == "handcrafted_items" {
			f = x
		}
	}
	hs, media, rep := HighlightItems(graphql.SplitParts(f.body), 9)
	if !rep.OK() || len(hs) != 1 || hs[0].HighlightId != 17900000000000001 || len(hs[0].Items) != 2 || hs[0].ItemCount != 2 {
		t.Fatalf("highlights: %+v %v", hs, rep)
	}
	var video int
	for _, m := range media {
		if m.DurationMs == 12500 {
			video++
		}
	}
	if len(media) != 5 || video != 2 {
		t.Fatalf("media: %d rows, %d with the video's length", len(media), video)
	}
}
