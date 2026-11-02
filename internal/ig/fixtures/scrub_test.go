package fixtures

import (
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
)

// A synthetic answer with every kind of personal data Instagram serves.
const answer = `{"data":{"xig_user_by_username":{"pk":"48211934","id":"17841448211934","username":"zara.qadri","full_name":"Zara Qadri",
"biography":"Photographer in Erbil 📷 #travel","bio_links":[{"title":"My shop","url":"https://zaras-shop.example.org/x","lynx_url":"https://l.instagram.com/?u=https%3A%2F%2Ft.me%2FZara.Qadri&e=AT0"}],
"profile_pic_url":"https:\/\/scontent-fra5-1.cdninstagram.com\/v\/t51.2885-19\/12345_n.jpg?stp=dst&_nc_ht=x&oh=00_ABC&oe=67A1B2C3",
"linked_fb_info":{"linked_fb_page":{"name":"Zara Qadri Photography","id":"103942"}},
"polaris_ordered_timeline_connection":{"edges":[{"node":{"pk":"3456789012345678901","code":"DAbCdEfGhIj","caption":{"text":"Sunset with @omar_k at Duhok"},
"accessibility_caption":"Photo by Zara Qadri on June 3","display_uri":"https://scontent.cdninstagram.com/v/t51/999_n.jpg?oe=67A1B2C3&oh=1",
"user":{"username":"zara.qadri","id":"17841448211934"},"media_type":1,"product_type":"feed","clips_tab_pinned_user_ids":["77700011"]}}],"page_info":{"end_cursor":"QVFE","has_next_page":true}}}}}
{"data":{"xig_polaris_media":{"comments_connection":{"edges":[{"node":{"pk":"17998877665544332","text":"Beautiful shot Zara! 😍","created_at":1730000000,
"user":{"pk":"5550001","username":"omar_k","profile_pic_url":"https://scontent.cdninstagram.com/v/abc.jpg?oe=67A1B2C3","is_verified":false}}}],
"page_info":{"end_cursor":null,"has_next_page":false}}}},"extensions":{"is_final":true}}`

func TestScrubRemovesPersonalData(t *testing.T) {
	s := New("test-salt")
	out, err := s.JSON([]byte(answer))
	if err != nil {
		t.Fatal(err)
	}
	for _, real := range []string{"zara.qadri", "Zara Qadri", "Zara", "omar_k", "Erbil", "Duhok", "Sunset", "Beautiful",
		"48211934", "17841448211934", "5550001", "77700011", "DAbCdEfGhIj", "zaras-shop", "My shop", "Photography", "oh=00_ABC", "_nc_ht", "t51.2885-19", "Zara.Qadri", "t.me"} {
		if strings.Contains(string(out), real) {
			t.Errorf("%q survived:\n%s", real, out)
		}
	}
	if leaks := s.Leaks(out); len(leaks) > 0 {
		t.Errorf("leaks: %v", leaks)
	}
	// Structure survives: the parser still reads the scrubbed answer, the
	// media pk keeps its time, the shortcode matches the new pk, text keeps
	// its UTF-16 length and hashtag shape, and expiries stay.
	parts := splitLines(out)
	g, rep := parse.Grid(parts)
	if !rep.OK() || len(g.Items) != 1 {
		t.Fatalf("grid: %+v %v", g, rep)
	}
	it := g.Items[0]
	orig := parse.TakenAtFromPK("3456789012345678901")
	if it.TakenAt == nil || !it.TakenAt.AsTime().Equal(orig) {
		t.Fatalf("time bits lost: %v, want %v", it.TakenAt, orig)
	}
	if it.Shortcode != Shortcode(strings.TrimSpace(gjsonStr(parts[0], "data.xig_user_by_username.polaris_ordered_timeline_connection.edges.0.node.pk"))) {
		t.Fatalf("shortcode %s does not match its pk", it.Shortcode)
	}
	if utf16Len(it.Caption) != utf16Len("Sunset with @omar_k at Duhok") || !strings.Contains(it.Caption, "@") {
		t.Fatalf("caption %q", it.Caption)
	}
	p, _ := parse.Profile(parts[:1])
	if p.Profile.Username == "" || p.Profile.Username == "zara.qadri" || !strings.Contains(p.Profile.ProfilePic.GetUrl(), "oe=67A1B2C3") {
		t.Fatalf("profile: %+v", p.Profile)
	}
	if !strings.Contains(p.Profile.Biography, "#xxxxxx") || !strings.Contains(p.Profile.Biography, "📷") {
		t.Fatalf("biography %q", p.Profile.Biography)
	}
	// The same person keeps one pseudonym across documents.
	c, _ := parse.Comments(parts[1:], parse.CommentContext{})
	if len(c.Comments) != 1 || c.Comments[0].Author.Username == "omar_k" {
		t.Fatalf("comments: %+v", c.Comments)
	}
	if !strings.Contains(it.Caption, "@") || strings.Contains(string(out), `"omar_k"`) {
		t.Fatal("mention")
	}
}

func TestScrubHTMLKeepsOnlyWantedPayloads(t *testing.T) {
	page := `<html><head><meta property="og:description" content="Zara Qadri: Sunset"><title>Zara Qadri (@zara.qadri)</title></head><body>
<script>window.x = "zara.qadri";</script>
<script type="application/json" data-sjs>{"require":[["x",{"__bbox":{"result":{"data":{"xig_polaris_media":{"if_not_gated_logged_out":{"pk":"3456789012345678901","code":"DAbCdEfGhIj","taken_at":1730000000,"user":{"pk":"48211934","username":"zara.qadri"},"caption":{"text":"Hello Erbil"},"like_count":10,"comment_count":2,"media_type":1,"product_type":"feed"}}}}}}]]}</script>
<script type="application/json">{"other":"zara.qadri unrelated"}</script></body></html>`
	s := New("salt")
	out, err := s.HTML([]byte(page), "xig_polaris_media")
	if err != nil {
		t.Fatal(err)
	}
	for _, real := range []string{"zara.qadri", "Zara Qadri", "Erbil", "og:description", "unrelated", "DAbCdEfGhIj"} {
		if strings.Contains(string(out), real) {
			t.Errorf("%q survived:\n%s", real, out)
		}
	}
	code := Shortcode(s.ids["3456789012345678901"])
	res, rep := parse.PostPage(out, code, "https://www.instagram.com")
	if !rep.OK() || res.Post.GetLikeCount() != 10 || res.Post.Shortcode != code {
		t.Fatalf("post: %+v %v", res.Post, rep)
	}
}

func splitLines(b []byte) []gjson.Result {
	var out []gjson.Result
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		out = append(out, gjson.Parse(l))
	}
	return out
}

func gjsonStr(v gjson.Result, path string) string { return v.Get(path).String() }

func utf16Len(s string) int { return len(utf16.Encode([]rune(s))) }
