package parse

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// The website is read from external_url when the answer serves it
// (REQ-008 fix 18); without it, it stays empty and the bio links carry it.
func TestProfileExternalURL(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{`{"data":{"user":{"pk":"1","username":"a","external_url":"https://shop.example/","bio_links":[{"url":"https://l.example/"}]}}}`,
			"https://shop.example/"},
		{`{"data":{"user":{"pk":"1","username":"a","external_lynx_url":"https://l.instagram.com/?u=x"}}}`, "https://l.instagram.com/?u=x"},
		{`{"data":{"user":{"pk":"1","username":"a","bio_links":[{"url":"https://l.example/"}]}}}`, ""},
	} {
		res, _ := Profile(graphql.SplitParts([]byte(tc.body)))
		if res.Profile == nil || res.Profile.ExternalUrl != tc.want {
			t.Errorf("%s: external_url %q, want %q", tc.body, res.Profile.GetExternalUrl(), tc.want)
		}
	}
}

// A profile picture's media id follows the picture's file, not its signed
// URL: the same picture under a new signature keeps its id, and a changed
// picture gets a new one, so it is downloaded again.
func TestProfilePicMediaID(t *testing.T) {
	id := func(u string) string { return MediaID("profile:42"+picAsset(u), "original") }
	a := id("https://scontent.cdninstagram.com/v/t51.2885-19/111_n.jpg?stp=x&oh=00_A&oe=67A1B2C3")
	b := id("https://scontent-fra5-1.cdninstagram.com/v/t51.2885-19/111_n.jpg?stp=y&oh=00_B&oe=68000000")
	c := id("https://scontent.cdninstagram.com/v/t51.2885-19/222_n.jpg?oe=67A1B2C3")
	if a != b || a == c {
		t.Fatalf("ids: same picture %v, changed picture %v", a == b, a != c)
	}
	if picAsset("://bad") != "" || picAsset("https://host/") != "" {
		t.Fatal("picAsset of a URL with no file")
	}
}
