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
