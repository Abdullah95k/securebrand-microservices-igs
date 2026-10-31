package graphql

import (
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

// RequestBuilder writes one GraphQL request the way a platform's web client
// sends it: the endpoint path, the headers in the browser's wire order and
// the form body. The client adds the query's registry endpoint and headers
// on top, and owns pacing, the transport and the classification.
type RequestBuilder interface {
	Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) Built
}

// Built is a request ready for the wire.
type Built struct {
	Path   string // on the identity's host; a registry endpoint replaces it
	Header [][2]string
	Body   []byte
}

// DefaultEndpoint is the path of Facebook's persisted queries.
const DefaultEndpoint = "/api/graphql/"

// FacebookBuilder writes logged-out Facebook's requests.
type FacebookBuilder struct {
	AcceptLanguage string
	Locale         string
}

// Build implements RequestBuilder.
func (b FacebookBuilder) Build(id *session.Identity, q registry.Query, vars []byte, params map[string]string, referer string) Built {
	tok := id.Tokens()
	hdr := [][2]string{
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"x-fb-lsd", tok["lsd"]},
		{"sec-ch-ua", id.FP.SecChUa},
		{"sec-ch-ua-mobile", id.FP.SecChUaMobile},
		{"x-fb-friendly-name", q.FriendlyName},
		{"x-asbd-id", params["x_asbd_id"]},
		{"user-agent", id.FP.UserAgent},
		{"content-type", "application/x-www-form-urlencoded"},
		{"accept", "*/*"},
		{"origin", id.Base.Scheme + "://" + id.Base.Host},
		{"sec-fetch-site", "same-origin"},
		{"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"},
		{"referer", referer},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", b.AcceptLanguage},
		{"priority", "u=1, i"},
	}
	return Built{Path: DefaultEndpoint, Header: hdr, Body: EncodeForm(b.form(id, tok, params, q, vars))}
}

// Field is one form field.
type Field struct{ K, V string }

// form builds the POST body in the order a browser sends it.
func (b FacebookBuilder) form(id *session.Identity, tok session.Tokens, params map[string]string, q registry.Query, vars []byte) []Field {
	cometReq := params["__comet_req"]
	if tok["__comet_req"] != "" {
		cometReq = tok["__comet_req"]
	}
	f := []Field{
		{"av", "0"},
		{"__aaid", "0"},
		{"__user", "0"},
		{"__a", params["__a"]},
		{"__req", id.NextReq()},
		{"__hs", tok["__hs"]},
		{"dpr", params["dpr"]},
		{"__ccg", params["__ccg"]},
		{"__rev", tok["__rev"]},
		{"__s", id.SessionParam()},
		{"__hsi", tok["__hsi"]},
		{"__comet_req", cometReq},
	}
	if b.Locale != "" {
		f = append(f, Field{"locale", b.Locale})
	}
	if tok["fb_dtsg"] != "" {
		f = append(f, Field{"fb_dtsg", tok["fb_dtsg"]})
	}
	f = append(f,
		Field{"lsd", tok["lsd"]},
		Field{"jazoest", htmlx.Jazoest(tok)},
		Field{"__spin_r", tok["__spin_r"]},
		Field{"__spin_b", tok["__spin_b"]},
		Field{"__spin_t", tok["__spin_t"]},
		Field{"fb_api_caller_class", params["fb_api_caller_class"]},
		Field{"fb_api_req_friendly_name", q.FriendlyName},
		Field{"variables", string(vars)},
		Field{"server_timestamps", params["server_timestamps"]},
		Field{"doc_id", q.DocID},
	)
	return f
}

// EncodeForm URL-encodes fields in order, leaving out empty values (except
// variables, which is always sent).
func EncodeForm(f []Field) []byte {
	var b strings.Builder
	for i, p := range f {
		if p.V == "" && p.K != "variables" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(p.K))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(p.V))
	}
	return []byte(b.String())
}
