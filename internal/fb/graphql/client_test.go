package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/transport"
)

// recorder answers every request with an empty data object and keeps it.
type recorder struct {
	transport.Doer
	reqs *[]*transport.Request
}

func (r recorder) Do(_ context.Context, req *transport.Request) (*transport.Response, error) {
	*r.reqs = append(*r.reqs, req)
	return &transport.Response{Status: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"data":{"node":{"id":"1"}}}`)}, nil
}

func (recorder) Close() {}

// A query's registry endpoint and headers reach the wire; a query without
// them is sent exactly as before (fbs's default endpoint and header list).
func TestQueryEndpointAndHeaders(t *testing.T) {
	var reqs []*transport.Request
	proxies, err := proxy.New(config.Proxy{})
	if err != nil {
		t.Fatal(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: "https://www.example.com", Profile: "chrome_146",
		IdentityRPS: 1000, IdentityBurst: 1000, MaxRequestsMin: 1000, MaxRequestsMax: 1001, MaxPerTier: 1}, proxies, fps,
		func(transport.Options) (transport.Doer, error) { return recorder{reqs: &reqs}, nil })
	if err != nil {
		t.Fatal(err)
	}
	id, err := pool.Acquire(context.Background(), "direct")
	if err != nil {
		t.Fatal(err)
	}
	vars := json.RawMessage(`{"id":"{{id}}"}`)
	c := &Client{Rules: detect.DefaultRules(), AcceptLanguage: "en-US", Registry: registry.NewStatic(registry.Seed{
		ClientParams: map[string]string{"x_asbd_id": "359341"},
		Queries: []registry.Query{
			{Name: "plain", DocID: "1", FriendlyName: "PlainQuery", Variables: vars, Status: registry.StatusActive},
			{Name: "routed", DocID: "2", FriendlyName: "RoutedQuery", Variables: vars, Status: registry.StatusActive,
				Endpoint: "/graphql/query", Headers: []registry.Header{
					{Name: "x-root-field-name", Value: "xdt_root"},
					{Name: "accept", Value: "application/json"},
				}},
		}})}
	for _, q := range []string{"plain", "routed"} {
		resp, err := c.Do(context.Background(), id, Request{Query: q, Vars: map[string]any{"id": "7"}})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("%s: %v %+v", q, err, resp)
		}
	}
	if len(reqs) != 2 {
		t.Fatalf("%d requests", len(reqs))
	}
	plain, routed := reqs[0], reqs[1]
	if plain.URL != "https://www.example.com/api/graphql/" || routed.URL != "https://www.example.com/graphql/query" {
		t.Fatalf("urls %s %s", plain.URL, routed.URL)
	}
	names := func(r *transport.Request) []string {
		var out []string
		for _, h := range r.Header {
			out = append(out, h[0])
		}
		return out
	}
	want := []string{"sec-ch-ua-platform", "x-fb-lsd", "sec-ch-ua", "sec-ch-ua-mobile", "x-fb-friendly-name",
		"x-asbd-id", "user-agent", "content-type", "accept", "origin", "sec-fetch-site", "sec-fetch-mode",
		"sec-fetch-dest", "referer", "accept-encoding", "accept-language", "priority"}
	if got := names(plain); len(got) != len(want) || fmtList(got) != fmtList(want) {
		t.Fatalf("default header order changed:\n%v\n%v", got, want)
	}
	// The override keeps accept's position; the new header goes last.
	if got := names(routed); fmtList(got) != fmtList(append(append([]string{}, want...), "x-root-field-name")) {
		t.Fatalf("routed header order: %v", got)
	}
	for _, h := range routed.Header {
		if h[0] == "accept" && h[1] != "application/json" {
			t.Fatalf("accept = %q", h[1])
		}
		if h[0] == "x-root-field-name" && h[1] != "xdt_root" {
			t.Fatalf("x-root-field-name = %q", h[1])
		}
	}
	for _, h := range plain.Header {
		if h[0] == "accept" && h[1] != "*/*" {
			t.Fatalf("a query's headers leaked into another: accept = %q", h[1])
		}
	}
}

func fmtList(s []string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
