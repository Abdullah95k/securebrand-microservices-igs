package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
	igdetect "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/igtest"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/browser"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

func bootstrapper(t *testing.T) *browser.Bootstrapper {
	t.Helper()
	if _, err := browser.FindBinary(""); err != nil {
		t.Skip(err)
	}
	b, err := browser.NewBootstrapper(config.Browser{MaxConcurrent: 1, MaxShare: 0.5, Timeout: time.Minute}, "en-US,en;q=0.9", igclient.Hooks())
	if err != nil {
		t.Fatal(err)
	}
	b.Rules, b.CaptureFilter = igdetect.Rules(), browser.IsInstagramGraphQL
	return b
}

// A profile page whose tokens only a script writes: the HTTP bootstrap
// fails, the browser fallback with Instagram's hooks bootstraps the
// identity, and queries work with its tokens.
func TestBrowserBootstrapFallback(t *testing.T) {
	b := bootstrapper(t)
	env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{JSOnlyTokens: true}})
	ctx := context.Background()
	page := env.HTTP.URL + "/profile_01/"
	id, _ := env.Pool.Acquire(ctx, "direct")
	if _, err := env.Loader.Bootstrap(ctx, id, page); !errors.Is(err, session.ErrBootstrap) {
		t.Fatalf("without the browser the bootstrap must fail: %v", err)
	}
	env.Pool.Release(id)
	env.Loader.Browser = b
	id, _ = env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	pl, err := env.Loader.Bootstrap(ctx, id, page)
	if err != nil || !id.Bootstrapped() || pl.Class != detect.OK {
		t.Fatalf("browser bootstrap: %v %+v", err, pl)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "profile", Vars: map[string]any{"username": "profile_01"}})
	if err != nil || resp.Class != detect.OK {
		t.Fatalf("profile with browser tokens: %v %v %s", err, resp.Class, resp.Reason)
	}
}

// A render records the page's own GraphQL requests on both of Instagram's
// endpoints: a POST's doc_id from its body, a GET's from its query string
// and its friendly name from the header.
func TestRenderCapturesPostAndGet(t *testing.T) {
	b := bootstrapper(t)
	env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{JSOnlyTokens: true}})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	xs, err := b.Capture(ctx, id, env.HTTP.URL+"/profile_01/")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, x := range xs {
		got[x.FriendlyName] = x.DocID
	}
	if got[fakeig.OpProfile] == "" || got[fakeig.OpHighlights] == "" {
		t.Fatalf("captured %+v", xs)
	}
}

// The browser follows Instagram's wall paths no further: a page load sent
// to /accounts/login is a login wall.
func TestBrowserLoginWall(t *testing.T) {
	b := bootstrapper(t)
	env := igtest.New(t, igtest.Options{Faults: fakeig.Faults{PageWallFirst: 100}})
	ctx := context.Background()
	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	pl, err := b.Bootstrap(ctx, id, env.HTTP.URL+"/profile_01/")
	if pl == nil || pl.Class != detect.LoginWall {
		t.Fatalf("want a login wall: %v %+v", err, pl)
	}
}
