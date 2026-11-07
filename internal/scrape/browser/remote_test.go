package browser

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// A render in a running Chromium (the chart's browser container, Remote)
// opens a browser context of its own: two renders at once each send only
// their own cookies and the identity's user agent, a render goes through
// its identity's authenticated exit, and no context is left behind.
func TestRenderInARunningChromium(t *testing.T) {
	bin := chromium(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	proc, err := launch(ctx, bin, false, []string{"--no-proxy-server"})
	if err != nil {
		t.Fatal(err)
	}
	defer proc.stop()
	// ws://127.0.0.1:PORT/devtools/browser/ID -> http://127.0.0.1:PORT
	addr := "http://" + strings.SplitN(strings.TrimPrefix(proc.wsURL, "ws://"), "/", 2)[0]

	var mu sync.Mutex
	seen := map[string][]string{} // path -> Cookie headers
	agents := map[string]string{}
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = append(seen[r.URL.Path], r.Header.Get("Cookie"))
		agents[r.URL.Path] = r.Header.Get("User-Agent")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>a page</body></html>")
	}))
	t.Cleanup(page.Close)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, sid := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := Render(ctx, "", Options{URL: page.URL + "/" + sid, Remote: addr, UserAgent: "igs-agent-" + sid,
				AcceptLanguage: "ar-IQ,ar;q=0.9", Cookies: map[string]string{"sid": sid}})
			if err == nil && res.Status != 200 {
				t.Errorf("render %s: status %d", sid, res.Status)
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	for _, sid := range []string{"one", "two"} {
		cookies := seen["/"+sid]
		if len(cookies) == 0 || agents["/"+sid] != "igs-agent-"+sid {
			t.Errorf("render %s: cookies %v, agent %q", sid, cookies, agents["/"+sid])
		}
		for _, c := range cookies {
			if c != "sid="+sid {
				t.Errorf("render %s sent %q: the renders share cookies", sid, c)
			}
		}
	}
	mu.Unlock()

	// Through the identity's authenticated exit.
	px, served := proxy(t, "user-session-1", "s3cret")
	pu := strings.Replace(px.URL, "http://", "http://user-session-1:s3cret@", 1)
	if res, err := Render(ctx, "", Options{URL: page.URL + "/proxied", Remote: addr, Proxy: mustURL(t, pu)}); err != nil ||
		res.Status != 200 || served.Load() == 0 {
		t.Fatalf("through the exit: %v, proxied %d", err, served.Load())
	}

	// Every context was closed.
	c, err := dial(ctx, proc.wsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	var bcs struct {
		IDs []string `json:"browserContextIds"`
	}
	if err := c.call(ctx, "", "Target.getBrowserContexts", nil, &bcs); err != nil {
		t.Fatal(err)
	}
	if len(bcs.IDs) != 0 {
		t.Errorf("%d browser contexts left open", len(bcs.IDs))
	}

	// A Chromium that is not there fails the render at once.
	if _, err := Render(ctx, "", Options{URL: page.URL, Remote: "http://127.0.0.1:1"}); err == nil {
		t.Error("a render with no Chromium at Remote succeeded")
	}
}
