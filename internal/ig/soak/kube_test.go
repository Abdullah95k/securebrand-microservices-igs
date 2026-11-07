package soak

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Kube lists the crawler pods by label, deletes a running one (never one
// already terminating), and restarts Valkey with a pod-template annotation,
// always with the service account's token and in its own namespace.
func TestKube(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer sa-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		calls = append(calls, r.Method+" "+r.URL.Path+" "+r.URL.Query().Get("labelSelector")+" "+r.Header.Get("Content-Type")+" "+string(b))
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/igs-soak/pods":
			_, _ = io.WriteString(w, `{"items":[
				{"metadata":{"name":"igs-crawler-post-a"},"status":{"phase":"Running"}},
				{"metadata":{"name":"igs-crawler-post-b","deletionTimestamp":"2026-11-07T00:00:00Z"},"status":{"phase":"Running"}},
				{"metadata":{"name":"igs-crawler-profile-c"},"status":{"phase":"Pending"}}]}`)
		case r.Method == http.MethodDelete || r.Method == http.MethodPatch:
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	k := &Kube{Base: srv.URL, Namespace: "igs-soak", Token: "sa-token", Crawlers: "app.kubernetes.io/component in (crawler-post)",
		Valkey: "valkey", HTTP: srv.Client(), Rand: rand.New(rand.NewPCG(1, 2))}
	name, err := k.KillCrawler(context.Background())
	if err != nil || name != "igs-crawler-post-a" {
		t.Fatalf("killed %q, %v", name, err)
	}
	if err := k.RestartValkey(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"GET /api/v1/namespaces/igs-soak/pods app.kubernetes.io/component in (crawler-post)",
		"DELETE /api/v1/namespaces/igs-soak/pods/igs-crawler-post-a",
		"PATCH /apis/apps/v1/namespaces/igs-soak/statefulsets/valkey  application/strategic-merge-patch+json {\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"igs-soak/restarted-at\":",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls %q", calls)
	}
	for i, w := range want {
		if !strings.HasPrefix(calls[i], w) {
			t.Errorf("call %d: %q, want %q...", i, calls[i], w)
		}
	}
	k.Token = "wrong"
	if _, err := k.KillCrawler(context.Background()); err == nil {
		t.Error("a refused call reported no error")
	}
}
