package fakeig

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// With Admin, a server answers the soak driver: its profiles, random world
// changes, and every profile's ground truth. Without it, nothing is there.
func TestAdmin(t *testing.T) {
	w := NewWorld(Spec{Seed: 3, Profiles: 4, PostsPerProfile: 6, MaxComments: 5, PostSpacing: 12 * time.Hour})
	srv := httptest.NewServer(NewServer(w, Options{Admin: true}))
	defer srv.Close()
	get := func(path string, method string, out any) int {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if out != nil && resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode
	}
	var names []string
	if code := get(AdminPrefix+"profiles", http.MethodGet, &names); code != http.StatusOK || len(names) != 4 || names[0] != "profile_01" {
		t.Fatalf("profiles: %d %v", code, names)
	}
	since := Epoch.Add(-365 * 24 * time.Hour).Format(time.RFC3339)
	var before []ProfileTruth
	get(AdminPrefix+"truth?since="+since, http.MethodGet, &before)
	posts := func(ts []ProfileTruth) (n int, comments int64, likes int64) {
		for _, p := range ts {
			for _, pt := range p.Posts {
				n++
				comments += pt.CommentCount
				if pt.Likes != nil {
					likes += *pt.Likes
				}
			}
		}
		return
	}
	n0, c0, l0 := posts(before)
	var changes map[string]int
	if code := get(AdminPrefix+"mutate?n=60", http.MethodPost, &changes); code != http.StatusOK ||
		changes["new posts"] == 0 || changes["comments on recent posts"] == 0 || changes["like counts"] == 0 {
		t.Fatalf("mutate: %d %v", code, changes)
	}
	var after []ProfileTruth
	get(AdminPrefix+"truth?since="+since, http.MethodGet, &after)
	n1, c1, l1 := posts(after)
	if n1 != n0+changes["new posts"] || c1 <= c0 || l1 <= l0 {
		t.Errorf("truth after %v: posts %d -> %d, comments %d -> %d, likes %d -> %d", changes, n0, n1, c0, c1, l0, l1)
	}
	if code := get(AdminPrefix+"truth?since=yesterday", http.MethodGet, nil); code != http.StatusBadRequest {
		t.Errorf("a bad since: %d", code)
	}

	plain := httptest.NewServer(NewServer(w, Options{}))
	defer plain.Close()
	resp, err := http.Get(plain.URL + AdminPrefix + "profiles")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Error("a server without Admin answered the admin path")
	}
}

// A world change must not hold the server's lock while it waits for the
// world's: a GraphQL request holds the world's read lock and then needs
// the server's (its identity), so the two would wait for each other and
// every later request would queue behind them. The soak's fakeig froze
// that way under the crawlers' load (7 Nov 2026), until it was OOM-killed.
func TestMutateDoesNotDeadlockWithRequests(t *testing.T) {
	w := NewWorld(Spec{Seed: 3, Profiles: 4, PostsPerProfile: 6, MaxComments: 5, PostSpacing: 12 * time.Hour})
	s := NewServer(w, Options{Admin: true})
	srv := httptest.NewServer(s)
	defer srv.Close()

	// A GraphQL request in flight: it holds the world's read lock.
	w.mu.RLock()
	released := false
	release := func() {
		if !released {
			released = true
			w.mu.RUnlock()
		}
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		resp, err := http.Post(srv.URL+AdminPrefix+"mutate?n=50", "", nil)
		if err == nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // the change now waits for the world's write lock

	// The request now looks up its identity, under the server's lock.
	got := make(chan struct{})
	go func() {
		_ = s.identByCSRF("csrffake000001")
		close(got)
	}()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		release()
		<-got
		t.Fatal("deadlock: the world change holds the server's lock while it waits for the world's write lock")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A server says when it started, so the soak driver can tell that fakeig
// restarted and generated its world again.
func TestAdminInstance(t *testing.T) {
	w := NewWorld(Spec{Seed: 3, Profiles: 2, PostsPerProfile: 2, PostSpacing: time.Hour})
	started := func(s *Server) string {
		t.Helper()
		srv := httptest.NewServer(s)
		defer srv.Close()
		resp, err := http.Get(srv.URL + AdminPrefix + "instance")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var v struct{ Started string }
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil || v.Started == "" {
			t.Fatalf("instance: %v %+v", err, v)
		}
		return v.Started
	}
	first := NewServer(w, Options{Admin: true})
	if a, b := started(first), started(first); a != b {
		t.Errorf("the same server answered %s, then %s", a, b)
	}
	time.Sleep(time.Millisecond)
	if a, b := started(first), started(NewServer(w, Options{Admin: true})); a == b {
		t.Errorf("a restarted server answered the same start %s", a)
	}
}
