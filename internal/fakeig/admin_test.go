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
