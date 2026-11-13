package fbcand

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func TestLinks(t *testing.T) {
	bio := "Official page: facebook.com/Example.Shop. Also https://www.facebook.com/example.shop/ and fb.me/exshop, " +
		"https://l.facebook.com/l.php?u=x and https://facebook.com/ and example.com/facebook.com"
	links := `[{"url":"https://m.facebook.com/profile.php?id=100064&ref=bio","title":"FB"},{"url":"https://example.com/shop"}]`
	got := Links(bio, links, "https://web.facebook.com/example.shop?ref=site", "112233")
	want := []string{"facebook.com/Example.Shop", "fb.me/exshop", "https://m.facebook.com/profile.php?id=100064&ref=bio",
		"https://www.facebook.com/112233"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Links:\n got %q\nwant %q", got, want)
	}
	for raw, key := range map[string]string{
		"https://www.Facebook.com/Example.Shop/":        "facebook.com/example.shop",
		"facebook.com/example.shop?ref=bio#x":           "facebook.com/example.shop",
		"https://m.facebook.com/profile.php?id=7&ref=x": "facebook.com/profile.php?id=7",
		"https://fb.me/abc":                             "fb.me/abc",
	} {
		if k := Key(raw); k != key {
			t.Errorf("Key(%q) = %q, want %q", raw, k, key)
		}
	}
	if Links("no links here", "", "", "") != nil {
		t.Error("a profile without Facebook links has some")
	}
}

type fbs struct {
	mu     sync.Mutex
	status int
	got    []map[string]string
	auth   []string
}

func (f *fbs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path != Path || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var b map[string]string
	_ = json.NewDecoder(r.Body).Decode(&b)
	f.got = append(f.got, b)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"result":"queued"}`))
}

func (f *fbs) take() []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.got
	f.got = nil
	return out
}

// Each link is sent once, with the profile as evidence (not a private
// one's); again only when that account's bio changes; never for an
// account off the schedule. A 503 pauses sending and keeps the link; the
// day's ceiling holds.
func TestSender(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO profile_schedule (profile_ref, user_pk, next_run_at) VALUES ('u:shop', 1, now()), ('u:closed', 2, now()),
		('u:off', 3, now())`)
	exec(`UPDATE profile_schedule SET enabled = false WHERE user_pk = 3`)
	exec(`INSERT INTO profiles (user_pk, username, author_key, biography, private, last_profile_read_at) VALUES
		(1, 'shop', '\x01', 'Find us: facebook.com/shop.page', false, now() - interval '3 minutes'),
		(2, 'closed', '\x02', 'https://www.facebook.com/closed.page', true, now() - interval '2 minutes'),
		(3, 'off', '\x03', 'facebook.com/off.page', false, now() - interval '1 minute'),
		(4, 'unscheduled', '\x04', 'facebook.com/other.page', false, now())`)
	srv := &fbs{}
	hs := httptest.NewServer(srv)
	defer hs.Close()
	clock := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s := &Sender{DB: db, HTTP: hs.Client(), Now: func() time.Time { return clock },
		Cfg: config.IGFBCandidates{Enabled: true, URL: hs.URL, Key: "test-key", PerDay: 3, Interval: time.Minute,
			Backoff: 30 * time.Minute, Timeout: 5 * time.Second}}
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	got := srv.take()
	want := []map[string]string{{"ref": "facebook.com/shop.page", "source": "igs", "evidence_url": "https://www.instagram.com/shop/"},
		{"ref": "https://www.facebook.com/closed.page", "source": "igs"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	if srv.auth[0] != "Bearer test-key" {
		t.Errorf("Authorization %q", srv.auth[0])
	}
	// Read again unchanged: nothing more.
	exec(`UPDATE profiles SET last_profile_read_at = now() WHERE user_pk = 1`)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := srv.take(); len(got) != 0 {
		t.Fatalf("an unchanged link was sent again: %v", got)
	}
	// The bio changes and adds a link while fbs is off: both wait, and
	// sending pauses.
	srv.status = http.StatusServiceUnavailable
	exec(`UPDATE profiles SET biography = 'facebook.com/shop.page and fb.me/shop2', last_profile_read_at = now() + interval '1 second' WHERE user_pk = 1`)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := srv.take(); len(got) != 1 {
		t.Fatalf("while fbs is off: %d sends, want 1 then a pause", len(got))
	}
	srv.status = 0
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := srv.take(); len(got) != 0 {
		t.Fatalf("sent %v during the pause", got)
	}
	// After the pause, the day's ceiling (3) leaves room for one.
	clock = clock.Add(31 * time.Minute)
	if err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := srv.take(); len(got) != 1 || got[0]["ref"] != "facebook.com/shop.page" {
		t.Fatalf("after the pause: %v, want the changed account's first link alone", got)
	}
	var pending int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM fb_candidates WHERE pending`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Errorf("%d links pending, want 1 (over the day's ceiling)", pending)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM fb_candidates WHERE user_pk IN (3, 4)`).Scan(&n); err != nil || n != 0 {
		t.Errorf("links of accounts off the schedule: %d (%v)", n, err)
	}
}
