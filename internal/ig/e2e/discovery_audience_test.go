package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
)

// commenters makes n comments by distinct synthetic accounts, the first
// iraqi of them in Iraqi dialect.
func commenters(prefix string, n, iraqi int) []fakeig.CommentSpec {
	var out []fakeig.CommentSpec
	for i := range n {
		text := "nice post"
		if i < iraqi {
			text = "شلونكم حبايبي"
		}
		out = append(out, fakeig.CommentSpec{Username: fmt.Sprintf("%s_%02d", prefix, i), Text: text})
	}
	return out
}

// An account outside Iraq is in scope when 10% or more of its commenters
// are Iraqi (D-055). Only a candidate below the threshold with 10,000
// followers or more has its commenters read: the newest comments on its
// two newest posts with comments. Each commenter counts once, the owner's
// own comments not at all; one is Iraqi by its words or as an account igs
// already schedules. 15% of 40 registers; 5% of 40 stays below; 12
// commenters are too few to decide on; a small account is not read.
func TestDiscoveryIraqiCommenters(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags"}, Threshold: 4, MinPriority: 2, VetInFlight: 40,
			Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond,
			Audience: config.IGDiscoveryAudience{Share: 0.1, MinCommenters: 30, MinFollowers: 10000, Posts: 2, MaxComments: 100}}
	}})
	// 40 distinct commenters on the two newest posts: 4 in Iraqi dialect,
	// one naming Baghdad, and profile_01 (scheduled). A repeat commenter
	// and the owner's own reply do not count. The third post's Iraqi
	// commenters are past the window.
	first := append(commenters("fan_a", 20, 4), fakeig.CommentSpec{Username: "tagged_01_0", Text: "thank you"})
	second := append(commenters("fan_b", 18, 0), fakeig.CommentSpec{Username: "fan_b_city", Text: "تحياتي من بغداد"},
		fakeig.CommentSpec{Username: "profile_01", Text: "great work"}, fakeig.CommentSpec{Username: "fan_a_10", Text: "again"})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_0", FullName: "Style House", Bio: "Fashion and beauty, worldwide shipping",
		Followers: 50000, Captions: []string{"new collection", "summer looks", "behind the scenes"},
		Comments: [][]fakeig.CommentSpec{first, second, commenters("fan_c", 20, 20)}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_3", FullName: "Gadget Store", Bio: "Phones and accessories",
		Followers: 30000, Captions: []string{"unboxing", "review"},
		Comments: [][]fakeig.CommentSpec{commenters("buyer_a", 20, 2), commenters("buyer_b", 20, 0)}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_02_0", FullName: "Travel Diary", Bio: "Photos from the road",
		Followers: 20000, Captions: []string{"sunset", "mountains", "the sea"},
		Comments: [][]fakeig.CommentSpec{commenters("reader", 12, 12)}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_02_3", FullName: "Home Bakery", Bio: "Cakes to order",
		Followers: 5000, Captions: []string{"birthday cake"}, Comments: [][]fakeig.CommentSpec{commenters("guest", 40, 40)}})
	s.StartScheduler()
	ctx := context.Background()

	key := s.Key(t, api.ScopeProfilesWrite)
	for _, p := range s.World.Profiles()[:2] {
		if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, nil); code != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Username, code)
		}
	}
	want := map[string]string{"tagged_01_0": "registered", "tagged_01_3": "below", "tagged_02_0": "below", "tagged_02_3": "below"}
	status := func(u string) string {
		var st string
		_ = s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = $1`, u).Scan(&st)
		return st
	}
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		got, done := map[string]string{}, true
		for u, st := range want {
			if got[u] = status(u); got[u] != st {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("candidates %v, want %v", got, want)
		}
	}
	type read struct {
		share      *float64
		score      float64
		posts, n   int
		hasReadOut bool
	}
	get := func(u string) read {
		var r read
		var posts, n *int
		if err := s.DB.QueryRow(ctx, `SELECT (signals->>$2)::float8, score, (vet->'audience'->>'posts')::int,
				jsonb_array_length(coalesce(vet->'audience'->'commenters', '[]'::jsonb)), vet ? 'audience'
			FROM discovery_candidates WHERE username = $1`, u, discovery.SignalIraqiCommenters).
			Scan(&r.share, &r.score, &posts, &n, &r.hasReadOut); err != nil {
			t.Fatalf("%s: %v", u, err)
		}
		if posts != nil {
			r.posts = *posts
		}
		if n != nil {
			r.n = *n
		}
		return r
	}
	for u, w := range map[string]struct {
		share    float64
		posts, n int
	}{"tagged_01_0": {0.15, 2, 40}, "tagged_01_3": {0.05, 2, 40}, "tagged_02_0": {1, 1, 12}} {
		r := get(u)
		if r.share == nil || *r.share != w.share || r.posts != w.posts || r.n != w.n {
			t.Errorf("%s: share %v of %d commenters on %d posts, want %v of %d on %d", u, r.share, r.n, r.posts, w.share, w.n, w.posts)
		}
		if r.score >= 4 {
			t.Errorf("%s: score %v, want below the threshold (the share is not part of it)", u, r.score)
		}
	}
	if r := get("tagged_02_3"); r.hasReadOut || r.share != nil {
		t.Errorf("an account under 10,000 followers had its commenters read: %+v", r)
	}
	// The registered account is on the schedule.
	if n := count(t, s, `SELECT count(*) FROM profile_schedule WHERE profile_ref = 'u:tagged_01_0'`); n != 1 {
		t.Errorf("tagged_01_0 on the schedule %d times, want 1", n)
	}
}

// A refused comment read never fails the vet (production, 12 Nov 2026:
// vets fell from about 30 an hour to 3 once the commenter test went live).
// With Instagram rate-limiting every comment read, the candidate is still
// decided (below: no commenter sample) and its vet job completes.
func TestDiscoveryAudienceRefusedStillDecides(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Faults: fakeig.Faults{RateLimitFirst: map[string]int{fakeig.OpComments: 1000}},
		Config: func(c *config.Config) {
			c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags"}, Threshold: 4, MinPriority: 2, VetInFlight: 10,
				Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond,
				Audience: config.IGDiscoveryAudience{Share: 0.1, MinCommenters: 30, MinFollowers: 10000, Posts: 2, MaxComments: 100}}
			c.IG.Limits.IncludeComments = false
		}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_0", FullName: "Style House", Bio: "Fashion and beauty, worldwide shipping",
		Followers: 50000, Captions: []string{"new collection", "summer looks"},
		Comments: [][]fakeig.CommentSpec{commenters("fan_a", 20, 10), commenters("fan_b", 20, 10)}})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite)
	if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": s.World.Profiles()[0].Username}, nil); code != http.StatusCreated {
		t.Fatalf("register: %d", code)
	}
	var st string
	for deadline := time.Now().Add(2 * time.Minute); st != "below"; time.Sleep(200 * time.Millisecond) {
		_ = s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = 'tagged_01_0'`).Scan(&st)
		if time.Now().After(deadline) {
			t.Fatalf("tagged_01_0: %q, want below (decided without a commenter sample)", st)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discovery' AND profile_ref = 'u:tagged_01_0' AND status = 'completed'`); n != 1 {
		t.Errorf("%d completed vet jobs, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE username = 'tagged_01_0' AND vet ? 'audience'
		AND jsonb_array_length(coalesce(vet->'audience'->'commenters', '[]'::jsonb)) = 0`); n != 1 {
		t.Errorf("the refused sample was not kept as an empty audience")
	}
}
