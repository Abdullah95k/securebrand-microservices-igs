package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// An Iraqi account under the follower floor is not dropped (owner, 11 Nov
// 2026): it is kept as small, published as dormant, and vetted again
// after its wait. Once it has grown past the floor, that re-vet registers
// it. An English account stays below and is not re-vetted.
func TestDiscoverySmallAccountsRechecked(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"coauthors", "tags"}, Threshold: 4, MinPriority: 2,
			VetInFlight: 10, Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond, MinFollowers: 10,
			FollowerRechecks: []time.Duration{2 * time.Second}, RecheckShare: 0.1}
	}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "collab_01", FullName: "مصور بغداد", Bio: "مصور في بغداد 🇮🇶", Followers: 5,
		Captions: []string{"شلونكم"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_0", FullName: "Coffee Lab", Bio: "Specialty coffee", Followers: 5,
		Captions: []string{"new beans"}})
	s.StartScheduler()
	ctx := context.Background()
	key := s.Key(t, api.ScopeProfilesWrite)
	for _, p := range s.World.Profiles()[:2] {
		if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, nil); code != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Username, code)
		}
	}
	status := func(u string) string {
		var st string
		_ = s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = $1`, u).Scan(&st)
		return st
	}
	wait := func(u string, want ...string) {
		t.Helper()
		for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(100 * time.Millisecond) {
			st := status(u)
			for _, w := range want {
				if st == w {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %s, want %v", u, st, want)
			}
		}
	}
	// Under the floor: kept as small (or already due for its re-vet).
	wait("collab_01", "small", "recheck", "vetting")
	wait("tagged_01_0", "below")
	// It grows past the floor; its next re-vet registers it.
	if err := s.World.SetFollowers("collab_01", 50); err != nil {
		t.Fatal(err)
	}
	wait("collab_01", "registered")
	if n := count(t, s, `SELECT count(*) FROM discovery_vets WHERE username = 'collab_01' AND status = 'small'`); n < 1 {
		t.Errorf("collab_01's small decisions kept: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM profile_schedule WHERE profile_ref = 'u:collab_01'`); n != 1 {
		t.Errorf("collab_01 on the schedule %d times, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_vets WHERE username = 'tagged_01_0'`); n != 1 {
		t.Errorf("the below account was vetted %d times, want 1", n)
	}
	// Its small decision was published as dormant, then as added.
	seen := map[discoveryv1.Outcome]bool{}
	for _, r := range s.ReadTopic(t, topics.DiscoveryCandidate, 3*time.Second) {
		var c discoveryv1.Candidate
		if err := proto.Unmarshal(r.Value, &c); err != nil {
			t.Fatal(err)
		}
		if c.Account.GetHandle() == "collab_01" {
			seen[c.Outcome] = true
		}
	}
	if !seen[discoveryv1.Outcome_OUTCOME_DORMANT] || !seen[discoveryv1.Outcome_OUTCOME_ADDED] {
		t.Errorf("collab_01 published as %v, want dormant then added", seen)
	}
}
