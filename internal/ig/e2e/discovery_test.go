package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
)

// Profile discovery (M11): two registered profiles co-author with and tag
// other accounts. Each account met is vetted with one profile read; the
// Iraqi ones (an Arabic photographer in Baghdad, a Kurdish one in Erbil)
// are registered and backfilled, an English account stays below the
// threshold, a private one is not registered, and accounts that do not
// exist are gone. A vet publishes nothing: the accounts not registered
// leave no profile in the store. What every vet read is kept, on the
// candidate and in discovery_vets. An admin lists the candidates with
// their signals and reads, and a rejected one leaves the schedule.
func TestDiscoveryRegistersIraqiProfiles(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags", "coauthors", "mentions", "commenters"},
			Threshold: 4, MinPriority: 2, VetInFlight: 40, Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond}
	}})
	// fakeig names profile_0N's co-author collab_0N and its tagged accounts
	// tagged_0N_<post>.
	s.World.AddProfile(fakeig.ProfileSpec{Username: "collab_01", FullName: "مصور بغداد",
		Bio: "مصور فوتوغرافي في بغداد 🇮🇶 للحجز +964 770 123 4567", Followers: 5000,
		Captions: []string{"شلونكم حبايبي هسه نزلت الصور", "جلسة تصوير في شارع المتنبي"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "collab_02", FullName: "Hawre", Bio: "وێنەگر لە هەولێر", Followers: 2000,
		Captions: []string{"ئێوارەیەکی جوان لە شار"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_0", FullName: "Coffee Lab", Bio: "Specialty coffee in London",
		Followers: 800, Captions: []string{"new beans", "latte art"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_3", FullName: "سارة", Bio: "من البصرة", Private: true})
	// An Iraqi creator whose profile says nothing of Iraq: below the
	// threshold until more registered accounts link to it.
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_02_0", FullName: "نور", Bio: "صانعة محتوى للتعاون راسلوني", Followers: 90000,
		Captions: []string{"اطلالة اليوم", "يوم جميل"}})
	s.StartScheduler()
	ctx := context.Background()

	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	for _, p := range s.World.Profiles()[:2] {
		if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, nil); code != http.StatusCreated {
			t.Fatalf("register %s: %d", p.Username, code)
		}
	}
	want := map[string]string{"collab_01": "registered", "collab_02": "registered", "tagged_01_0": "below",
		"tagged_01_3": "private", "tagged_01_6": "gone", "tagged_02_0": "below"}
	status := func(u string) string {
		var st string
		_ = s.DB.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = $1`, u).Scan(&st)
		return st
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		done := true
		for u, st := range want {
			if status(u) != st {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			got := map[string]string{}
			for u := range want {
				got[u] = status(u)
			}
			t.Fatalf("candidates %v, want %v", got, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE comments > 0`); n == 0 {
		t.Error("no commenter was counted")
	}
	// Each candidate's audience is the registered accounts it was met in:
	// collab_01 co-authors with profile_01 alone; commenters seen on both
	// profiles count two.
	if n := count(t, s, `SELECT sources FROM discovery_candidates WHERE username = 'collab_01'`); n != 1 {
		t.Errorf("collab_01 met in %d registered accounts, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates c WHERE sources >= 2
		AND sources = (SELECT count(*) FROM discovery_sources d WHERE d.username = c.username)`); n == 0 {
		t.Error("no candidate counted two registered accounts")
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates c JOIN profile_schedule p ON p.user_pk = c.user_pk
		WHERE p.profile_ref IN ('u:profile_01', 'u:profile_02')`); n != 0 {
		t.Errorf("%d registered profiles counted as candidates of their own posts", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE vet IS NULL AND status IN ('registered','below','private','gone')`); n != 0 {
		t.Errorf("%d decided candidates lost what their vet read", n)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_vets v JOIN discovery_candidates c USING (username)
		WHERE c.username IN ('collab_01', 'collab_02', 'tagged_01_0', 'tagged_01_3', 'tagged_01_6') AND v.status = c.status`); n != 5 {
		t.Errorf("%d of 5 decisions kept in discovery_vets", n)
	}
	var bio string
	if err := s.DB.QueryRow(ctx, `SELECT vet->>'biography' FROM discovery_vets WHERE username = 'tagged_01_0'`).Scan(&bio); err != nil || bio == "" {
		t.Errorf("the below-threshold account's read was not kept: %q %v", bio, err)
	}
	if n := count(t, s, `SELECT count(*) FROM profiles WHERE username IN ('tagged_01_0', 'tagged_01_3', 'tagged_01_6')`); n != 0 {
		t.Errorf("a vet published %d profiles of accounts not registered", n)
	}

	// tagged_02_0's audience grows (twelve registered accounts, posts
	// located in Iraq): scored again from its stored read, with no new read
	// of Instagram, it is registered.
	vets := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discovery' AND profile_ref = 'u:tagged_02_0'`)
	if _, err := s.DB.Exec(ctx, `UPDATE discovery_candidates SET sources = 12, iraq_context = 2 WHERE username = 'tagged_02_0'`); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Minute); status("tagged_02_0") != "registered"; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("tagged_02_0 not registered on a grown audience: %s", status("tagged_02_0"))
		}
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'discovery' AND profile_ref = 'u:tagged_02_0'`); n != vets {
		t.Errorf("the rescore read Instagram again: %d vet jobs, was %d", n, vets)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_vets WHERE username = 'tagged_02_0'`); n != 2 {
		t.Errorf("tagged_02_0 has %d kept decisions, want 2 (below, then registered)", n)
	}

	// The discovered profiles are on the schedule and backfilled.
	for _, u := range []string{"collab_01", "collab_02"} {
		var job string
		if err := s.DB.QueryRow(ctx, `SELECT coalesce(last_job_id::text, '') FROM profile_schedule WHERE profile_ref = $1`, "u:"+u).
			Scan(&job); err != nil || job == "" {
			t.Fatalf("%s not on the schedule: %v", u, err)
		}
		if st := s.WaitStatus(t, job, 2*time.Minute); st != "completed" {
			t.Fatalf("%s backfill %s", u, st)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM posts p JOIN profiles f ON f.user_pk = p.owner_pk WHERE f.username = 'collab_01'`); n != 2 {
		t.Errorf("collab_01's backfill stored %d posts, want 2", n)
	}

	admin := s.Key(t, httpapi.ScopeAdmin)
	var list struct {
		Candidates []api.Candidate `json:"candidates"`
	}
	if code := s.Do(t, admin, "GET", "/v1/discovery/candidates?status=registered", nil, &list); code != http.StatusOK {
		t.Fatalf("list: %d", code)
	}
	if len(list.Candidates) != 3 || list.Candidates[0].Score == nil || *list.Candidates[0].Score < 4 || len(list.Candidates[0].Signals) == 0 ||
		len(list.Candidates[0].Read) == 0 {
		t.Fatalf("registered candidates %+v", list.Candidates)
	}
	if code := s.Do(t, key, "GET", "/v1/discovery/candidates", nil, nil); code != http.StatusForbidden {
		t.Fatalf("candidates without admin: %d", code)
	}

	// Rejected: off the schedule, and not discovered again.
	if code := s.Do(t, admin, "POST", "/v1/discovery/candidates/collab_02/reject", nil, nil); code != http.StatusNoContent {
		t.Fatalf("reject: %d", code)
	}
	if n := count(t, s, `SELECT count(*) FROM profile_schedule WHERE profile_ref = 'u:collab_02'`); n != 0 || status("collab_02") != "rejected" {
		t.Fatalf("collab_02 rejected: %d schedules, status %s", n, status("collab_02"))
	}
}
