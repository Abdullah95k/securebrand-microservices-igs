package e2e

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
)

// Instagram keyword discovery, Track 1 (igdisc): a registered profile's
// newest post names Basra, and a comment on it names Erbil. The accounts
// met there (the tagged account, the co-author, the commenter) get the
// keywords as evidence and become candidates with origin "keyword"; an
// account met on an older post naming Baghdad, outside the window, gets
// none, and neither does the profile itself. The vets score the evidence
// as keyword_context (0.5 for one place), which alone registers no one.
// Erasing the profile deletes the evidence found on its posts. Every
// example is synthetic.
func TestIgdiscKeywordEvidence(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Config: func(c *config.Config) {
		c.IG.Discovery = config.IGDiscovery{Enabled: true, Sources: []string{"tags", "coauthors", "mentions", "commenters"},
			Threshold: 4, MinPriority: 2, VetInFlight: 40, Interval: 200 * time.Millisecond, FlushEvery: 100 * time.Millisecond}
		// fakeig's newest post is 12 to 36 hours old; its fourth newest is
		// 54 hours older.
		c.IG.IGDisc = config.IGDisc{Enabled: true, Window: 48 * time.Hour, FlushEvery: 100 * time.Millisecond, Priority: 2}
	}})
	ctx := context.Background()
	p := s.World.Profiles()[0]
	find := func(tagged string) *fakeig.Post {
		for _, post := range p.Posts {
			if slices.Contains(post.Tagged, tagged) {
				return post
			}
		}
		t.Fatalf("no post of %s tags %s", p.Username, tagged)
		return nil
	}
	newest, older := find("tagged_01_0"), find("tagged_01_3")
	if age := time.Since(newest.TakenAt); age > 47*time.Hour {
		t.Fatalf("fakeig's newest post is %s old", age)
	}
	newest.Caption = "جلسة تصوير في البصرة"
	older.Caption = "رحلة الى بغداد"
	newest.Comments = append([]fakeig.Comment{{PK: 7_100_000_000_000_000_001, Text: "تحياتي من اربيل",
		CreatedAt: newest.TakenAt.Add(30 * time.Minute), UserPK: 61_999_000_001, Username: "erbil_fan_01"}}, newest.Comments...)
	s.World.AddProfile(fakeig.ProfileSpec{Username: "tagged_01_0", FullName: "Coffee Lab", Bio: "Specialty coffee", Followers: 800,
		Captions: []string{"new beans"}})
	s.World.AddProfile(fakeig.ProfileSpec{Username: "erbil_fan_01", FullName: "Food Lover", Bio: "food and travel", Followers: 1200,
		Captions: []string{"lunch"}})
	s.StartScheduler()
	s.StartEraser()

	key := s.Key(t, api.ScopeProfilesWrite, httpapi.ScopeDataRead)
	if code := s.Do(t, key, "POST", "/v1/profiles", map[string]any{"profile": p.Username}, nil); code != http.StatusCreated {
		t.Fatalf("register %s: %d", p.Username, code)
	}
	want := map[string]string{"tagged_01_0": "below", "erbil_fan_01": "below", "collab_01": "gone"}
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

	rows, err := s.DB.Query(ctx, `SELECT username || ' ' || keyword_id || ' ' || via FROM igdisc_evidence ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	// The list holds Basra with and without the article: both match.
	wantEvidence := []string{
		"collab_01 basra:ar:البصره caption", "collab_01 basra:ar:بصره caption",
		"erbil_fan_01 erbil:ar:اربیل comment",
		"tagged_01_0 basra:ar:البصره caption", "tagged_01_0 basra:ar:بصره caption",
	}
	if !slices.Equal(got, wantEvidence) {
		t.Fatalf("evidence %v, want %v", got, wantEvidence)
	}
	if n := count(t, s, `SELECT count(*) FROM igdisc_evidence WHERE source_pk = $1 AND content_time > now() - interval '48 hours'`, p.PK); n != 5 {
		t.Errorf("%d pieces of evidence on %s's recent posts, want 5", n, p.Username)
	}
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE username IN ('tagged_01_0', 'erbil_fan_01', 'collab_01')
		AND 'keyword' = ANY(origins) AND 'network' = ANY(origins)`); n != 3 {
		t.Errorf("%d of 3 candidates carry both origins", n)
	}
	// Met on the older post only (outside the window): a network candidate,
	// with no keyword evidence.
	if n := count(t, s, `SELECT count(*) FROM discovery_candidates WHERE username = 'tagged_01_3' AND NOT ('keyword' = ANY(origins))`); n != 1 {
		t.Errorf("tagged_01_3: %d network-only candidates, want 1", n)
	}
	for _, u := range []string{"tagged_01_0", "erbil_fan_01"} {
		var kc, kw string
		if err := s.DB.QueryRow(ctx, `SELECT coalesce(signals->>'keyword_context', ''), coalesce(sightings->>'keywords', '')
			FROM discovery_vets WHERE username = $1`, u).Scan(&kc, &kw); err != nil {
			t.Fatalf("%s's vet: %v", u, err)
		}
		if kc != "0.5" || kw != "1" {
			t.Errorf("%s: keyword_context %q, keywords %q; want 0.5 and 1", u, kc, kw)
		}
	}

	admin := s.Key(t, httpapi.ScopeAdmin)
	var e api.Erasure
	if code := s.Do(t, admin, "POST", "/v1/erasure-requests", map[string]any{"subject_type": "profile", "username": p.Username}, &e); code != http.StatusAccepted {
		t.Fatalf("profile erasure: %d", code)
	}
	waitErasure(t, s, admin, e.ID.String())
	if n := count(t, s, `SELECT count(*) FROM igdisc_evidence WHERE source_pk = $1`, p.PK); n != 0 {
		t.Errorf("%d pieces of evidence found on the erased profile's posts left", n)
	}
}
