package discovery

import (
	"strconv"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"
)

// Each decided candidate in the contract's form (REQ-019 item 4.1), passing
// the production check; capped (canary only) and undecided ones aren't
// published.
func TestCandidateEvent(t *testing.T) {
	seen := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	decided := seen.Add(time.Hour)
	vet := &Vet{Found: true, FullName: "مصور بغداد", Biography: "في بغداد +964 770 123 4567", Followers: 5000}
	s := Sightings{Tags: 1, Coauthors: 3, Sources: 2}
	for _, c := range []struct {
		status  string
		vet     *Vet
		pk      int64
		want    discoveryv1.Outcome
		reason  string
		scope   contentv1.CollectionScope
		private bool
	}{
		{StatusRegistered, vet, 42, discoveryv1.Outcome_OUTCOME_ADDED, "", contentv1.CollectionScope_COLLECTION_SCOPE_FULL, false},
		{StatusBelow, vet, 42, discoveryv1.Outcome_OUTCOME_NOT_RELEVANT, "not_relevant", 0, false},
		{StatusPrivate, &Vet{Found: true, Private: true}, 7, discoveryv1.Outcome_OUTCOME_SECOND_LIST, "private",
			contentv1.CollectionScope_COLLECTION_SCOPE_PROFILE_ONLY, true},
		{StatusGone, &Vet{}, 0, discoveryv1.Outcome_OUTCOME_UNAVAILABLE, "gone", 0, false},
		{StatusFailed, nil, 0, discoveryv1.Outcome_OUTCOME_UNAVAILABLE, "unreadable", 0, false},
	} {
		key, ev, ok := CandidateEvent("collab_01", c.pk, c.vet, s, nil, c.status, 5, c.status == StatusRegistered, seen, decided)
		if !ok {
			t.Fatalf("%s not published", c.status)
		}
		if err := conformance.Candidate(ev, conformance.Options{Production: true}); err != nil {
			t.Errorf("%s: %v", c.status, err)
		}
		wantKey := "ig:u:collab_01"
		if c.pk != 0 {
			wantKey = "ig:account:" + strconv.FormatInt(c.pk, 10)
		}
		if key != wantKey || ev.Account.GetId() != map[bool]string{true: wantKey, false: ""}[c.pk != 0] ||
			ev.Outcome != c.want || ev.OutcomeReason != c.reason || ev.Scope != c.scope || ev.TargetPlatform != "ig" ||
			ev.FoundBy != "igs" || ev.Source != discoveryv1.Source_SOURCE_COAUTHOR || len(ev.Evidence) != 2 ||
			!ev.FoundAt.AsTime().Equal(seen) || !ev.DecidedAt.AsTime().Equal(decided) {
			t.Errorf("%s: key %s %+v", c.status, key, ev)
		}
		if c.vet != nil && c.vet.Found && ev.GetIsPrivate() != c.private {
			t.Errorf("%s: is_private %v", c.status, ev.GetIsPrivate())
		}
	}
	_, ev, _ := CandidateEvent("collab_01", 42, vet, s, nil, StatusRegistered, 5, true, seen, decided)
	if r := ev.Relevance; r.GetScore() != 5 || !r.GetRelevant() || len(r.MatchedRules) == 0 {
		t.Errorf("relevance %+v", r)
	}
	if _, imported, _ := CandidateEvent("x", 1, vet, Sightings{}, []string{"google"}, StatusBelow, 0, false, seen, decided); imported.Source !=
		discoveryv1.Source_SOURCE_IMPORT {
		t.Errorf("an imported candidate's source is %v", imported.Source)
	}
	for _, st := range []string{StatusCapped, StatusNew, StatusVetting} {
		if _, _, ok := CandidateEvent("x", 1, vet, s, nil, st, 0, false, seen, decided); ok {
			t.Errorf("%s published", st)
		}
	}
}
