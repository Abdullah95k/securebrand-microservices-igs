package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/buildinfo"
)

// CandidatePublisher publishes a decided candidate on discovery.candidate.v1
// (REQ-019 item 4.1). The scheduler sets it; nil publishes nothing.
type CandidatePublisher func(ctx context.Context, key string, c *discoveryv1.Candidate) error

// outcomes maps a decided candidate's status to the contract's outcome,
// its reason and what igs collects. Capped is left out: the contract allows
// it only in tests and the canary, and production runs no caps (D-008).
var outcomes = map[string]struct {
	outcome discoveryv1.Outcome
	reason  string
	scope   contentv1.CollectionScope
}{
	StatusRegistered: {discoveryv1.Outcome_OUTCOME_ADDED, "", contentv1.CollectionScope_COLLECTION_SCOPE_FULL},
	StatusBelow:      {discoveryv1.Outcome_OUTCOME_NOT_RELEVANT, "not_relevant", 0},
	StatusRejected:   {discoveryv1.Outcome_OUTCOME_NOT_RELEVANT, "not_relevant", 0},
	StatusPrivate:    {discoveryv1.Outcome_OUTCOME_SECOND_LIST, "private", contentv1.CollectionScope_COLLECTION_SCOPE_PROFILE_ONLY},
	StatusGone:       {discoveryv1.Outcome_OUTCOME_UNAVAILABLE, "gone", 0},
	StatusFailed:     {discoveryv1.Outcome_OUTCOME_UNAVAILABLE, "unreadable", 0},
}

// CandidateEvent is an Instagram candidate igs checked, with its outcome.
// ok is false for statuses the contract doesn't publish. vet is nil when
// the check never read the profile.
func CandidateEvent(username string, pk int64, vet *Vet, s Sightings, origins []string, status string, score float64,
	relevant bool, firstSeen, decided time.Time) (key string, c *discoveryv1.Candidate, ok bool) {
	o, ok := outcomes[status]
	if !ok {
		return "", nil, false
	}
	ref := &contentv1.AccountRef{Platform: "ig", Handle: username, Url: "https://www.instagram.com/" + username + "/"}
	key = "ig:u:" + username
	if pk != 0 {
		ref.NativeId = strconv.FormatInt(pk, 10)
		ref.Id = "ig:account:" + ref.NativeId
		key = ref.Id
	}
	sum := sha256.Sum256([]byte("candidate|" + key + "|" + status + "|" + decided.UTC().Format(time.RFC3339Nano)))
	c = &discoveryv1.Candidate{EventId: hex.EncodeToString(sum[:]), TargetPlatform: "ig", Account: ref,
		FoundAt: timestamppb.New(firstSeen), FoundBy: "igs", Source: source(s, origins), Evidence: evidence(s),
		Outcome: o.outcome, OutcomeReason: o.reason, Scope: o.scope, DecidedAt: timestamppb.New(decided),
		Provenance: &contentv1.Provenance{Collector: "igs", CollectorVersion: buildinfo.Version(),
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT}}
	if vet != nil && vet.Found {
		private := vet.Private
		c.IsPrivate = &private
		c.Relevance = &contentv1.Relevance{Relevant: &relevant, Score: &score,
			MatchedRules: CountrySignals(vet.FullName, vet.Biography, vet.Category, "", vet.Links)}
	}
	return key, c, true
}

// source is how the crawl met the candidate most often; an imported one
// (igsctl discovery import, from Google or Facebook) is an import.
func source(s Sightings, origins []string) discoveryv1.Source {
	best, n := discoveryv1.Source_SOURCE_UNSPECIFIED, 0
	for _, k := range []struct {
		src discoveryv1.Source
		n   int
	}{{discoveryv1.Source_SOURCE_TAG, s.Tags}, {discoveryv1.Source_SOURCE_COAUTHOR, s.Coauthors},
		{discoveryv1.Source_SOURCE_MENTION, s.Mentions}, {discoveryv1.Source_SOURCE_COMMENTER, s.Comments}} {
		if k.n > n {
			best, n = k.src, k.n
		}
	}
	if best == discoveryv1.Source_SOURCE_UNSPECIFIED && len(origins) > 0 {
		return discoveryv1.Source_SOURCE_IMPORT
	}
	return best
}

// evidence is one entry per way the crawl met the candidate, with how
// often and by how many registered accounts.
func evidence(s Sightings) []*discoveryv1.Evidence {
	var out []*discoveryv1.Evidence
	for _, k := range []struct {
		via string
		n   int
	}{{"tag", s.Tags}, {"coauthor", s.Coauthors}, {"mention", s.Mentions}, {"comment", s.Comments}, {"iraq_location", s.IraqContext}} {
		if k.n > 0 {
			out = append(out, &discoveryv1.Evidence{Via: k.via, Sightings: int64(k.n), DistinctSources: int64(s.Sources)})
		}
	}
	return out
}
