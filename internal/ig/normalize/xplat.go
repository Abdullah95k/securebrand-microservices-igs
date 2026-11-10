package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// candidatePlatforms are the platforms whose collectors take candidates
// (registry/platforms.yaml, status active), Instagram aside.
var candidatePlatforms = map[string]bool{"fb": true, "tt": true, "yt": true}

// linkVia names how an Instagram profile linked an account elsewhere.
var linkVia = map[contentv1.LinkSource]string{
	contentv1.LinkSource_LINK_SOURCE_BIO:            "bio_link",
	contentv1.LinkSource_LINK_SOURCE_WEBSITE_FIELD:  "website_field",
	contentv1.LinkSource_LINK_SOURCE_LINKED_ACCOUNT: "linked_account",
}

// linkCandidates turns a profile's links to Facebook, TikTok and YouTube
// into candidates for those platforms' collectors (REQ-019 item 4.1, "a
// link found on one platform becomes a candidate for another"), keyed by
// the target platform and the link. Each is pending: the other collector
// decides. The volume rule passes each at most once a day.
func linkCandidates(p *igv1.Profile, links []*contentv1.Link) []Out {
	var out []Out
	seen := map[string]bool{}
	found := p.GetObs().GetObservedAt()
	for _, l := range links {
		if !candidatePlatforms[l.TargetPlatform] || (l.Url == "" && l.TargetId == "") {
			continue
		}
		ref := &contentv1.AccountRef{Platform: l.TargetPlatform, Url: l.Url, NativeId: l.TargetId}
		key := l.TargetPlatform + ":url:" + l.Url
		if l.TargetId != "" {
			key = l.TargetPlatform + ":id:" + l.TargetId
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		day := time.Now().UTC().Format(time.DateOnly)
		if found != nil {
			day = found.AsTime().UTC().Format(time.DateOnly)
		}
		sum := sha256.Sum256([]byte("candidate|" + key + "|" + pk(p.UserPk) + "|" + day))
		pv := envelope(p.Obs, KindAccount, KindAccount, pk(p.UserPk)).Provenance
		out = append(out, Out{Topic: topics.DiscoveryCandidate, Key: key, Msg: &discoveryv1.Candidate{
			EventId: hex.EncodeToString(sum[:]), TargetPlatform: l.TargetPlatform, Account: ref, FoundAt: found, FoundBy: "igs",
			SourcePlatform: Platform, Source: discoveryv1.Source_SOURCE_PROFILE_LINK, Outcome: discoveryv1.Outcome_OUTCOME_PENDING,
			Evidence: []*discoveryv1.Evidence{{ContentId: ID(KindAccount, pk(p.UserPk)), Url: ProfileURL(p.Username),
				Via: linkVia[l.Source], Sightings: 1}},
			Provenance: pv,
		}})
	}
	return out
}
