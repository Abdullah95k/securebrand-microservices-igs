package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	coveragev1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/coverage/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/buildinfo"
)

// sharedCounters are the contract's counter names; igs's others are
// published as "ig.<name>".
var sharedCounters = []string{"requests", "bytes", "posts", "comments", "replies", "blocked", "page_loads"}

// reasonCounters maps igs's counters of missing data to the contract's
// reason classes (coverage.proto, Reason).
var reasonCounters = []struct{ counter, class, detail string }{
	{"posts_gated", "wall", "posts behind the login wall"},
	{"login_wall", "login_required", "pages that served the login page"},
	{"profiles_private", "private", "private profiles"},
	{"profiles_unavailable", "removed", "profiles not found"},
	{"posts_unavailable", "removed", "posts not found"},
	{"timelines_truncated", "limit", "timelines cut short"},
	{"timelines_capped", "limit", "timelines stopped at the job's cap"},
	{"posts_capped", "limit", "posts beyond the job's cap"},
	{"posts_truncated", "limit", "posts whose comments were cut short"},
	{"blocked", "rate_limited", "requests blocked or rate limited"},
}

// jobCoverage is a finished job's collection.coverage.v1 event (REQ-019
// slice 4, item 4.2), keyed by the job ID, beside igs's own ig.job.v1.
func jobCoverage(id, kind, final, ref string, userPK *int64, created, finished time.Time, counters map[string]int64,
	comp *commonv1.Completeness) *coveragev1.JobCoverage {
	sum := sha256.Sum256([]byte("coverage|" + id + "|" + final))
	ev := &coveragev1.JobCoverage{EventId: hex.EncodeToString(sum[:]), Platform: normalize.Platform, Collector: "igs", JobId: id,
		Kind: coverageKind(kind), Status: coverageStatus(final), SubjectRef: ref, CreatedAt: timestamppb.New(created),
		FinishedAt: timestamppb.New(finished), Counters: map[string]int64{}, JobCount: 1,
		Provenance: &contentv1.Provenance{Collector: "igs", CollectorVersion: buildinfo.Version(), JobIds: []string{id},
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT}}
	if userPK != nil && *userPK != 0 {
		ev.SubjectId = normalize.ID(normalize.KindAccount, strconv.FormatInt(*userPK, 10))
	}
	for k, v := range counters {
		if slices.Contains(sharedCounters, k) {
			ev.Counters[k] = v
		} else {
			ev.Counters["ig."+k] = v
		}
	}
	if comp != nil {
		ev.Completeness = &coveragev1.Completeness{Posts: comp.Posts, PostsComplete: comp.PostsComplete,
			PostsTruncated: comp.PostsTruncated, CommentsReported: comp.CommentsReported, CommentsCollected: comp.CommentsCollected}
		// The ratios mean something only when the job read posts.
		if comp.Posts > 0 {
			median, minimum := comp.MedianRatio, comp.MinRatio
			ev.Completeness.MedianRatio, ev.Completeness.MinRatio = &median, &minimum
		}
	}
	for _, r := range reasonCounters {
		if n := counters[r.counter]; n > 0 {
			ev.Reasons = append(ev.Reasons, &coveragev1.Reason{Class: r.class, Count: n, Detail: r.detail})
		}
	}
	return ev
}

func coverageKind(kind string) coveragev1.JobKind {
	switch kind {
	case "scheduled":
		return coveragev1.JobKind_JOB_KIND_SCHEDULED
	case "backfill":
		return coveragev1.JobKind_JOB_KIND_BACKFILL
	case "on_demand":
		return coveragev1.JobKind_JOB_KIND_ON_DEMAND
	case "refresh":
		return coveragev1.JobKind_JOB_KIND_REFRESH
	case "discovery":
		return coveragev1.JobKind_JOB_KIND_DISCOVERY_CHECK
	}
	return coveragev1.JobKind_JOB_KIND_UNSPECIFIED
}

func coverageStatus(final string) coveragev1.JobStatus {
	switch final {
	case "completed":
		return coveragev1.JobStatus_JOB_STATUS_COMPLETED
	case "partial":
		return coveragev1.JobStatus_JOB_STATUS_PARTIAL
	case "failed":
		return coveragev1.JobStatus_JOB_STATUS_FAILED
	case "cancelled":
		return coveragev1.JobStatus_JOB_STATUS_CANCELLED
	}
	return coveragev1.JobStatus_JOB_STATUS_UNSPECIFIED
}
