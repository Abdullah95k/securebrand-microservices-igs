package scheduler

import (
	"testing"
	"time"

	coveragev1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/coverage/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
)

// A finished job's coverage in the contract's form (REQ-019 item 4.2):
// shared counters as they are, igs's own as "ig.<name>", missing data as
// reason classes, and the ratios only when posts were read.
func TestJobCoverage(t *testing.T) {
	pk := int64(42)
	at := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	ev := jobCoverage("job-1", "scheduled", "partial", "u:example", &pk, at, at.Add(time.Minute),
		map[string]int64{"posts": 12, "comments": 40, "reels": 3, "posts_gated": 2, "timelines_truncated": 1, "blocked": 0},
		&commonv1.Completeness{Posts: 12, PostsComplete: 10, MedianRatio: 0.9, MinRatio: 0.5})
	if ev.Platform != "ig" || ev.Collector != "igs" || ev.JobId != "job-1" || ev.EventId == "" || ev.JobCount != 1 ||
		ev.Kind != coveragev1.JobKind_JOB_KIND_SCHEDULED || ev.Status != coveragev1.JobStatus_JOB_STATUS_PARTIAL ||
		ev.SubjectId != "ig:account:42" || ev.SubjectRef != "u:example" {
		t.Errorf("event %+v", ev)
	}
	if ev.Counters["posts"] != 12 || ev.Counters["comments"] != 40 || ev.Counters["ig.reels"] != 3 || ev.Counters["ig.posts_gated"] != 2 {
		t.Errorf("counters %v", ev.Counters)
	}
	classes := map[string]int64{}
	for _, r := range ev.Reasons {
		classes[r.Class] += r.Count
	}
	if len(classes) != 2 || classes["wall"] != 2 || classes["limit"] != 1 {
		t.Errorf("reasons %v (a zero counter gives none)", classes)
	}
	if ev.Completeness.GetMedianRatio() != 0.9 || ev.Completeness.GetPostsComplete() != 10 {
		t.Errorf("completeness %+v", ev.Completeness)
	}
	if again := jobCoverage("job-1", "scheduled", "partial", "u:example", &pk, at, at, nil, nil); again.EventId != ev.EventId {
		t.Error("the same job's outcome changed its event ID")
	}
	empty := jobCoverage("job-2", "on_demand", "failed", "u:x", nil, at, at, map[string]int64{}, &commonv1.Completeness{})
	if empty.Completeness.MedianRatio != nil || empty.SubjectId != "" || empty.Kind != coveragev1.JobKind_JOB_KIND_ON_DEMAND ||
		empty.Status != coveragev1.JobStatus_JOB_STATUS_FAILED {
		t.Errorf("a job without posts or account: %+v", empty)
	}
}
