// Package topics names every Kafka topic igs produces or consumes (ADR
// 0005 §3). Names get kafka.topic_prefix prepended. None of them is one of
// fbs's (ADR 0004 §4): public topics are ig.*, internal ones igs.*.
package topics

import (
	"fmt"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
)

// Public contract.
const (
	Profile        = "ig.profile.v1"
	Post           = "ig.post.v1"
	PostMetrics    = "ig.post_metrics.v1"
	Comment        = "ig.comment.v1"
	Media          = "ig.media.v1"
	Highlight      = "ig.highlight.v1"
	MediaReady     = "ig.media.ready.v1"
	MediaProcessed = "ig.media.processed.v1"
	MediaDeleted   = "ig.media.deleted.v1"
	Job            = "ig.job.v1"
	Erasure        = "ig.erasure.v1"
)

// Internal work queues.
const (
	TaskMedia = "igs.task.media"
	Retry30s  = "igs.task.retry.30s"
	Retry5m   = "igs.task.retry.5m"
	DLQ       = "igs.task.dlq"
	// PersistDLQ holds entity records the persister could not write.
	PersistDLQ = "igs.persist.dlq"
)

// Entity lists the public entity topics the persister consumes; the job
// finalizer waits for the persister to catch up on exactly these.
var Entity = []string{Profile, Post, PostMetrics, Comment, Media, Highlight}

// RetryTopics are consumed by the retry relay.
var RetryTopics = []string{Retry30s, Retry5m}

// Lanes in priority order.
var Lanes = []igstaskv1.Lane{igstaskv1.Lane_LANE_P0, igstaskv1.Lane_LANE_P1, igstaskv1.Lane_LANE_P2}

// TaskTopic returns the work topic for a task kind and lane, and false for
// a kind with no topic (a task from another producer).
func TaskTopic(kind igstaskv1.Kind, lane igstaskv1.Lane) (string, bool) {
	var stage string
	switch kind {
	case igstaskv1.Kind_KIND_MEDIA:
		return TaskMedia, true
	case igstaskv1.Kind_KIND_PROFILE:
		stage = "profile"
	case igstaskv1.Kind_KIND_POST:
		stage = "post"
	default:
		return "", false
	}
	return fmt.Sprintf("igs.task.%s.%s", stage, LaneName(lane)), true
}

// TaskTopics lists every work topic of the profile and post stages.
func TaskTopics() []string {
	var out []string
	for _, k := range []igstaskv1.Kind{igstaskv1.Kind_KIND_PROFILE, igstaskv1.Kind_KIND_POST} {
		for _, l := range Lanes {
			t, _ := TaskTopic(k, l)
			out = append(out, t)
		}
	}
	return append(out, TaskMedia)
}

// LaneName returns p0/p1/p2.
func LaneName(l igstaskv1.Lane) string {
	switch l {
	case igstaskv1.Lane_LANE_P0:
		return "p0"
	case igstaskv1.Lane_LANE_P2:
		return "p2"
	default:
		return "p1"
	}
}

// ParseLane is the inverse of LaneName.
func ParseLane(s string) igstaskv1.Lane {
	switch s {
	case "p0":
		return igstaskv1.Lane_LANE_P0
	case "p2":
		return igstaskv1.Lane_LANE_P2
	default:
		return igstaskv1.Lane_LANE_P1
	}
}

// ValueSchemas maps each public topic to the proto file (import path in
// api/proto) declaring its value message, for the Schema Registry subjects
// (<topic>-value). Records carry plain protobuf and the message type in the
// igs-type header.
var ValueSchemas = map[string]string{
	Profile:        "securebrand/ig/v1/profile.proto",
	Post:           "securebrand/ig/v1/post.proto",
	PostMetrics:    "securebrand/ig/v1/post.proto",
	Comment:        "securebrand/ig/v1/comment.proto",
	Media:          "securebrand/ig/v1/media.proto",
	Highlight:      "securebrand/ig/v1/highlight.proto",
	MediaReady:     "securebrand/common/v1/media.proto",
	MediaProcessed: "securebrand/common/v1/media.proto",
	MediaDeleted:   "securebrand/common/v1/media.proto",
	Job:            "securebrand/common/v1/job.proto",
	Erasure:        "securebrand/common/v1/erasure.proto",
}
