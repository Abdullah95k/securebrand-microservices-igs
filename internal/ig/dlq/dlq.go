// Package dlq replays igs's dead letters: crawl tasks parked on
// igs.task.dlq, and entity records the persister quarantined on
// igs.persist.dlq (see internal/crawl/dlq for fbs's tasks).
package dlq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// Replayed is a dead-lettered task made ready to run again.
type Replayed struct {
	Task  *igstaskv1.Task
	Value []byte // the encoded task
	Key   string // the record key: the task's scope, as the worker keys it
	// Rejoined: its job was still running, so the new task id joined the
	// job's accounting and the job waits for it. A finished job's replay
	// still writes its data, outside the job's counters.
	Rejoined bool
}

// Prepare decodes a dead-lettered task and returns it under a new task id
// with its retry counts reset (task.Replay): the dead letter marked the old
// id done, so the worker would skip it as is. A task of a running job
// rejoins that job; media tasks are not part of job accounting. An
// undecodable value is an error; the caller decides whether to pass it on
// unchanged.
func Prepare(ctx context.Context, tr *jobs.Tracker, value []byte, now time.Time) (Replayed, error) {
	var t igstaskv1.Task
	if err := proto.Unmarshal(value, &t); err != nil {
		return Replayed{}, fmt.Errorf("dlq: decode task: %w", err)
	}
	if t.TaskId == "" || t.Kind == igstaskv1.Kind_KIND_UNSPECIFIED {
		return Replayed{}, fmt.Errorf("dlq: not an igs task")
	}
	r := Replayed{Task: task.Replay(&t, now), Key: task.ScopeKey(&t)}
	if tr != nil && r.Task.JobId != "" && r.Task.Kind != igstaskv1.Kind_KIND_MEDIA {
		running, err := tr.Running(ctx, r.Task.JobId)
		if err != nil {
			return Replayed{}, fmt.Errorf("dlq: job %s: %w", r.Task.JobId, err)
		}
		if running {
			if err := tr.Spawn(ctx, r.Task.JobId, r.Task.TaskId); err != nil {
				return Replayed{}, fmt.Errorf("dlq: rejoin job %s: %w", r.Task.JobId, err)
			}
			r.Rejoined = true
		}
	}
	b, err := proto.Marshal(r.Task)
	if err != nil {
		return Replayed{}, err
	}
	r.Value = b
	return r, nil
}

// Options select what Replay takes off a dead-letter topic.
type Options struct {
	// Persisted replays the persister's quarantine (igs.persist.dlq)
	// instead of the crawl DLQ.
	Persisted bool
	Reason    string        // only records with this x-reason ("" = all)
	Max       int           // stop after this many records
	Idle      time.Duration // stop after this long without records
}

// Result counts what Replay did.
type Result struct {
	Replayed    int `json:"replayed"`
	Rejoined    int `json:"rejoined"`    // tasks that rejoined their running job
	Undecodable int `json:"undecodable"` // records passed on unchanged
	Skipped     int `json:"skipped"`     // other reasons, or no origin topic
}

// Group is the consumer group a replay reads its topic with (prefixed by
// the caller's client): committed offsets keep a second replay from taking
// the same records again.
func Group(persisted bool) string {
	if persisted {
		return "igs-ig-persist-dlq-replay"
	}
	return "igs-ig-dlq-replay"
}

// Topic is the dead-letter topic a replay reads.
func Topic(persisted bool) string {
	if persisted {
		return topics.PersistDLQ
	}
	return topics.DLQ
}

// Replay takes records off the DLQ through cl (a client in Group's group
// consuming Topic) and produces each to its origin topic through p: tasks
// under new ids (Prepare), quarantined entity records unchanged with their
// type headers. Offsets are committed after each produced batch.
func Replay(ctx context.Context, cl *kgo.Client, p *kafka.Producer, tr *jobs.Tracker, o Options) (Result, error) {
	var res Result
	if o.Max <= 0 {
		o.Max = 1000
	}
	if o.Idle <= 0 {
		o.Idle = 10 * time.Second
	}
	for res.Replayed < o.Max {
		pctx, cancel := context.WithTimeout(ctx, o.Idle)
		f := cl.PollRecords(pctx, min(100, o.Max-res.Replayed))
		cancel()
		if err := ctx.Err(); err != nil {
			return res, err
		}
		recs := f.Records()
		if len(recs) == 0 {
			break
		}
		var out []*kgo.Record
		for _, r := range recs {
			if o.Reason != "" && kafka.HeaderValue(r, kafka.HeaderReason) != o.Reason {
				res.Skipped++
				continue
			}
			orig := kafka.HeaderValue(r, kafka.HeaderOrigTopic)
			if orig == "" {
				res.Skipped++
				continue
			}
			rec := &kgo.Record{Topic: p.Topic(strings.TrimPrefix(orig, p.Topic(""))), Key: r.Key, Value: r.Value}
			typeHeaders := func() {
				for _, h := range r.Headers {
					if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType {
						rec.Headers = append(rec.Headers, h)
					}
				}
			}
			if o.Persisted {
				// Entity records go back unchanged, with their type headers.
				typeHeaders()
			} else if rp, err := Prepare(ctx, tr, r.Value, time.Now()); err == nil {
				rec.Key, rec.Value = []byte(rp.Key), rp.Value
				typeHeaders()
				if rp.Rejoined {
					res.Rejoined++
				}
			} else if kafka.HeaderValue(r, kafka.HeaderReason) == "undecodable" {
				// The worker could not decode it either: as is.
				res.Undecodable++
			} else {
				return res, err
			}
			out = append(out, rec)
		}
		if len(out) > 0 {
			if err := p.ProduceSync(ctx, out...); err != nil {
				return res, err
			}
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil {
			return res, err
		}
		res.Replayed += len(out)
	}
	return res, nil
}
