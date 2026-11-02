// Package dlq prepares dead-lettered crawl tasks for another run.
package dlq

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/task"
)

// Replayed is a dead-lettered task made ready to run again.
type Replayed struct {
	Task  *taskv1.Task
	Value []byte // the encoded task
	Key   string // the record key (the task's scope, as the worker uses)
	// Rejoined: its job was still running, so the new task id joined the
	// job's accounting and the job waits for it. A finished job's replay
	// still writes its data, outside the job's counters.
	Rejoined bool
}

// Prepare decodes a dead-lettered task and returns it under a new task id
// with its retry counts reset (task.Replay). Replaying the same id did
// nothing for 7 days: the dead letter had marked it done. An undecodable
// value is an error; the caller decides whether to pass it on unchanged.
func Prepare(ctx context.Context, tr *jobs.Tracker, value []byte, now time.Time) (Replayed, error) {
	var t taskv1.Task
	if err := proto.Unmarshal(value, &t); err != nil {
		return Replayed{}, fmt.Errorf("dlq: decode task: %w", err)
	}
	r := Replayed{Task: task.Replay(&t, now), Key: task.ScopeKey(&t)}
	if tr != nil && r.Task.JobId != "" {
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
