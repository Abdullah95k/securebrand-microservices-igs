package stage

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Worker runs stage tasks consumed from Kafka.
type Worker struct {
	D      *Deps
	Stages map[igstaskv1.Kind]Stage
	// MaxAttempts bounds retries since a task last advanced its checkpoint;
	// MaxTotalAttempts every retry (0: 10 × MaxAttempts).
	MaxAttempts      uint32
	MaxTotalAttempts uint32
	LeaseTTL         time.Duration
	// RetryScale shortens the retry delays (30s, 5m) by this factor; 0
	// keeps them. Tests only.
	RetryScale float64
}

var stageNames = map[igstaskv1.Kind]string{igstaskv1.Kind_KIND_PROFILE: "profile", igstaskv1.Kind_KIND_POST: "post"}

func stageName(k igstaskv1.Kind) string {
	if n := stageNames[k]; n != "" {
		return n
	}
	return "unknown"
}

// Handle implements kafka.Handler. A task whose run panics is
// dead-lettered, not retried: redelivering it would crash every pod.
func (w *Worker) Handle(ctx context.Context, rec *kgo.Record) (err error) {
	var t igstaskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		return w.deadLetterRaw(ctx, rec, "undecodable", err.Error())
	}
	defer func() {
		if p := recover(); p != nil {
			slog.Error("task panicked", "task", t.TaskId, "kind", t.Kind.String(), "panic", p, "stack", string(debug.Stack()))
			err = w.deadLetter(ctx, &t, "panic", fmt.Sprint(p), nil)
		}
	}()
	return w.Run(ctx, &t)
}

// Run processes one decoded task.
func (w *Worker) Run(ctx context.Context, t *igstaskv1.Task) error {
	d := w.D
	name := stageName(t.Kind)
	lane := topics.LaneName(t.Lane)
	st, ok := w.Stages[t.Kind]
	if !ok {
		return w.deadLetter(ctx, t, "no_stage", "no stage for kind "+t.Kind.String(), nil)
	}
	if d.Jobs.Cancelled(ctx, t.JobId) {
		metrics.Tasks.WithLabelValues(name, lane, "cancelled").Inc()
		return w.finish(ctx, t, map[string]int64{"cancelled": 1})
	}
	if isDone, err := d.Ckpt.IsDone(ctx, t.TaskId); err != nil {
		return err
	} else if isDone {
		_, err := d.Jobs.Done(ctx, t.JobId, t.TaskId, nil)
		return ignoreUnspawned(err)
	}
	lease, ok, err := d.Ckpt.Acquire(ctx, task.ScopeKey(t), t.LineageId, w.LeaseTTL)
	if err != nil {
		return err
	}
	if !ok {
		metrics.Tasks.WithLabelValues(name, lane, "lease_busy").Inc()
		return w.toRetry(ctx, t, "30s")
	}
	before, _, beforeErr := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
	start := time.Now()
	out := st.Run(ctx, t)
	metrics.TaskSeconds.WithLabelValues(name).Observe(time.Since(start).Seconds())
	lease.Release(ctx)

	switch out.Result {
	case Done:
		metrics.Tasks.WithLabelValues(name, lane, "done").Inc()
		if err := d.spawn(ctx, out.FollowUps...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Continue:
		metrics.Tasks.WithLabelValues(name, lane, "yield").Inc()
		if err := d.spawn(ctx, append([]*igstaskv1.Task{out.Next}, out.FollowUps...)...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Retry:
		next := out.Next
		next.Stalls = t.Stalls + 1
		after, _, afterErr := d.Ckpt.Load(ctx, t.LineageId, t.Seq)
		if beforeErr == nil && afterErr == nil && (after.Cursor != before.Cursor || after.Pages > before.Pages) {
			next.Stalls = 0
		}
		if reason := w.exhausted(next); reason != "" {
			metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
			slog.Warn("task failed after retries", "stage", name, "job", t.JobId, "task", t.TaskId,
				"attempts", next.Attempt, "stalls", next.Stalls, "reason", out.Reason)
			return w.deadLetter(ctx, t, reason, out.Reason, out.Counters)
		}
		if afterErr == nil && t.JobId != "" {
			if cost := unsaved(costCounters(out.Counters), after.Counters); len(cost) > 0 {
				if err := d.Jobs.AddCounters(ctx, t.JobId, cost); err != nil {
					slog.Debug("jobs: retry counters", "job", t.JobId, "err", err)
				}
			}
		}
		metrics.Tasks.WithLabelValues(name, lane, "retry").Inc()
		metrics.Retries.WithLabelValues(name, out.Delay).Inc()
		slog.Debug("task retry", "stage", name, "task", t.TaskId, "attempt", next.Attempt, "stalls", next.Stalls,
			"delay", out.Delay, "reason", out.Reason)
		return w.toRetry(ctx, next, out.Delay)
	default:
		metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
		return w.deadLetter(ctx, t, "permanent", out.Reason, out.Counters)
	}
}

var costKeys = []string{"requests", "wire_bytes", "blocked", "soft_errors", "transport_errors", "drift"}

func unsaved(c, saved map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range c {
		if d := v - saved[k]; d > 0 {
			out[k] = d
		}
	}
	return out
}

func costCounters(c map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for _, k := range costKeys {
		if v := c[k]; v != 0 {
			out[k] = v
		}
	}
	return out
}

func (w *Worker) exhausted(next *igstaskv1.Task) string {
	total := w.MaxTotalAttempts
	if total == 0 {
		total = 10 * w.MaxAttempts
	}
	switch {
	case next.Stalls >= w.MaxAttempts:
		return "max_attempts"
	case next.Attempt >= total:
		return "max_total_attempts"
	}
	return ""
}

func (w *Worker) finish(ctx context.Context, t *igstaskv1.Task, c map[string]int64) error {
	if err := w.D.Ckpt.MarkDone(ctx, t.TaskId); err != nil {
		return err
	}
	_, err := w.D.Jobs.Done(ctx, t.JobId, t.TaskId, c)
	return ignoreUnspawned(err)
}

func (w *Worker) toRetry(ctx context.Context, t *igstaskv1.Task, delay string) error {
	topic, wait := topics.Retry30s, 30*time.Second
	if delay == "5m" {
		topic, wait = topics.Retry5m, 5*time.Minute
	}
	if w.RetryScale > 0 {
		wait = time.Duration(float64(wait) * w.RetryScale)
	}
	orig, _ := topics.TaskTopic(t.Kind, t.Lane)
	notBefore := time.Now().Add(wait).UnixMilli()
	rec, err := w.D.Producer.Record(topic, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(orig)},
		kgo.RecordHeader{Key: kafka.HeaderNotBefore, Value: []byte(strconv.FormatInt(notBefore, 10))})
	if err != nil {
		return err
	}
	return w.D.Producer.ProduceSync(ctx, rec)
}

// deadLetter parks t on the DLQ with its (unprefixed) origin topic and
// finishes it in job accounting.
func (w *Worker) deadLetter(ctx context.Context, t *igstaskv1.Task, reason, detail string, c map[string]int64) error {
	metrics.DLQ.WithLabelValues(stageName(t.Kind), reason).Inc()
	orig, _ := topics.TaskTopic(t.Kind, t.Lane)
	rec, err := w.D.Producer.Record(topics.DLQ, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(orig)},
		kgo.RecordHeader{Key: kafka.HeaderReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))})
	if err != nil {
		return err
	}
	if err := w.D.Producer.ProduceSync(ctx, rec); err != nil {
		return err
	}
	c = maps.Clone(c)
	if c == nil {
		c = map[string]int64{}
	}
	c["tasks_failed"]++
	if t.Kind == igstaskv1.Kind_KIND_POST {
		_ = w.D.Jobs.UpdatePostState(ctx, t.JobId, t.GetPost().GetMediaPk(), jobs.PostFailed)
	}
	return w.finish(ctx, t, c)
}

func (w *Worker) deadLetterRaw(ctx context.Context, rec *kgo.Record, reason, detail string) error {
	metrics.DLQ.WithLabelValues("unknown", reason).Inc()
	out := &kgo.Record{Topic: w.D.Producer.Topic(topics.DLQ), Key: rec.Key, Value: rec.Value, Headers: []kgo.RecordHeader{
		{Key: kafka.HeaderOrigTopic, Value: []byte(strings.TrimPrefix(rec.Topic, w.D.Producer.Topic("")))},
		{Key: kafka.HeaderReason, Value: []byte(reason)},
		{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))},
	}}
	return w.D.Producer.ProduceSync(ctx, out)
}

func ignoreUnspawned(err error) error {
	if err != nil {
		slog.Debug("jobs: done", "err", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// saveCkpt saves a segment's resume point with its counters so far.
func (d *Deps) saveCkpt(ctx context.Context, t *igstaskv1.Task, st checkpoint.State, c map[string]int64) error {
	st.Counters = maps.Clone(c)
	return d.Ckpt.Save(ctx, t.LineageId, t.Seq, st)
}

// RetryRelay moves parked tasks back to their original topic once due.
type RetryRelay struct {
	Producer *kafka.Producer
}

func (r *RetryRelay) Handle(ctx context.Context, rec *kgo.Record) error {
	orig := kafka.HeaderValue(rec, kafka.HeaderOrigTopic)
	if orig == "" {
		return nil
	}
	if nb, err := strconv.ParseInt(kafka.HeaderValue(rec, kafka.HeaderNotBefore), 10, 64); err == nil {
		if wait := time.Until(time.UnixMilli(nb)); wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	out := &kgo.Record{Topic: r.Producer.Topic(orig), Key: rec.Key, Value: rec.Value}
	for _, h := range rec.Headers {
		if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType || h.Key == kafka.HeaderTraceParent {
			out.Headers = append(out.Headers, h)
		}
	}
	if err := r.Producer.ProduceSync(ctx, out); err != nil {
		return fmt.Errorf("retry relay: %w", err)
	}
	return nil
}
