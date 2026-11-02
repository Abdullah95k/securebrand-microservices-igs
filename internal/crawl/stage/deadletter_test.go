package stage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/topics"
)

// dlqEnv is a worker over an in-memory Kafka and Redis, with a reader of
// its dead letter queue.
func dlqEnv(t *testing.T, stages map[taskv1.Kind]Stage) (*Worker, *jobs.Tracker, func() []*kgo.Record) {
	t.Helper()
	kc, err := kfake.NewCluster(kfake.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)
	cfg := kafka.ClientConfig{Brokers: kc.ListenAddrs(), ClientID: "test", TopicPrefix: "t."}
	p, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	tr := jobs.New(r)
	w := &Worker{D: &Deps{Producer: p, Jobs: tr, Ckpt: checkpoint.New(r)}, Stages: stages, MaxAttempts: 3, LeaseTTL: time.Minute}
	read := func() []*kgo.Record {
		cl, err := kgo.NewClient(kgo.SeedBrokers(kc.ListenAddrs()...), kgo.ConsumeTopics("t."+topics.DLQ),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return cl.PollFetches(ctx).Records()
	}
	return w, tr, read
}

// A task of a kind no stage handles (from a newer producer, or another
// platform's task on the wrong topic) is dead-lettered and finished; it
// used to panic in topics.Task and take the consumer down.
func TestUnknownKindIsDeadLettered(t *testing.T) {
	w, tr, read := dlqEnv(t, nil)
	ctx := context.Background()
	tk := &taskv1.Task{TaskId: "t1", JobId: "j1", Kind: taskv1.Kind(42), Lane: taskv1.Lane_LANE_P1}
	if err := tr.Spawn(ctx, "j1", "t1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx, tk); err != nil {
		t.Fatal(err)
	}
	recs := read()
	if len(recs) != 1 || kafka.HeaderValue(recs[0], kafka.HeaderReason) != "no_stage" || kafka.HeaderValue(recs[0], kafka.HeaderOrigTopic) != "" {
		t.Fatalf("dead letters: %d %v", len(recs), recs)
	}
	if st, _ := tr.Status(ctx, "j1"); st.Pending != 0 || st.Counters["tasks_failed"] != 1 {
		t.Fatalf("job accounting: %+v", st)
	}
}

type panicky struct{}

func (panicky) Run(context.Context, *taskv1.Task) Outcome { panic("boom") }

// A stage that panics dead-letters its task (reason "panic") instead of
// crashing every pod the record is redelivered to.
func TestPanickingStageIsDeadLettered(t *testing.T) {
	w, tr, read := dlqEnv(t, map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: panicky{}})
	ctx := context.Background()
	tk := &taskv1.Task{TaskId: "t2", JobId: "j2", LineageId: "l2", Kind: taskv1.Kind_KIND_POST, Lane: taskv1.Lane_LANE_P0,
		Scope: &taskv1.Task_Post{Post: &taskv1.PostScope{PostId: 7}}}
	if err := tr.Spawn(ctx, "j2", "t2"); err != nil {
		t.Fatal(err)
	}
	b, _ := proto.Marshal(tk)
	if err := w.Handle(ctx, &kgo.Record{Topic: "t.fbs.task.post.p0", Value: b}); err != nil {
		t.Fatal(err)
	}
	recs := read()
	if len(recs) != 1 || kafka.HeaderValue(recs[0], kafka.HeaderReason) != "panic" ||
		kafka.HeaderValue(recs[0], kafka.HeaderOrigTopic) != "fbs.task.post.p0" {
		t.Fatalf("dead letters: %v", recs)
	}
	if st, _ := tr.Status(ctx, "j2"); st.Pending != 0 {
		t.Fatalf("job still waits: %+v", st)
	}
}

// An undecodable record's origin topic is written without the prefix, like
// a decoded task's, so a replay finds it either way.
func TestRawDeadLetterOriginIsUnprefixed(t *testing.T) {
	w, _, read := dlqEnv(t, nil)
	if err := w.Handle(context.Background(), &kgo.Record{Topic: "t.fbs.task.page.p1", Value: []byte{0xff, 0x01}}); err != nil {
		t.Fatal(err)
	}
	recs := read()
	if len(recs) != 1 || kafka.HeaderValue(recs[0], kafka.HeaderOrigTopic) != "fbs.task.page.p1" ||
		kafka.HeaderValue(recs[0], kafka.HeaderReason) != "undecodable" {
		t.Fatalf("dead letters: %v", recs)
	}
}
