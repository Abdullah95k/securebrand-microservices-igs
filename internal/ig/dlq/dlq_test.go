package dlq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func deadPost(job string) *igstaskv1.Task {
	return &igstaskv1.Task{TaskId: "old", LineageId: "lin", Seq: 3, JobId: job, Kind: igstaskv1.Kind_KIND_POST,
		Lane: igstaskv1.Lane_LANE_P1, Cursor: "c3", Attempt: 9, Stalls: 5, ItemsSeen: 120,
		Scope: &igstaskv1.Task_Post{Post: &igstaskv1.PostScope{MediaPk: 7}}}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// A replayed task runs again: a new id (the dead letter marked the old one
// done), fresh retry counts, the same stream position, the worker's record
// key. It rejoins a job still running, and only such a job.
func TestPrepare(t *testing.T) {
	ctx := context.Background()
	tr := jobs.New(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()}))
	must(t, tr.Spawn(ctx, "running", "old", "other"))
	_, err := tr.Done(ctx, "running", "old", map[string]int64{"tasks_failed": 1})
	must(t, err)

	b, _ := proto.Marshal(deadPost("running"))
	rp, err := Prepare(ctx, tr, b, time.Unix(1700000000, 0))
	must(t, err)
	nt := rp.Task
	if nt.TaskId == "old" || nt.Attempt != 0 || nt.Stalls != 0 || nt.LineageId != "lin" || nt.Seq != 3 ||
		nt.Cursor != "c3" || nt.ItemsSeen != 120 || rp.Key != "post:7" || !rp.Rejoined {
		t.Fatalf("replayed %+v, key %s, rejoined %v", nt, rp.Key, rp.Rejoined)
	}
	var back igstaskv1.Task
	must(t, proto.Unmarshal(rp.Value, &back))
	if back.TaskId != nt.TaskId {
		t.Fatal("value does not hold the replayed task")
	}
	if st, _ := tr.Status(ctx, "running"); st.Pending != 2 {
		t.Fatalf("the running job should wait for the replay: pending %d", st.Pending)
	}

	// A finished job is not rejoined, nor is a media task (no accounting).
	b, _ = proto.Marshal(deadPost("finished"))
	if rp, err := Prepare(ctx, tr, b, time.Now()); err != nil || rp.Rejoined {
		t.Fatalf("finished job: rejoined %v, %v", rp.Rejoined, err)
	}
	media := &igstaskv1.Task{TaskId: "m", JobId: "running", Kind: igstaskv1.Kind_KIND_MEDIA,
		Scope: &igstaskv1.Task_Media{Media: &igstaskv1.MediaScope{Media: &igv1.Media{MediaId: "x"}}}}
	b, _ = proto.Marshal(media)
	if rp, err := Prepare(ctx, tr, b, time.Now()); err != nil || rp.Rejoined || rp.Key != "media:x" {
		t.Fatalf("media task: %+v, %v", rp, err)
	}

	if _, err := Prepare(ctx, tr, []byte("\xff\xff not a task"), time.Now()); err == nil {
		t.Fatal("garbage decoded as a task")
	}
	if _, err := Prepare(ctx, tr, nil, time.Now()); err == nil {
		t.Fatal("an empty record is not a task")
	}
}

// Against a real broker: dead-lettered tasks go back to their stage's
// topic under new ids, a reason filter leaves the others, the persister's
// quarantine goes back unchanged with its type header, and a second replay
// finds nothing (committed offsets).
func TestReplay(t *testing.T) {
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("dlq%d.", rand.Int64N(1<<40))
	p, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "dlq-test", TopicPrefix: prefix, Linger: time.Millisecond})
	must(t, err)
	t.Cleanup(p.Close)
	names := []string{topics.DLQ, topics.PersistDLQ, "igs.task.post.p1", "ig.comment.v1"}
	specs := make([]kafka.TopicSpec, len(names))
	for i, n := range names {
		specs[i] = kafka.TopicSpec{Name: n, Partitions: 1}
	}
	must(t, kafka.EnsureTopics(ctx, p.Client(), prefix, specs, 1))
	t.Cleanup(func() {
		full := make([]string, len(names))
		for i, n := range names {
			full[i] = prefix + n
		}
		_, _ = kadm.NewClient(p.Client()).DeleteTopics(context.Background(), full...)
	})

	park := func(dlqTopic, orig, reason string, msg proto.Message) {
		t.Helper()
		rec, err := p.Record(dlqTopic, "k", msg,
			kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(orig)},
			kgo.RecordHeader{Key: kafka.HeaderReason, Value: []byte(reason)})
		must(t, err)
		must(t, p.ProduceSync(ctx, rec))
	}
	park(topics.DLQ, "igs.task.post.p1", "attempts", deadPost("j"))
	park(topics.DLQ, "igs.task.post.p1", "blocked", deadPost("j"))
	park(topics.PersistDLQ, "ig.comment.v1", "unwritable", &igv1.Comment{CommentPk: 42, Text: "synthetic"})

	replay := func(persisted bool, reason string) Result {
		t.Helper()
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(prefix+Group(persisted)),
			kgo.ConsumeTopics(prefix+Topic(persisted)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
		must(t, err)
		defer cl.Close()
		res, err := Replay(ctx, cl, p, nil, Options{Persisted: persisted, Reason: reason, Idle: 3 * time.Second})
		must(t, err)
		return res
	}
	if res := replay(false, "attempts"); res.Replayed != 1 || res.Skipped != 1 {
		t.Fatalf("task replay: %+v", res)
	}
	if res := replay(true, ""); res.Replayed != 1 {
		t.Fatalf("quarantine replay: %+v", res)
	}
	if res := replay(false, ""); res.Replayed != 0 {
		t.Fatalf("second replay took records again: %+v", res)
	}

	read := func(topic string) []*kgo.Record {
		t.Helper()
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(prefix+topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		must(t, err)
		defer cl.Close()
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return cl.PollRecords(pctx, 10).Records()
	}
	tasks := read("igs.task.post.p1")
	if len(tasks) != 1 {
		t.Fatalf("%d records on the post topic, want 1", len(tasks))
	}
	var nt igstaskv1.Task
	must(t, proto.Unmarshal(tasks[0].Value, &nt))
	if nt.TaskId == "old" || nt.Attempt != 0 || string(tasks[0].Key) != "post:7" {
		t.Fatalf("replayed task %+v key %s", &nt, tasks[0].Key)
	}
	comments := read("ig.comment.v1")
	if len(comments) != 1 || kafka.HeaderValue(comments[0], kafka.HeaderType) != string(proto.MessageName(&igv1.Comment{})) {
		t.Fatalf("quarantined comment not replayed with its type: %d records", len(comments))
	}
	var c igv1.Comment
	must(t, proto.Unmarshal(comments[0].Value, &c))
	if c.CommentPk != 42 {
		t.Fatalf("comment changed on replay: %+v", &c)
	}
}
