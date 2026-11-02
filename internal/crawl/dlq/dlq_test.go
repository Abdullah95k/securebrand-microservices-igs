package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
)

func dead(t *testing.T, job string) []byte {
	t.Helper()
	b, err := proto.Marshal(&taskv1.Task{TaskId: "old", LineageId: "lin", Seq: 3, JobId: job, Kind: taskv1.Kind_KIND_POST,
		Cursor: "c3", Attempt: 9, Stalls: 5, ItemsSeen: 120, Scope: &taskv1.Task_Post{Post: &taskv1.PostScope{PostId: 7}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The replay runs: the dead letter marked the old id done (the worker
// skips it for 7 days), so the replay has a new id, fresh retry counts, and
// the same stream position. It rejoins a job that is still running.
func TestReplayRunsAndRejoinsARunningJob(t *testing.T) {
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	tr, ck := jobs.New(r), checkpoint.New(r)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(tr.Spawn(ctx, "j1", "old", "other"))
	_, err := tr.Done(ctx, "j1", "old", map[string]int64{"tasks_failed": 1}) // the dead letter
	must(err)
	must(ck.MarkDone(ctx, "old"))

	rp, err := Prepare(ctx, tr, dead(t, "j1"), time.Unix(1700000000, 0))
	must(err)
	nt := rp.Task
	if nt.TaskId == "old" || nt.Attempt != 0 || nt.Stalls != 0 || nt.LineageId != "lin" || nt.Seq != 3 || nt.Cursor != "c3" ||
		nt.ItemsSeen != 120 || rp.Key != "post:7" || !rp.Rejoined {
		t.Fatalf("replayed: %+v (key %s, rejoined %v)", nt, rp.Key, rp.Rejoined)
	}
	if done, _ := ck.IsDone(ctx, nt.TaskId); done {
		t.Fatal("the new id must not be marked done")
	}
	var back taskv1.Task
	must(proto.Unmarshal(rp.Value, &back))
	if back.TaskId != nt.TaskId {
		t.Fatal("value does not hold the replayed task")
	}
	// The job now waits for the replay as well as its other task.
	if st, _ := tr.Status(ctx, "j1"); st.Pending != 2 {
		t.Fatalf("pending %d, want 2", st.Pending)
	}
	if _, err := tr.Done(ctx, "j1", nt.TaskId, nil); err != nil {
		t.Fatalf("the replay's completion counts for its job: %v", err)
	}
}

// A finished job is not reopened: the replay writes its data outside the
// job's counters (re-finalising would overwrite them).
func TestReplayOfAFinishedJob(t *testing.T) {
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	tr := jobs.New(r)
	if err := tr.Spawn(ctx, "j2", "old"); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Done(ctx, "j2", "old", nil); err != nil {
		t.Fatal(err)
	}
	rp, err := Prepare(ctx, tr, dead(t, "j2"), time.Now())
	if err != nil || rp.Rejoined {
		t.Fatalf("rejoined a finished job: %+v %v", rp, err)
	}
	if rp2, err := Prepare(ctx, tr, dead(t, "gone"), time.Now()); err != nil || rp2.Rejoined {
		t.Fatalf("expired job: %+v %v", rp2, err)
	}
	if st, _ := tr.Status(ctx, "j2"); st.Pending != 0 {
		t.Fatalf("finished job reopened: %+v", st)
	}
	if _, err := Prepare(ctx, tr, []byte{0xff}, time.Now()); err == nil {
		t.Fatal("undecodable value must be an error")
	}
}
