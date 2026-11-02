package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Complete marks the task done for the TTL given and drops the segment's
// checkpoint only when asked (a finished task's), keeping it for a
// dead-lettered one, whose replay runs the same segment.
func TestComplete(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	for _, drop := range []bool{true, false} {
		lineage := "lin"
		if !drop {
			lineage = "dead"
		}
		if err := s.Save(ctx, lineage, 2, State{Cursor: "c", Pages: 3}); err != nil {
			t.Fatal(err)
		}
		if err := s.Complete(ctx, "task-"+lineage, lineage, 2, 96*time.Hour, drop); err != nil {
			t.Fatal(err)
		}
		if done, err := s.IsDone(ctx, "task-"+lineage); err != nil || !done {
			t.Fatalf("drop %v: not marked done (%v)", drop, err)
		}
		if ttl := mr.TTL("igs:done:task-" + lineage); ttl != 96*time.Hour {
			t.Fatalf("done marker TTL %s, want 96h", ttl)
		}
		_, ok, err := s.Load(ctx, lineage, 2)
		if err != nil {
			t.Fatal(err)
		}
		if ok == drop {
			t.Fatalf("drop %v: checkpoint still there: %v", drop, ok)
		}
	}
}
