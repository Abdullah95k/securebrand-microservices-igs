package probe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/proxy"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// While every exit of the tier is cooling down, a profile waits (asking
// again every poll) up to maxWait, instead of being given up at once as
// 274 profiles were in the owner's full run.
func TestAcquireWaitsForACoolingTier(t *testing.T) {
	want := &session.Identity{ID: "x"}
	getter := func(fails int) func() (*session.Identity, error) {
		n := 0
		return func() (*session.Identity, error) {
			if n++; n <= fails {
				return nil, proxy.ErrTierOpen
			}
			return want, nil
		}
	}
	notices := 0
	onWait := func() { notices++ }

	id, waited, err := acquire(context.Background(), time.Second, time.Millisecond, getter(3), onWait)
	if err != nil || id != want || waited <= 0 || notices != 1 {
		t.Fatalf("waited: id %v, %s, %v, %d notices", id, waited, err, notices)
	}

	notices = 0
	id, waited, err = acquire(context.Background(), 0, time.Millisecond, getter(3), onWait)
	if !errors.Is(err, proxy.ErrTierOpen) || id != nil || waited != 0 || notices != 0 {
		t.Fatalf("no wait allowed: id %v, %s, %v, %d notices", id, waited, err, notices)
	}

	id, _, err = acquire(context.Background(), 20*time.Millisecond, time.Millisecond, getter(1<<30), onWait)
	if !errors.Is(err, proxy.ErrTierOpen) || id != nil {
		t.Fatalf("gave up after maxWait: id %v, %v", id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := acquire(ctx, time.Hour, time.Millisecond, getter(1<<30), onWait); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped: %v", err)
	}

	id, waited, err = acquire(context.Background(), time.Hour, time.Millisecond, getter(0), onWait)
	if err != nil || id != want || waited != 0 {
		t.Fatalf("no cooling: id %v, %s, %v", id, waited, err)
	}
}
