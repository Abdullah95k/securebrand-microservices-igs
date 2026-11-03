package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// WaitRate spaces requests under one key fleet-wide (two limiters on one
// Redis share it) at a rate below one a second; other keys are not slowed.
func TestWaitRateSpacesAKeyAcrossLimiters(t *testing.T) {
	mr := miniredis.RunT(t)
	a := NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	b := NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()
	const rps = 8.0 // one every 125ms
	start := time.Now()
	var wg sync.WaitGroup
	for i := range 4 {
		l := a
		if i%2 == 1 {
			l = b
		}
		wg.Go(func() {
			if err := l.WaitRate(ctx, "exit:x", rps); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	// Four requests at 8/s with a burst of one: the last waits ~375ms.
	if el := time.Since(start); el < 300*time.Millisecond || el > 2*time.Second {
		t.Fatalf("4 requests at %v/s took %v", rps, el)
	}
	other := time.Now()
	if err := a.WaitRate(ctx, "exit:y", rps); err != nil || time.Since(other) > 100*time.Millisecond {
		t.Fatalf("another exit waits for none: %v %v", time.Since(other), err)
	}
	if err := a.WaitRate(ctx, "exit:z", 0); err != nil {
		t.Fatal(err)
	}
}
