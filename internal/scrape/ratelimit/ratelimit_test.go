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

// The limiter fails closed: while Valkey does not answer, a wait never
// lets the request go; it waits until the context ends, and resumes once
// Valkey answers again.
func TestWaitFailsClosedWithoutValkey(t *testing.T) {
	defer func(lo, hi time.Duration) { StoreRetryMin, StoreRetryMax = lo, hi }(StoreRetryMin, StoreRetryMax)
	StoreRetryMin, StoreRetryMax = 20*time.Millisecond, 100*time.Millisecond
	mr := miniredis.RunT(t)
	l := NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond}))
	mr.Close()

	for _, wait := range []func(context.Context) error{
		func(ctx context.Context) error { return l.WaitRate(ctx, "exit:isp#1", 0.25) },
		func(ctx context.Context) error { return l.Wait(ctx, "global", 1000) },
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		start := time.Now()
		err := wait(ctx)
		cancel()
		if err == nil || time.Since(start) < 350*time.Millisecond {
			t.Fatalf("Valkey down: returned %v after %v; want a wait until the context ends", err, time.Since(start))
		}
	}

	// Valkey comes back: a waiting request goes ahead.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- l.WaitRate(ctx, "exit:isp#2", 0.25)
	}()
	time.Sleep(150 * time.Millisecond)
	if err := mr.Restart(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("after Valkey came back: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("still waiting after Valkey came back")
	}
}
