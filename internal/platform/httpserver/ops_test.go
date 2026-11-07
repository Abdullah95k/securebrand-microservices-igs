package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// Started before the stores open, the server answers liveness at once and
// says "starting" to readiness; once the binary runs it, readiness is the
// checks'; when the binary stops, so does the server (the production start
// of 7 Nov 2026: a pod waiting for a busy database was killed by its
// liveness probe because nothing listened on :9090).
func TestStartBeforeTheStores(t *testing.T) {
	addr := freeAddr(t)
	o := NewOps(addr)
	o.checkEvery = 0 // every probe runs the checks (TestReadinessReusesItsResult covers the reuse)
	stop, err := o.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	base := "http://" + addr
	if code, _ := get(t, base+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz while starting: %d", code)
	}
	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "starting") {
		t.Fatalf("readyz while starting: %d %q", code, body)
	}

	// The stores are open: the binary adds its checks and runs the server.
	var failing atomic.Bool
	o.AddCheck("postgres", func(context.Context) error {
		if failing.Load() {
			return errors.New("down")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- o.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code, _ := get(t, base+"/readyz"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("readyz never became ready after Run")
		}
		time.Sleep(10 * time.Millisecond)
	}
	failing.Store(true)
	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "postgres: down") {
		t.Fatalf("readyz with a failing check: %d %q", code, body)
	}

	cancel()
	select {
	case err := <-ran:
		if err != nil {
			t.Fatalf("Run after its context ended: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
	if _, err := http.Get(base + "/healthz"); err == nil {
		t.Fatal("the server still answers after Run returned")
	}
}

// A binary whose store never opens returns before Run: stop closes the
// server, and the address is free again.
func TestStopBeforeRun(t *testing.T) {
	addr := freeAddr(t)
	o := NewOps(addr)
	stop, err := o.Start()
	if err != nil {
		t.Fatal(err)
	}
	stop()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the address is still taken after stop: %v", err)
	}
	_ = ln.Close()
}

// An address already in use fails Start at once, so the binary exits with
// the reason instead of running without its ops server.
func TestStartOnATakenAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if _, err := NewOps(ln.Addr().String()).Start(); err == nil || !strings.Contains(err.Error(), "ops: listen") {
		t.Fatalf("a taken address: %v", err)
	}
}

// Without Start (tests, tools), Run serves as before and is ready at once.
func TestRunWithoutStart(t *testing.T) {
	addr := freeAddr(t)
	o := NewOps(addr)
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- o.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code != http.StatusOK {
				t.Fatalf("readyz: %d", code)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never listened: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
}

// Readiness runs its checks at most once per checkEvery and reuses the
// result in between, a failure too: Kubernetes probes every 10 seconds, and
// each check through the platform's PgBouncer would otherwise keep a server
// connection from ever closing. Liveness never runs a check. A probe that
// gives up before the checks end doesn't cancel them.
func TestReadinessReusesItsResult(t *testing.T) {
	addr := freeAddr(t)
	o := NewOps(addr)
	if o.checkEvery != time.Minute {
		t.Fatalf("checkEvery %s, want a minute", o.checkEvery)
	}
	o.checkEvery = 300 * time.Millisecond
	var calls atomic.Int32
	var failing, slow atomic.Bool
	o.AddCheck("postgres", func(ctx context.Context) error {
		calls.Add(1)
		if slow.Load() {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if failing.Load() {
			return errors.New("down")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = o.Run(ctx) }()
	base := "http://" + addr
	deadline := time.Now().Add(5 * time.Second)
	for {
		if resp, err := http.Get(base + "/readyz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never listened")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The first probe above ran the checks; these reuse its result.
	for range 5 {
		if code, _ := get(t, base+"/readyz"); code != http.StatusOK {
			t.Fatalf("readyz: %d", code)
		}
		get(t, base+"/healthz")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d check rounds for 6 probes within checkEvery, want 1", n)
	}

	// A store that fails is seen once the result is older than checkEvery,
	// and the failure is reused too.
	failing.Store(true)
	time.Sleep(350 * time.Millisecond)
	for range 3 {
		if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "postgres: down") {
			t.Fatalf("readyz with the store down: %d %q", code, body)
		}
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d check rounds, want 2", n)
	}

	// A probe that gives up early leaves the checks to finish: the store is
	// back and slow, the client waits 50 ms, and the next probe reuses the
	// passing result instead of a cancelled one.
	failing.Store(false)
	slow.Store(true)
	time.Sleep(350 * time.Millisecond)
	impatient := &http.Client{Timeout: 50 * time.Millisecond}
	if _, err := impatient.Get(base + "/readyz"); err == nil {
		t.Fatal("the impatient probe got an answer before the slow check ended")
	}
	time.Sleep(250 * time.Millisecond)
	if code, body := get(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("after a probe gave up: %d %q", code, body)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("%d check rounds, want 3", n)
	}
}
