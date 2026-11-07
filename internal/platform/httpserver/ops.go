// Package httpserver provides the operations server every binary exposes:
// /metrics, /healthz (liveness) and /readyz (readiness checks).
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Check reports whether a dependency is usable.
type Check func(ctx context.Context) error

// Ops is the operations HTTP server.
//
// A binary starts it first (Start), before opening its stores: opening can
// wait minutes for a busy database (pg.RetryFor), and a liveness probe that
// finds nothing listening would kill the pod first (REQ-008, the production
// start of 7 Nov 2026). /healthz answers 200 from then on; /readyz answers
// 503 "starting" until Run, which the binary reaches once its stores are
// open, and then runs the readiness checks.
type Ops struct {
	addr    string
	mu      sync.RWMutex
	checks  map[string]Check
	ready   atomic.Bool
	started *http.Server
	served  chan error

	// The readiness checks' last result, reused for checkEvery.
	checkEvery time.Duration
	resultMu   sync.Mutex
	checkedAt  time.Time
	failure    string // "" when every check passed
}

// CheckEvery is how long /readyz reuses its checks' result. Each check is a
// round trip to a store, and Kubernetes asks every 10 seconds: through the
// platform's PgBouncer, a database touched that often never lets its idle
// server connections close, and the shared database runs out of them
// (production, 7 Nov 2026). Once a minute is enough for readiness: a store
// that fails makes the work fail and retry at once anyway.
const CheckEvery = time.Minute

// checkTimeout bounds one round of checks. They run on their own context, so
// a probe that gives up first (Kubernetes' default timeout is 1 s) doesn't
// cancel them and leave a false failure for the next minute.
const checkTimeout = 3 * time.Second

func NewOps(addr string) *Ops { return &Ops{addr: addr, checks: map[string]Check{}, checkEvery: CheckEvery} }

// AddCheck registers a readiness check.
func (o *Ops) AddCheck(name string, c Check) {
	o.mu.Lock()
	o.checks[name] = c
	o.mu.Unlock()
}

func (o *Ops) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !o.ready.Load() {
			http.Error(w, "starting: opening the stores", http.StatusServiceUnavailable)
			return
		}
		if failure := o.readiness(); failure != "" {
			http.Error(w, failure, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// readiness is the checks' result: the last one if it is younger than
// checkEvery, else a new one. Concurrent probes wait for the same round.
func (o *Ops) readiness() string {
	o.resultMu.Lock()
	defer o.resultMu.Unlock()
	if !o.checkedAt.IsZero() && time.Since(o.checkedAt) < o.checkEvery {
		return o.failure
	}
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	o.failure = ""
	o.mu.RLock()
	for name, c := range o.checks {
		if err := c(ctx); err != nil {
			o.failure = name + ": " + err.Error()
			break
		}
	}
	o.mu.RUnlock()
	o.checkedAt = time.Now()
	return o.failure
}

// Start listens at once and serves in the background, not ready yet. It
// fails if the address can't be bound. stop shuts the server down if the
// binary returns before Run (a store that never opened); after Run it does
// nothing.
func (o *Ops) Start() (stop func(), err error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", o.addr)
	if err != nil {
		return nil, fmt.Errorf("ops: listen %s: %w", o.addr, err)
	}
	o.started = &http.Server{Handler: o.Handler(), ReadHeaderTimeout: 10 * time.Second}
	o.served = make(chan error, 1)
	go func() { o.served <- o.started.Serve(ln) }()
	slog.Info("http listening; opening the stores", "addr", ln.Addr().String())
	return func() {
		if !o.ready.Load() {
			_ = o.started.Close()
			// The listener is closed once Serve returns.
			if err := <-o.served; err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Warn("ops: server stopped", "err", err)
			}
		}
	}, nil
}

// Run marks the binary ready and serves until ctx is done: the server
// Start began, or a new one. It belongs in the binary's run group.
func (o *Ops) Run(ctx context.Context) error {
	o.ready.Store(true)
	if o.started == nil {
		return Serve(ctx, o.addr, o.Handler())
	}
	select {
	case err := <-o.served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return o.started.Shutdown(shutdownCtx)
	}
}

// Serve runs an HTTP server and shuts it down gracefully when ctx is done.
func Serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("http listening", "addr", addr)
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
