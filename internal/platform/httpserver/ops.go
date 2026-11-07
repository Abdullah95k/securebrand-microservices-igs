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
}

func NewOps(addr string) *Ops { return &Ops{addr: addr, checks: map[string]Check{}} }

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
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		o.mu.RLock()
		defer o.mu.RUnlock()
		for name, c := range o.checks {
			if err := c(ctx); err != nil {
				http.Error(w, name+": "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
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
