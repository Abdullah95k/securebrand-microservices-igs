package pg

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A database that accepts the connection and never answers (a pooler with
// every server connection busy) holds the first ping without failing it.
// Opening says, at INFO and while it waits, that it is still waiting, so a
// pod's log explains why it isn't up yet (the production start of 7 Nov
// 2026: media-janitor's log was empty until its probe killed it).
func TestOpenLogsWhileTheDatabaseIsSlow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer func() { _ = c.Close() }() // held open, never answered
		}
	}()

	var logs syncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)
	prevSlow := slowLog
	slowLog = 100 * time.Millisecond
	defer func() { slowLog = prevSlow }()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	dsn := "postgres://u:p@" + ln.Addr().String() + "/db?sslmode=disable&connect_timeout=5"
	if _, err := Open(ctx, dsn, 1, Retry(0)); err == nil {
		t.Fatal("opened a database that never answers")
	}
	if n := strings.Count(logs.String(), "pg: still waiting for the database"); n < 2 {
		t.Fatalf("%d waiting lines while the ping hung; logs:\n%s", n, logs.String())
	}
}
