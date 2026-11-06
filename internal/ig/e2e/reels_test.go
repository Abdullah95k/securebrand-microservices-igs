package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
)

// A scheduled run reads the reels tab only ig.schedule.reels_window back.
// Once a profile's last full pass is ig.schedule.reels_full_every old, a
// scheduled run reads the tab back to the age limit again, so older reels'
// counts stay current too. A full run always reads it all.
func TestReelsWindow(t *testing.T) {
	s := Start(t, Options{Spec: spec(), Faults: fakeig.Faults{MaxPageSize: 12}})
	ctx := context.Background()
	lim := s.Limits()
	p := s.World.Profiles()[0]
	window := s.Cfg.IG.Schedule.ReelsWindow
	if window <= 0 || s.Cfg.IG.Schedule.ReelsFullEvery <= 0 {
		t.Fatalf("reels_window %s, reels_full_every %s: the defaults should set both", window, s.Cfg.IG.Schedule.ReelsFullEvery)
	}

	// A reel inside the window and one outside it (but inside the age
	// limit), each showing its counts and not pinned: the scheduled grid
	// never reads either again, so only the reels tab changes their likes.
	now := time.Now()
	var recent, old *fakeig.Post
	inWindow := 0
	for _, x := range p.Reels() {
		if x.TakenAt.After(now.Add(-window)) {
			inWindow++
		}
		if x.LikesHidden || x.Gated || slices.Contains(p.Pinned, x) {
			continue
		}
		switch {
		case recent == nil && x.TakenAt.After(now.Add(-window+time.Hour)):
			recent = x
		case old == nil && x.TakenAt.Before(now.Add(-window-24*time.Hour)):
			old = x
		}
	}
	if recent == nil || old == nil {
		t.Fatalf("the world has no reel inside (%v) or outside (%v) the %s window", recent != nil, old != nil, window)
	}
	all := len(p.Reels())

	job := s.Crawl(t, p.Username, full, lim, nil)
	st := s.Wait(t, 3*time.Minute, job)
	if got := st[job].Counters["reels_full_passes"]; got != 1 {
		t.Errorf("full run: %d full reels passes, want 1 (counters %v)", got, st[job].Counters)
	}
	if got := st[job].Counters["reels"]; got != int64(all) {
		t.Errorf("full run read %d reels, want all %d", got, all)
	}
	lastFull := reelsCursor(t, s, p.PK, "last_full_at")
	if lastFull.IsZero() {
		t.Fatal("full run: no last_full_at for the profile's reels")
	}

	// A scheduled run within the week: the window only.
	setLikes(t, s, recent.PK, 11111)
	setLikes(t, s, old.PK, 22222)
	job = s.Crawl(t, p.Username, incremental, lim, nil)
	st = s.Wait(t, 3*time.Minute, job)
	c := st[job].Counters
	if c["reels_full_passes"] != 0 {
		t.Errorf("scheduled run inside the week made a full pass (counters %v)", c)
	}
	if c["reels"] < int64(inWindow-1) || c["reels"] > int64(inWindow+1) || c["reels"] >= int64(all) {
		t.Errorf("scheduled run read %d reels, want the %d in the window (of %d)", c["reels"], inWindow, all)
	}
	if got := likes(t, s, recent.PK); got != 11111 {
		t.Errorf("reel inside the window: likes %d, want 11111", got)
	}
	if got := likes(t, s, old.PK); got == 22222 {
		t.Error("reel outside the window was read on a windowed run")
	}
	if reelsCursor(t, s, p.PK, "last_incremental_at").IsZero() {
		t.Error("scheduled run: no last_incremental_at for the profile's reels")
	}
	if !reelsCursor(t, s, p.PK, "last_full_at").Equal(lastFull) {
		t.Error("a windowed run moved last_full_at")
	}

	// A week after the last full pass, the scheduled run reads it all.
	if _, err := s.DB.Exec(ctx, `UPDATE crawl_cursors SET last_full_at = now() - make_interval(secs => $2)
		WHERE scope_type = 'profile_reels' AND scope_id = $1`, p.PK, (s.Cfg.IG.Schedule.ReelsFullEvery + time.Hour).Seconds()); err != nil {
		t.Fatal(err)
	}
	job = s.Crawl(t, p.Username, incremental, lim, nil)
	st = s.Wait(t, 3*time.Minute, job)
	c = st[job].Counters
	if c["reels_full_passes"] != 1 || c["reels"] != int64(all) {
		t.Errorf("scheduled run a week on: %d full passes and %d reels, want 1 and %d (counters %v)", c["reels_full_passes"], c["reels"], all, c)
	}
	if got := likes(t, s, old.PK); got != 22222 {
		t.Errorf("reel outside the window after the weekly full pass: likes %d, want 22222", got)
	}
	if at := reelsCursor(t, s, p.PK, "last_full_at"); !at.After(lastFull) {
		t.Errorf("weekly full pass: last_full_at %s, want after %s", at, lastFull)
	}
}

func setLikes(t *testing.T, s *Stack, pk, n int64) {
	t.Helper()
	if err := s.World.SetLikes(pk, n); err != nil {
		t.Fatal(err)
	}
}

func likes(t *testing.T, s *Stack, pk int64) int64 {
	t.Helper()
	var n int64
	if err := s.DB.QueryRow(context.Background(), `SELECT coalesce(like_count, -1) FROM posts WHERE media_pk = $1`, pk).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// reelsCursor reads a time column of the profile's reels cursor; zero when
// unset.
func reelsCursor(t *testing.T, s *Stack, pk int64, col string) time.Time {
	t.Helper()
	var at *time.Time
	err := s.DB.QueryRow(context.Background(), `SELECT `+col+` FROM crawl_cursors
		WHERE scope_type = 'profile_reels' AND scope_id = $1`, pk).Scan(&at)
	if err != nil || at == nil {
		return time.Time{}
	}
	return *at
}
