package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// Adaptive profiles are placed by their posting rate: warm at the
// threshold, long tail below; a profile whose pk is unknown waits; pinned
// and custom ones are left alone; a placed one is not checked again before
// auto_every.
func TestTiererPlacesByPostingRate(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	sc := &cfg.IG.Schedule
	sc.Tiers.WarmPostsPerDay = 2 // 28 posts in 14 days
	rows := []struct {
		ref      string
		pk       *int64
		tier     string
		adaptive bool
		posts    int
	}{
		{"u:busy", ptr(1), "long_tail", true, 30},
		{"u:quiet", ptr(2), "warm", true, 3},
		{"u:unknown", nil, "long_tail", true, 0},
		{"u:pinned", ptr(3), "hot", false, 0},
		{"u:custom", ptr(4), "custom", false, 40},
	}
	for _, r := range rows {
		if _, err := db.Exec(ctx, `INSERT INTO profile_schedule (profile_ref, user_pk, tier, adaptive, interval_seconds, next_run_at)
			VALUES ($1, $2, $3, $4, 3600, now() + interval '1 day')`, r.ref, r.pk, r.tier, r.adaptive); err != nil {
			t.Fatal(err)
		}
		for i := range r.posts {
			if _, err := db.Exec(ctx, `INSERT INTO post_index (media_pk, owner_pk, taken_at, shortcode) VALUES ($1, $2, now() - $3::int * interval '1 hour', $4)`,
				*r.pk*1000+int64(i), *r.pk, i*10, r.ref+string(rune('a'+i%26))+string(rune('a'+i/26))); err != nil {
				t.Fatal(err)
			}
		}
	}
	tr := &Tierer{DB: db, Cfg: cfg}
	if err := tr.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]struct {
		tier string
		secs int
	}{
		"u:busy":    {dispatch.TierWarm, int(sc.TierInterval(dispatch.TierWarm).Seconds())},
		"u:quiet":   {dispatch.TierLongTail, int(sc.TierInterval(dispatch.TierLongTail).Seconds())},
		"u:unknown": {dispatch.TierLongTail, 3600},
		"u:pinned":  {dispatch.TierHot, 3600},
		"u:custom":  {dispatch.TierCustom, 3600},
	}
	for ref, w := range want {
		var tier string
		var secs int
		var next time.Time
		if err := db.QueryRow(ctx, `SELECT tier, interval_seconds, next_run_at FROM profile_schedule WHERE profile_ref = $1`, ref).
			Scan(&tier, &secs, &next); err != nil {
			t.Fatal(err)
		}
		if tier != w.tier || secs != w.secs {
			t.Errorf("%s: %s every %ds, want %s every %ds", ref, tier, secs, w.tier, w.secs)
		}
		if ref == "u:busy" && next.After(time.Now().Add(time.Hour+time.Minute)) {
			t.Errorf("a profile moved to warm keeps its next run a day away: %v", next)
		}
	}
	// Checked again only after auto_every.
	if _, err := db.Exec(ctx, `DELETE FROM post_index WHERE owner_pk = 1`); err != nil {
		t.Fatal(err)
	}
	_ = tr.Tick(ctx)
	var tier string
	_ = db.QueryRow(ctx, `SELECT tier FROM profile_schedule WHERE profile_ref = 'u:busy'`).Scan(&tier)
	if tier != dispatch.TierWarm {
		t.Fatalf("re-placed before auto_every: %s", tier)
	}
}

func ptr(v int64) *int64 { return &v }

// With a quiet tier (D-055), adaptive profiles posting fewer than
// quiet_posts_per_week over auto_window are visited every quiet; busier
// ones stay long tail or warm.
func TestTiererPlacesQuietProfiles(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	sc := &cfg.IG.Schedule
	sc.Tiers.WarmPostsPerDay, sc.Tiers.QuietPostsPerWeek, sc.Tiers.Quiet = 2, 1, 24*time.Hour // over 14 days: warm at 28, quiet under 2
	for _, r := range []struct {
		ref   string
		pk    int64
		posts int
	}{{"u:silent", 1, 0}, {"u:rare", 2, 1}, {"u:weekly", 3, 3}, {"u:busy", 4, 30}} {
		if _, err := db.Exec(ctx, `INSERT INTO profile_schedule (profile_ref, user_pk, tier, adaptive, interval_seconds, next_run_at)
			VALUES ($1, $2, 'long_tail', true, 10800, now() + interval '3 hours')`, r.ref, r.pk); err != nil {
			t.Fatal(err)
		}
		for i := range r.posts {
			if _, err := db.Exec(ctx, `INSERT INTO post_index (media_pk, owner_pk, taken_at, shortcode) VALUES ($1, $2, now() - $3::int * interval '1 hour', $4)`,
				r.pk*1000+int64(i), r.pk, i*10, r.ref+string(rune('a'+i%26))+string(rune('a'+i/26))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := (&Tierer{DB: db, Cfg: cfg}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	for ref, want := range map[string]struct {
		tier string
		secs int
	}{
		"u:silent": {dispatch.TierQuiet, 86400},
		"u:rare":   {dispatch.TierQuiet, 86400},
		"u:weekly": {dispatch.TierLongTail, int(sc.TierInterval(dispatch.TierLongTail).Seconds())},
		"u:busy":   {dispatch.TierWarm, int(sc.TierInterval(dispatch.TierWarm).Seconds())},
	} {
		var tier string
		var secs int
		if err := db.QueryRow(ctx, `SELECT tier, interval_seconds FROM profile_schedule WHERE profile_ref = $1`, ref).Scan(&tier, &secs); err != nil {
			t.Fatal(err)
		}
		if tier != want.tier || secs != want.secs {
			t.Errorf("%s: %s every %ds, want %s every %ds", ref, tier, secs, want.tier, want.secs)
		}
	}
	// A quiet profile keeps its next run (3 hours away): it is not pushed a day out.
	var next time.Time
	_ = db.QueryRow(ctx, `SELECT next_run_at FROM profile_schedule WHERE profile_ref = 'u:silent'`).Scan(&next)
	if next.After(time.Now().Add(3*time.Hour + time.Minute)) {
		t.Errorf("a quiet profile's next run moved out: %v", next)
	}
}

// Place resolves tiers and intervals, and refuses what does not fit.
func TestPlace(t *testing.T) {
	cfg, _ := config.Load("")
	for _, c := range []struct {
		tier     string
		iv       time.Duration
		want     string
		adaptive bool
		bad      bool
	}{
		{"", 0, dispatch.TierLongTail, true, false},
		{"auto", 0, dispatch.TierLongTail, true, false},
		{"hot", 0, dispatch.TierHot, false, false},
		{"quiet", 0, dispatch.TierQuiet, false, false},
		{"", 30 * time.Minute, dispatch.TierCustom, false, false},
		{"custom", 0, "", false, true},
		{"hot", time.Hour, "", false, true},
		{"", time.Minute, "", false, true},
		{"", 31 * 24 * time.Hour, "", false, true},
		{"daily", 0, "", false, true},
	} {
		pl, err := dispatch.Place(cfg, c.tier, c.iv)
		if (err != nil) != c.bad || (!c.bad && (pl.Tier != c.want || pl.Adaptive != c.adaptive)) {
			t.Errorf("Place(%q, %v) = %+v, %v", c.tier, c.iv, pl, err)
		}
	}
	if pl, _ := dispatch.Place(cfg, "hot", 0); pl.Interval != 10*time.Minute {
		t.Errorf("hot every %v", pl.Interval)
	}
}
