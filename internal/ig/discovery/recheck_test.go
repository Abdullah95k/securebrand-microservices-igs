package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

func TestRecheckSlots(t *testing.T) {
	for _, c := range []struct {
		n     int
		share float64
		want  int
	}{{20, 0.1, 2}, {5, 0.1, 1}, {20, 0, 0}, {0, 0.1, 0}, {50, 0.25, 12}} {
		if got := recheckSlots(c.n, c.share); got != c.want {
			t.Errorf("recheckSlots(%d, %v) = %d, want %d", c.n, c.share, got, c.want)
		}
	}
}

// Small candidates (in scope, under the follower floor) are queued for a
// re-vet after the n-th wait following their n-th small decision, the last
// wait repeating, and after the first wait once seen again. Candidates
// decided below only for the floor before small existed become small.
func TestDueRechecks(t *testing.T) {
	db := testenv.PostgresIG(t)
	ctx := context.Background()
	day := 24 * time.Hour
	cfg := &config.Config{}
	cfg.IG.Discovery = config.IGDiscovery{Threshold: 4, MinFollowers: 10, FollowerRechecks: []time.Duration{7 * day, 14 * day, 30 * day}}
	add := func(u, status string, decidedDaysAgo, seenDaysAgo int, smallVets int, score float64, vet string) {
		t.Helper()
		if _, err := db.Exec(ctx, `INSERT INTO discovery_candidates (username, status, decided_at, last_seen_at, score, vet)
			VALUES ($1, $2, now() - $3::int * interval '1 day', now() - $4::int * interval '1 day', $5, $6::jsonb)`,
			u, status, decidedDaysAgo, seenDaysAgo, score, vet); err != nil {
			t.Fatal(err)
		}
		for i := range smallVets {
			if _, err := db.Exec(ctx, `INSERT INTO discovery_vets (username, vetted_at, status, score, signals, sightings, vet)
				VALUES ($1, now() - $2::int * interval '1 hour', 'small', 5, '{}', '{}', '{}')`, u, i+1); err != nil {
				t.Fatal(err)
			}
		}
	}
	small := `{"found": true, "followers": 4}`
	add("first_due", "small", 8, 20, 1, 5, small)       // 1st wait (7d) over
	add("first_waiting", "small", 6, 20, 1, 5, small)   // not yet
	add("second_waiting", "small", 10, 20, 2, 5, small) // 2nd wait is 14d
	add("second_due", "small", 15, 20, 2, 5, small)     // over
	add("fourth_waiting", "small", 29, 40, 4, 5, small) // the last wait (30d) repeats
	add("fourth_due", "small", 31, 40, 4, 5, small)     // over
	add("seen_again", "small", 8, 1, 3, 5, small)       // seen again: the 1st wait applies
	add("seen_too_soon", "small", 3, 1, 3, 5, small)    // seen again, but within 7d
	add("floor_only", "below", 2, 2, 0, 5, small)       // below only for the floor: becomes small
	add("below_score", "below", 2, 2, 0, 2, small)      // below the score: stays below
	add("below_private", "below", 2, 2, 0, 5, `{"found": true, "private": true, "followers": 4}`)
	v := &Vetter{DB: db, Cfg: cfg}
	if err := v.dueRechecks(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"first_due": "recheck", "first_waiting": "small", "second_waiting": "small", "second_due": "recheck",
		"fourth_waiting": "small", "fourth_due": "recheck", "seen_again": "recheck", "seen_too_soon": "small",
		"floor_only": "small", "below_score": "below", "below_private": "below"}
	for u, w := range want {
		var st string
		if err := db.QueryRow(ctx, `SELECT status FROM discovery_candidates WHERE username = $1`, u).Scan(&st); err != nil || st != w {
			t.Errorf("%s: %s %v, want %s", u, st, err, w)
		}
	}
}
