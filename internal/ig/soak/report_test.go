package soak

import (
	"strings"
	"testing"
	"time"
)

// The verdict line keeps its "igs soak: PASS" start, which the platform
// reads, and says when the soak ended in Iraq time (REQ-008 fix 23).
func TestReportVerdictLine(t *testing.T) {
	r := Report{Profiles: 500, Took: 24*time.Hour + 3*time.Minute, Ended: time.Date(2026, 11, 8, 3, 30, 0, 0, time.UTC)}
	first := r.Lines()[0]
	if !strings.HasPrefix(first, "igs soak: PASS after 24h3m0s, 500 profiles") || !strings.HasSuffix(first, "ended 2026-11-08 06:30 Iraq time") {
		t.Errorf("verdict line: %q", first)
	}
	r.Diffs = []string{"x"}
	if first := r.Lines()[0]; !strings.HasPrefix(first, "igs soak: FAIL") {
		t.Errorf("a difference: %q", first)
	}
}

// When the harness fails, the soak fails and says so: fakeig left more
// than 1% of the world changes unanswered (its world stood still), or it
// restarted and generated its world again, so the comparison says nothing
// about igs.
func TestReportHarness(t *testing.T) {
	ok := Report{MutateTries: 1000, MutateFails: 10}
	ok.harness("a", "a")
	if !ok.Pass() || len(ok.HarnessFails) != 0 {
		t.Fatalf("1%% unanswered and no restart: %v", ok.HarnessFails)
	}
	for name, c := range map[string]struct {
		fails       int
		first, last string
		want        string
	}{
		"world stood still": {fails: 11, first: "a", last: "a", want: "11 of 1000 world changes unanswered"},
		"fakeig restarted":  {fails: 0, first: "a", last: "b", want: "fakeig restarted (started a, now b)"},
	} {
		r := Report{MutateTries: 1000, MutateFails: c.fails}
		r.harness(c.first, c.last)
		lines := strings.Join(r.Lines(), "\n")
		if r.Pass() || !strings.Contains(lines, "the harness failed, not necessarily igs") || !strings.Contains(lines, c.want) ||
			!strings.HasSuffix(lines, "igs soak: FAIL") {
			t.Errorf("%s: pass %v, report:\n%s", name, r.Pass(), lines)
		}
	}
}
