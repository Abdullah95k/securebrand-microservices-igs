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
