package stage

import (
	"testing"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
)

// A retry without progress counts a stall and waits 30s the first time,
// 5m after; progress starts the count again; a park (every exit resting)
// counts nothing and waits 5m; a 5m outcome (drift) stays 5m.
func TestRetryPlan(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stalls     uint32
		out        Outcome
		progressed bool
		want       uint32
		delay      string
	}{
		{"first block", 0, Outcome{Delay: "30s"}, false, 1, "30s"},
		{"second block", 1, Outcome{Delay: "30s"}, false, 2, "5m"},
		{"fourth block", 3, Outcome{Delay: "30s"}, false, 4, "5m"},
		{"progress", 3, Outcome{Delay: "30s"}, true, 0, "30s"},
		{"park", 1, Outcome{Delay: "5m", Park: true}, false, 1, "5m"},
		{"park after progress", 0, Outcome{Delay: "5m", Park: true}, true, 0, "5m"},
		{"drift", 0, Outcome{Delay: "5m"}, false, 1, "5m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &igstaskv1.Task{Stalls: tc.stalls}
			next := &igstaskv1.Task{}
			if d := retryPlan(task, next, tc.out, tc.progressed); d != tc.delay || next.Stalls != tc.want {
				t.Fatalf("delay %s stalls %d, want %s %d", d, next.Stalls, tc.delay, tc.want)
			}
		})
	}
	w := &Worker{MaxAttempts: 5}
	if r := w.exhausted(&igstaskv1.Task{Stalls: 4, Attempt: 49}); r != "" {
		t.Fatalf("4 stalls, 49 attempts: %s", r)
	}
	if r := w.exhausted(&igstaskv1.Task{Stalls: 1, Attempt: 50}); r != "max_total_attempts" {
		t.Fatalf("parks are bounded by max_total_attempts: %q", r)
	}
}
