package iraqtime

import (
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	at := time.Date(2026, 11, 7, 6, 15, 30, 0, time.UTC)
	if got := Format(at); got != "2026-11-07 09:15 Iraq time" {
		t.Errorf("Format: %q", got)
	}
	if got := Format(time.Time{}); got != "never" {
		t.Errorf("zero: %q", got)
	}
	if got := Span(at, at.Add(27*time.Minute)); got != "2026-11-07 09:15 to 09:42 Iraq time" {
		t.Errorf("same day: %q", got)
	}
	// 22:30 UTC is already the next day in Iraq.
	if got := Span(at, time.Date(2026, 11, 7, 22, 30, 0, 0, time.UTC)); got != "2026-11-07 09:15 to 2026-11-08 01:30 Iraq time" {
		t.Errorf("across midnight: %q", got)
	}
}
