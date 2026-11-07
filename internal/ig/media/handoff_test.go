package media

import (
	"testing"
	"time"
)

// Every file gets the contract's deadline: 72 hours unless set, never under 24.
func TestLifetime(t *testing.T) {
	for ttl, want := range map[time.Duration]time.Duration{0: 72 * time.Hour, -time.Hour: 72 * time.Hour,
		time.Hour: 24 * time.Hour, 48 * time.Hour: 48 * time.Hour, 720 * time.Hour: 720 * time.Hour} {
		if got := Lifetime(ttl); got != want {
			t.Errorf("Lifetime(%v) = %v, want %v", ttl, got, want)
		}
	}
}
