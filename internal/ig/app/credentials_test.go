package app

import (
	"context"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

// Only the stages that read Instagram keep raw responses: the retry relay
// alone needs no object store, so the platform can give crawler-retry no
// S3 key (REQ-008 fix 22b).
func TestFetches(t *testing.T) {
	for _, tc := range []struct {
		stages []string
		want   bool
	}{
		{nil, true},
		{[]string{"profile"}, true},
		{[]string{"post"}, true},
		{[]string{"retry"}, false},
		{[]string{" retry", "post "}, true},
	} {
		if got := Fetches(tc.stages); got != tc.want {
			t.Errorf("%q: %v, want %v", tc.stages, got, tc.want)
		}
	}
}

// A component without an object-store key fails at once, naming the
// variables, instead of trying the store anonymously.
func TestOpenS3WithoutKey(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.S3.AccessKey, cfg.S3.SecretKey = "", ""
	_, err = OpenS3(context.Background(), cfg, "media")
	if err == nil || !strings.Contains(err.Error(), "S3_ACCESS_KEY") || !strings.Contains(err.Error(), "media") {
		t.Fatalf("no key: %v", err)
	}
}
