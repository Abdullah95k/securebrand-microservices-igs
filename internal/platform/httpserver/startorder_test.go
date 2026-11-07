package httpserver

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// opens matches the calls that open a store and can wait on it.
var opens = regexp.MustCompile(`pg\.Open(Schema)?\(|clients\.(Redis|Kafka)\(|igapp\.OpenS3\(`)

// Every igs binary that serves starts its ops server before it opens a
// store, so a liveness probe never finds nothing listening while a busy
// database is retried (the production start of 7 Nov 2026). The serving
// function is the one that adds readiness checks.
func TestOpsStartsBeforeTheStores(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	cmds, err := filepath.Glob(filepath.Join(filepath.Dir(here), "..", "..", "..", "cmd", "igs-*", "main.go"))
	if err != nil || len(cmds) == 0 {
		t.Fatalf("no binaries found: %v", err)
	}
	checked := 0
	for _, path := range cmds {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		at := strings.Index(src, "ops.AddCheck(")
		if at < 0 {
			continue // not a server (igs-soak, igs-spike)
		}
		begin := strings.LastIndex(src[:at], "\nfunc ")
		end := strings.Index(src[at:], "\nfunc ")
		body := src[begin:]
		if end >= 0 {
			body = src[begin : at+end]
		}
		name := filepath.Base(filepath.Dir(path))
		start := strings.Index(body, "ops.Start()")
		if start < 0 {
			t.Errorf("%s: the serving function never calls ops.Start()", name)
			continue
		}
		if loc := opens.FindStringIndex(body); loc != nil && loc[0] < start {
			t.Errorf("%s: %s is called before ops.Start()", name, body[loc[0]:loc[1]])
		}
		checked++
	}
	if checked < 7 {
		t.Errorf("checked %d serving binaries, want the 7 (api, scheduler, crawler, persister, normalizer, media, registry-sync)", checked)
	}
}
