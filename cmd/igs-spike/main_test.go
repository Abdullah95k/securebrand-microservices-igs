package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/probe"
)

func TestUsername(t *testing.T) {
	for in, want := range map[string]string{
		"brand": "brand", "@brand": "brand", "https://www.instagram.com/brand/": "brand",
		"instagram.com/brand?hl=en": "brand", "  # a comment": "", "brand # note": "brand", "": "",
	} {
		if got := username(in); got != want {
			t.Errorf("username(%q) = %q, want %q", in, got, want)
		}
	}
}

// The command end to end against fakeig over the fingerprinted transport:
// it reads the profiles file, writes a report, and the report holds no
// username.
func TestProbeCommand(t *testing.T) {
	w := fakeig.NewWorld(fakeig.DefaultSpec())
	hs := httptest.NewServer(fakeig.NewServer(w, fakeig.Options{}))
	defer hs.Close()
	dir := t.TempDir()
	list := filepath.Join(dir, "canary.txt")
	var names []string
	for _, p := range w.Profiles() {
		names = append(names, "@"+p.Username)
	}
	if err := os.WriteFile(list, []byte("# canary\n"+strings.Join(names, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "report.json")
	if code := runProbe([]string{"--profiles", list, "--base", hs.URL, "--out", out, "--rps", "1000", "--max-grid-pages", "5"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rep probe.Report
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Summary.Bootstrapped != len(names) || rep.Summary.GridEnds != len(names) {
		t.Fatalf("summary %+v", rep.Summary)
	}
	for _, p := range w.Profiles() {
		if strings.Contains(string(b), p.Username) {
			t.Fatalf("the report names %s", p.Username)
		}
	}
	if fi, _ := os.Stat(out); fi.Mode().Perm() != 0o600 {
		t.Fatalf("report mode %v", fi.Mode().Perm())
	}
}
