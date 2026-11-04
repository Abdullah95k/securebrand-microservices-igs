package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igtopics "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// The topic file covers every topic igs produces or consumes, and the
// chart hook's overrides apply: a replication factor and a partition cap.
func TestLoadTopics(t *testing.T) {
	doc, err := loadTopics(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Replicas != 3 {
		t.Fatalf("replicas %d", doc.Replicas)
	}
	have := map[string]int32{}
	for _, tp := range doc.Topics {
		have[tp.Name] = tp.Partitions
	}
	want := append([]string{igtopics.DLQ, igtopics.PersistDLQ, igtopics.Retry30s, igtopics.Retry5m, igtopics.TaskMedia}, igtopics.Entity...)
	for _, n := range want {
		if have[n] == 0 {
			t.Errorf("%s missing from %s", n, TopicsFile)
		}
	}
	capped, err := loadTopics(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if capped.Replicas != 1 {
		t.Fatalf("replicas override: %d", capped.Replicas)
	}
	for _, tp := range capped.Topics {
		if tp.Partitions > 3 || tp.Partitions < 1 {
			t.Errorf("%s: %d partitions under a cap of 3", tp.Name, tp.Partitions)
		}
	}
}

// migrate up, as the chart's pre-install and pre-upgrade hook runs it:
// schema ig, the registry seed and the partitions, and a second run (the
// next upgrade) changes nothing and does not fail.
func TestMigrateUpTwice(t *testing.T) {
	pub := testenv.Postgres(t)
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Postgres.DSN = pub.Config().ConnString()
	ctx := context.Background()
	for i := range 2 {
		if err := migrate(ctx, cfg, "up"); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	qs, err := igRegistry.List(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	active := map[string]bool{}
	for _, q := range qs {
		if q.Status == registry.StatusActive {
			active[q.Name] = true
		}
	}
	for _, n := range []string{"profile", "grid", "comments"} {
		if !active[n] {
			t.Errorf("seed query %s not active after migrate up: %+v", n, qs)
		}
	}
	// The place list is seeded once: the second run added nothing.
	seed, err := keywords.Seed()
	if err != nil {
		t.Fatal(err)
	}
	var kws int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM discovery_keywords`).Scan(&kws); err != nil || kws != len(seed) {
		t.Fatalf("%d place keywords after two runs, want the seed's %d (%v)", kws, len(seed), err)
	}
	var parts int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'ig'`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if parts == 0 {
		t.Fatal("no partitions in schema ig")
	}
	if err := migrate(ctx, cfg, "status"); err != nil {
		t.Fatal(err)
	}
}

// A keyword CSV: a header row, an enabled spelling and a disabled one.
func TestReadKeywordCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kw.csv")
	if err := os.WriteFile(path, []byte("term,language,group,enabled\nAl Mawsil,en,nineveh,true\nموصلي,ar,nineveh,false\nKalak,en\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ks, err := readKeywordFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []keywords.SeedKeyword{{Term: "Al Mawsil", Language: "en", Group: "nineveh", Enabled: true},
		{Term: "موصلي", Language: "ar", Group: "nineveh"}, {Term: "Kalak", Language: "en", Enabled: true}}
	if !slices.Equal(ks, want) {
		t.Fatalf("got %+v", ks)
	}
}
