package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	igtopics "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
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

// The platform's content.* topics and subjects are left out unless asked
// for (a Kafka or Schema Registry of igs's own); igs's own are all kept.
func TestPlatformTopicsLeftOut(t *testing.T) {
	doc, err := loadTopics(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	own, skipped := ownTopics(doc.Topics, false)
	if !slices.Equal(skipped, igtopics.Content) {
		t.Errorf("skipped %v, want the content topics %v", skipped, igtopics.Content)
	}
	for _, s := range own {
		if platformOwned(s.Name) {
			t.Errorf("%s kept without --include-content", s.Name)
		}
	}
	if len(own)+len(skipped) != len(doc.Topics) {
		t.Errorf("%d kept + %d skipped of %d", len(own), len(skipped), len(doc.Topics))
	}
	all, none := ownTopics(doc.Topics, true)
	if len(all) != len(doc.Topics) || len(none) != 0 {
		t.Errorf("--include-content kept %d of %d, skipped %v", len(all), len(doc.Topics), none)
	}

	schemas := ownSchemas(false)
	for _, c := range igtopics.Content {
		if _, ok := schemas[c]; ok {
			t.Errorf("subject for %s registered without --include-content", c)
		}
	}
	if _, ok := schemas[igtopics.Post]; !ok {
		t.Errorf("igs's own %s subject left out", igtopics.Post)
	}
	if got := len(ownSchemas(true)); got != len(igtopics.ValueSchemas) {
		t.Errorf("--include-content: %d subjects, want %d", got, len(igtopics.ValueSchemas))
	}
}

// The dry run says what would change and that nothing was; an apply names
// the flag each held change needs.
func TestPrintTopicDiffs(t *testing.T) {
	diffs := []kafka.TopicDiff{
		{Topic: "igs.task.dlq", WantPartitions: 6, WantRetention: 336 * time.Hour, Changes: []string{"create"}},
		{Topic: "ig.post.v1", Exists: true, HavePartitions: 12, WantPartitions: 48, HaveRetention: 720 * time.Hour,
			WantRetention: 168 * time.Hour, Changes: []string{"add partitions", "lower retention"},
			Held: []string{"add partitions", "lower retention"}},
		{Topic: "ig.job.v1", Exists: true, HavePartitions: 12, WantPartitions: 12, HaveRetention: 168 * time.Hour, WantRetention: 168 * time.Hour},
	}
	var dry strings.Builder
	printTopicDiffs(&dry, diffs, []string{"content.post.v1"}, "", kafka.ApplyOptions{})
	for _, want := range []string{"would create", "would add partitions, lower retention", "12 -> 48", "720h -> 168h",
		"unchanged", "skipped: the platform's topic", "dry run: 1 to create, 1 existing with differences, 1 unchanged, 1 skipped. Nothing was changed"} {
		if !strings.Contains(dry.String(), want) {
			t.Errorf("dry run output lacks %q:\n%s", want, dry.String())
		}
	}
	diffs[0].Applied = []string{"create"}
	var applied strings.Builder
	printTopicDiffs(&applied, diffs, nil, "", kafka.ApplyOptions{Apply: true})
	for _, want := range []string{"done: create", "held: add partitions, lower retention (needs --grow-partitions and --set-retention)",
		"applied: 1 created, 0 changed, 1 with changes held back, 1 unchanged, 0 skipped"} {
		if !strings.Contains(applied.String(), want) {
			t.Errorf("apply output lacks %q:\n%s", want, applied.String())
		}
	}
}
