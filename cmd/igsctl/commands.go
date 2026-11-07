package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	protosrc "github.com/abdullah95k/securebrand-microservices-igs/api/proto"
	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dlq"
	igtopics "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/schemareg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// TopicsFile is igs's topic list in the embedded configs.
const TopicsFile = "ig/topics.yaml"

func openDB(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	return pg.OpenSchema(ctx, cfg.Postgres.DSN, 4, store.IGSchema, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
}

var igRegistry = registry.In(registry.IGTables)

// migrate applies schema ig's migrations, seeds the query registry and
// creates the partitions the scheduler's maintenance would (it keeps them
// ahead afterwards); down rolls back the last migration.
//
// The migrations run on a pool without schema ig in its search path: on a
// first install the schema does not exist yet, and pg.OpenSchema refuses
// it. The migrations set their own search path.
func migrate(ctx context.Context, cfg *config.Config, sub string) error {
	pub, err := pg.Open(ctx, cfg.Postgres.DSN, 2, pg.WithCA(cfg.Postgres.CAPEM), pg.Pooled(cfg.Postgres.Pooled))
	if err != nil {
		return err
	}
	defer pub.Close()
	switch sub {
	case "status":
		lines, err := store.MigrateIGStatus(ctx, pub)
		if err != nil {
			return err
		}
		fmt.Println(strings.Join(lines, "\n"))
		return nil
	case "down":
		return store.MigrateIGDown(ctx, pub)
	}
	if err := store.MigrateIGUp(ctx, pub); err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	b, err := configs.FS.ReadFile(cfg.IG.RegistrySeed)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(b)
	if err != nil {
		return err
	}
	if err := igRegistry.ApplySeed(ctx, db, seed); err != nil {
		return err
	}
	kseed, err := keywords.Seed()
	if err != nil {
		return err
	}
	added, err := keywords.ImportSeed(ctx, db, kseed)
	if err != nil {
		return err
	}
	n, err := store.IGLayout.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{
		MonthsBack: cfg.IG.Limits.MaxAgeCapDays/30 + 1, MonthsAhead: max(cfg.Scheduler.PartitionMonthsAhead, 1), DaysBack: 1, DaysAhead: 14})
	if err != nil {
		return err
	}
	fmt.Printf("schema %s migrated; registry seeded (%d queries); %d new place keywords; %d partitions created\n", store.IGSchema, len(seed.Queries), added, n)
	// Each component's own role, where the platform created it, gets its
	// grants on schema ig (REQ-008 fix 22b); the others keep the owner.
	granted, err := store.ApplyRoleGrants(ctx, db)
	if err != nil {
		return err
	}
	if len(granted) == 0 {
		fmt.Println("roles: none of igs's per-component roles exists; every component uses the owner")
	} else {
		fmt.Printf("roles: granted %s\n", strings.Join(granted, ", "))
	}
	return nil
}

// topicDoc is configs/ig/topics.yaml.
type topicDoc struct {
	Replicas int16             `yaml:"replicas"`
	Topics   []kafka.TopicSpec `yaml:"topics"`
}

// loadTopics reads igs's topic list with the overrides applied: a
// replication factor (0 keeps the file's) and a partition cap (0: none).
func loadTopics(replicas, partitionsCap int) (topicDoc, error) {
	var doc topicDoc
	b, err := configs.FS.ReadFile(TopicsFile)
	if err != nil {
		return doc, err
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return doc, err
	}
	if replicas > 0 {
		doc.Replicas = int16(replicas)
	}
	for i := range doc.Topics {
		if partitionsCap > 0 && doc.Topics[i].Partitions > int32(partitionsCap) {
			doc.Topics[i].Partitions = int32(partitionsCap)
		}
	}
	return doc, nil
}

// platformOwned reports a topic the shared platform owns: igs never creates,
// changes or registers it on a shared cluster (the content.* topics).
func platformOwned(topic string) bool {
	return strings.HasPrefix(topic, "content.")
}

// kafkaApply compares configs/ig/topics.yaml with the cluster. By default it
// is a dry run that prints the difference. With --apply it creates the
// missing topics; existing topics stay exactly as they are unless
// --grow-partitions or --set-retention allows that change. The content.*
// topics belong to the platform and are left alone, unless
// --include-content asks for them (a stack with its own Kafka: tests,
// rehearsals, the fake-Instagram soak).
func kafkaApply(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("kafka apply", flag.ExitOnError)
	pcap := fs.Int("partitions-cap", 0, "cap partition counts (local dev, one-node rehearsals)")
	repl := fs.Int("replicas", 0, "override the replication factor")
	apply := fs.Bool("apply", false, "make the changes (without it: a dry run that prints the difference)")
	grow := fs.Bool("grow-partitions", false, "allow adding partitions to existing topics")
	retention := fs.Bool("set-retention", false, "allow changing existing topics' retention (raising or lowering it)")
	content := fs.Bool("include-content", false, "include the platform's content.* topics (only on a Kafka of igs's own)")
	_ = fs.Parse(args)
	doc, err := loadTopics(*repl, *pcap)
	if err != nil {
		return err
	}
	specs, skipped := ownTopics(doc.Topics, *content)
	kcfg, err := clients.Kafka(cfg, "-igsctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	opt := kafka.ApplyOptions{Apply: *apply, GrowPartitions: *grow, SetRetention: *retention}
	diffs, err := kafka.ApplyTopics(ctx, p.Client(), cfg.Kafka.TopicPrefix, specs, doc.Replicas, opt)
	printTopicDiffs(os.Stdout, diffs, skipped, cfg.Kafka.TopicPrefix, opt)
	return err
}

// ownTopics drops the platform's topics unless includeContent.
func ownTopics(specs []kafka.TopicSpec, includeContent bool) (own []kafka.TopicSpec, skipped []string) {
	for _, s := range specs {
		if platformOwned(s.Name) && !includeContent {
			skipped = append(skipped, s.Name)
			continue
		}
		own = append(own, s)
	}
	return own, skipped
}

// printTopicDiffs prints one line per topic and a summary: what is
// different, what was done, and what was held back and by which flag.
func printTopicDiffs(w io.Writer, diffs []kafka.TopicDiff, skipped []string, prefix string, opt kafka.ApplyOptions) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tRETENTION\tRESULT")
	var create, changed, held, same int
	for _, d := range diffs {
		parts := fmt.Sprint(d.WantPartitions)
		ret := retentionText(d.WantRetention)
		if d.Exists {
			parts = fmt.Sprintf("%d -> %d", d.HavePartitions, d.WantPartitions)
			if d.WantRetention != 0 {
				ret = retentionText(d.HaveRetention) + " -> " + ret
			}
		}
		var res []string
		switch {
		case len(d.Changes) == 0:
			res = append(res, "unchanged")
			same++
		case !opt.Apply:
			res = append(res, "would "+strings.Join(d.Changes, ", "))
		default:
			if len(d.Applied) > 0 {
				res = append(res, "done: "+strings.Join(d.Applied, ", "))
			}
		}
		if slices.Contains(d.Applied, "create") || (!opt.Apply && slices.Contains(d.Changes, "create")) {
			create++
		} else if len(d.Applied) > 0 {
			changed++
		}
		if len(d.Held) > 0 {
			held++
			if opt.Apply {
				res = append(res, "held: "+strings.Join(d.Held, ", ")+" (needs "+heldFlags(d.Held)+")")
			}
		}
		res = append(res, d.Notes...)
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", d.Topic, parts, ret, strings.Join(res, "; "))
	}
	for _, t := range skipped {
		_, _ = fmt.Fprintf(tw, "%s\t-\t-\tskipped: the platform's topic (--include-content on a Kafka of igs's own)\n", prefix+t)
	}
	_ = tw.Flush()
	if !opt.Apply {
		_, _ = fmt.Fprintf(w, "dry run: %d to create, %d existing with differences, %d unchanged, %d skipped. Nothing was changed; --apply creates the missing topics.\n",
			create, countDiffering(diffs), same, len(skipped))
		return
	}
	_, _ = fmt.Fprintf(w, "applied: %d created, %d changed, %d with changes held back, %d unchanged, %d skipped\n", create, changed, held, same, len(skipped))
}

func countDiffering(diffs []kafka.TopicDiff) int {
	n := 0
	for _, d := range diffs {
		if d.Exists && len(d.Changes) > 0 {
			n++
		}
	}
	return n
}

// heldFlags names the flags that would allow the held changes.
func heldFlags(held []string) string {
	var f []string
	for _, h := range held {
		switch {
		case h == "add partitions" && !slices.Contains(f, "--grow-partitions"):
			f = append(f, "--grow-partitions")
		case strings.HasSuffix(h, "retention") && !slices.Contains(f, "--set-retention"):
			f = append(f, "--set-retention")
		}
	}
	return strings.Join(f, " and ")
}

func retentionText(d time.Duration) string {
	switch {
	case d < 0:
		return "forever"
	case d == 0:
		return "?"
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	default:
		return d.String()
	}
}

// schemasApply registers the schemas of igs's public ig topics, or with
// check only verifies they are compatible with what is registered. The
// content.* subjects are left out: the platform registers the shared
// contract. --include-content adds them, for a registry of igs's own.
func schemasApply(ctx context.Context, cfg *config.Config, check bool, args []string) error {
	fs := flag.NewFlagSet("schemas", flag.ExitOnError)
	content := fs.Bool("include-content", false, "also register the content.* subjects (only on a Schema Registry of igs's own)")
	_ = fs.Parse(args)
	sr := cfg.Kafka.SchemaRegistry
	if sr.URL == "" {
		return errors.New("kafka.schema_registry.url is not set")
	}
	c := &schemareg.Client{URL: sr.URL, Username: sr.Username, Password: sr.Password}
	res, err := schemareg.Apply(ctx, c, protosrc.FS, cfg.Kafka.TopicPrefix, ownSchemas(*content), check)
	printJSON(res)
	return err
}

// ownSchemas is igtopics.ValueSchemas without the platform's topics unless
// includeContent.
func ownSchemas(includeContent bool) map[string]string {
	m := make(map[string]string, len(igtopics.ValueSchemas))
	for t, f := range igtopics.ValueSchemas {
		if platformOwned(t) && !includeContent {
			continue
		}
		m[t] = f
	}
	return m
}

func apikeyCreate(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
	name := fs.String("name", "", "client name")
	scopes := fs.String("scopes", "jobs:write,profiles:write,data:read", "comma separated scopes")
	rate := fs.Int("rate", 0, "requests per minute (0: api.default_rate_per_minute)")
	_ = fs.Parse(args)
	if *name == "" {
		return errors.New("--name is required")
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	key, id, err := httpapi.CreateClient(ctx, db, *name, strings.Split(*scopes, ","), *rate)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"client_id": id, "name": *name, "scopes": strings.Split(*scopes, ","), "api_key": key,
		"note": "store this key now; it cannot be shown again"})
	return nil
}

func apikeyRevoke(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("apikey revoke", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix (the part between igs_ and the second _)")
	_ = fs.Parse(args)
	if *prefix == "" {
		return errors.New("--prefix is required")
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	tag, err := db.Exec(ctx, `UPDATE api_clients SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL`, *prefix)
	if err != nil {
		return err
	}
	fmt.Printf("%d key(s) revoked\n", tag.RowsAffected())
	return nil
}

func registryList(ctx context.Context, cfg *config.Config) error {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	qs, err := igRegistry.List(ctx, db)
	if err != nil {
		return err
	}
	printJSON(qs)
	return nil
}

func registrySet(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("registry set", flag.ExitOnError)
	file := fs.String("file", "", "JSON file with a registry query (name, doc_id, friendly_name, variables, parser, ...)")
	_ = fs.Parse(args)
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var q registry.Query
	if err := json.Unmarshal(b, &q); err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := igRegistry.Upsert(ctx, db, q); err != nil {
		return err
	}
	fmt.Printf("registry entry %s/%s saved\n", q.Name, q.DocID)
	return nil
}

// registryImport makes every query of a registry file active: Phase 0's
// registry.yaml (igs-spike phase0's docids step) after C3.
func registryImport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("registry import", flag.ExitOnError)
	file := fs.String("file", "", "registry YAML (phase0/out/registry.yaml from igs-spike phase0)")
	keep := fs.Bool("keep-promoted", false, "leave queries whose active entry did not come from the seed (registry-sync's promotions)")
	_ = fs.Parse(args)
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(b)
	if err != nil {
		return err
	}
	if q := registry.PlaceholderQueries(seed); len(q) > 0 {
		return fmt.Errorf("%s still has placeholder doc_ids for %v: run Phase 0's docids step first", *file, q)
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, q := range seed.Queries {
		if *keep {
			var source, doc string
			err := db.QueryRow(ctx, `SELECT source, doc_id FROM `+registry.IGTables.Queries+` WHERE name = $1 AND status = 'active'
				ORDER BY promoted_at DESC NULLS LAST LIMIT 1`, q.Name).Scan(&source, &doc)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if err == nil && source != "seed" {
				fmt.Printf("%-20s %s kept (active from %s)\n", q.Name, doc, source)
				continue
			}
		}
		q.Status, q.Source = registry.StatusActive, "import"
		if err := igRegistry.Upsert(ctx, db, q); err != nil {
			return fmt.Errorf("%s: %w", q.Name, err)
		}
		fmt.Printf("%-20s %s active\n", q.Name, q.DocID)
	}
	return nil
}

// dlqReplay puts dead-lettered tasks back on their stage's topic under new
// ids (rejoining their job while it runs), or with --persist the
// persister's quarantine back on its entity topics.
func dlqReplay(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
	reason := fs.String("reason", "", "only replay this x-reason")
	maxN := fs.Int("max", 1000, "maximum records")
	idle := fs.Duration("idle", 10*time.Second, "stop after this long without records")
	persisted := fs.Bool("persist", false, "replay the persister's quarantine (igs.persist.dlq) instead of the crawl DLQ")
	_ = fs.Parse(args)
	kcfg, err := clients.Kafka(cfg, "-igsctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	rdb, err := clients.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	cl, err := kgo.NewClient(append(kcfg.Opts(), kgo.ConsumerGroup(cfg.Kafka.TopicPrefix+dlq.Group(*persisted)),
		kgo.ConsumeTopics(cfg.Kafka.TopicPrefix+dlq.Topic(*persisted)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit())...)
	if err != nil {
		return err
	}
	defer cl.Close()
	res, err := dlq.Replay(ctx, cl, p, jobs.New(rdb), dlq.Options{Persisted: *persisted, Reason: *reason, Max: *maxN, Idle: *idle})
	printJSON(res)
	return err
}

// discoveryImport takes accounts found elsewhere (a file of usernames or
// profile links, one a line; - reads standard input) as discovery
// candidates, in batches of discovery.MaxIntake.
func discoveryImport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("discovery import", flag.ExitOnError)
	file := fs.String("file", "", "file of usernames or profile links, one a line (- for standard input)")
	origin := fs.String("origin", "manual", "where they were found: google, facebook, youtube, manual...")
	_ = fs.Parse(args)
	if *file == "" {
		return errors.New("--file is required")
	}
	in := os.Stdin
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	total := discovery.IntakeResult{Invalid: []string{}}
	var batch []string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		res, err := discovery.Intake(ctx, db, cfg.IG.Discovery, batch, *origin)
		if err != nil {
			return err
		}
		total.Added += res.Added
		total.Known += res.Known
		total.Invalid = append(total.Invalid, res.Invalid...)
		batch = batch[:0]
		return nil
	}
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if batch = append(batch, line); len(batch) == discovery.MaxIntake {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	printJSON(total)
	return nil
}
