package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
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
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/schemareg"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// TopicsFile is igs's topic list in the embedded configs.
const TopicsFile = "ig/topics.yaml"

func openDB(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	return pg.OpenSchema(ctx, cfg.Postgres.DSN, 4, store.IGSchema)
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
	pub, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
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
	n, err := store.IGLayout.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{
		MonthsBack: cfg.IG.Limits.MaxAgeCapDays/30 + 1, MonthsAhead: max(cfg.Scheduler.PartitionMonthsAhead, 1), DaysBack: 1, DaysAhead: 14})
	if err != nil {
		return err
	}
	fmt.Printf("schema %s migrated; registry seeded (%d queries); %d partitions created\n", store.IGSchema, len(seed.Queries), n)
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

func kafkaApply(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("kafka apply", flag.ExitOnError)
	pcap := fs.Int("partitions-cap", 0, "cap partition counts (local dev, one-node rehearsals)")
	repl := fs.Int("replicas", 0, "override the replication factor")
	_ = fs.Parse(args)
	doc, err := loadTopics(*repl, *pcap)
	if err != nil {
		return err
	}
	kcfg, err := clients.Kafka(cfg, "-igsctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	if err := kafka.EnsureTopics(ctx, p.Client(), cfg.Kafka.TopicPrefix, doc.Topics, doc.Replicas); err != nil {
		return err
	}
	fmt.Printf("%d topics applied\n", len(doc.Topics))
	return nil
}

// schemasApply registers the public ig topics' schemas, or with check only
// verifies they are compatible with what is registered.
func schemasApply(ctx context.Context, cfg *config.Config, check bool) error {
	sr := cfg.Kafka.SchemaRegistry
	if sr.URL == "" {
		return errors.New("kafka.schema_registry.url is not set")
	}
	c := &schemareg.Client{URL: sr.URL, Username: sr.Username, Password: sr.Password}
	res, err := schemareg.Apply(ctx, c, protosrc.FS, cfg.Kafka.TopicPrefix, igtopics.ValueSchemas, check)
	printJSON(res)
	return err
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
		defer f.Close()
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
