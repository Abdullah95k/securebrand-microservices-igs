package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	igapi "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// readLines reads one entry a line from file (- for standard input),
// skipping blank lines and # comments.
func readLines(file string) ([]string, error) {
	if file == "" {
		return nil, errors.New("--file is required")
	}
	in := os.Stdin
	if file != "-" {
		f, err := os.Open(filepath.Clean(file))
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	var out []string
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}

// profilesImport registers the profiles of a file (usernames, @names or
// profile links, one a line) for the 3-hourly schedule under an API client
// (--client, by default "igsctl"), and starts each new one's backfill, as
// POST /v1/profiles/bulk does.
func profilesImport(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("profiles import", flag.ExitOnError)
	file := fs.String("file", "", "file of usernames or profile links, one a line (- for standard input)")
	client := fs.String("client", "igsctl", "API client (key prefix) the profiles are registered under; created revoked if missing")
	every := fs.Duration("interval", 0, "time between scheduled runs (default: ig.schedule.interval)")
	paused := fs.Bool("paused", false, "register without scheduling runs (resume with igsctl profiles resume)")
	noBackfill := fs.Bool("no-backfill", false, "do not start backfills")
	_ = fs.Parse(args)
	entries, err := readLines(*file)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := clients.Redis(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	kcfg, err := clients.Kafka(cfg, "-igsctl")
	if err != nil {
		return err
	}
	p, err := kafka.NewProducer(kcfg)
	if err != nil {
		return err
	}
	defer p.Close()
	id, err := dispatch.SystemClient(ctx, db, *client)
	if err != nil {
		return err
	}
	reg := dispatch.Registration{ClientID: id, Interval: *every, Enabled: !*paused}
	if *noBackfill {
		f := false
		reg.Backfill = &f
	}
	d := &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: p}
	res := igapi.RegisterAll(ctx, d, cfg, reg, entries)
	printJSON(res)
	if len(res.Failed) > 0 {
		return errors.New("some profiles could not be registered")
	}
	return nil
}

// profilesEnable pauses (enabled false) or resumes the scheduled runs of
// the profiles in a file; their data and registrations stay.
func profilesEnable(ctx context.Context, cfg *config.Config, args []string, enabled bool) error {
	fs := flag.NewFlagSet("profiles pause|resume", flag.ExitOnError)
	file := fs.String("file", "", "file of usernames or profile links, one a line (- for standard input)")
	_ = fs.Parse(args)
	entries, err := readLines(*file)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	var refs, invalid []string
	for _, e := range entries {
		ref, err := task.NormalizeRef(strings.TrimPrefix(e, "@"))
		if err != nil {
			invalid = append(invalid, e)
			continue
		}
		refs = append(refs, ref)
	}
	// Resuming runs a profile at its next slot, not at once.
	tag, err := db.Exec(ctx, `UPDATE profile_schedule SET enabled = $2,
			next_run_at = CASE WHEN $2 THEN greatest(next_run_at, now() + make_interval(secs => interval_seconds * random() * 0.1)) ELSE next_run_at END
		WHERE profile_ref = ANY($1)`, refs, enabled)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"changed": tag.RowsAffected(), "not_registered": int64(len(refs)) - tag.RowsAffected(), "invalid": invalid,
		"at": time.Now().UTC()})
	return nil
}
