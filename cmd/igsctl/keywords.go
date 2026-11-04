package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
)

// discoveryKeywords runs `igsctl discovery keywords ...`, the shared Iraq
// place list discovery matches (ig.discovery_keywords):
//
//	seed                 load the embedded seed (fbs's iraq.yaml and igs's
//	                     additions); migrate up does this too
//	import --file F      load a seed file (YAML, the same format) or a CSV
//	                     of term,language[,group[,enabled]]
//	list [--language L] [--group G]
//	enable ID | disable ID
//
// Loading adds only keywords not listed yet; listed ones keep their state.
// Crawlers and the scheduler pick changes up within 5 minutes.
func discoveryKeywords(ctx context.Context, cfg *config.Config, args []string) error {
	if len(args) == 0 {
		return errors.New("discovery keywords: want seed, import --file F, list, enable ID or disable ID")
	}
	action, rest := args[0], args[1:]
	fs := flag.NewFlagSet("discovery keywords "+action, flag.ContinueOnError)
	file := fs.String("file", "", "seed YAML or CSV (term,language,group,enabled)")
	lang := fs.String("language", "", "list: only this language (en, ar, ckb, kmr)")
	group := fs.String("group", "", "list: only this group")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	var seed []keywords.SeedKeyword
	switch action {
	case "seed":
		seed, err = keywords.Seed()
	case "import":
		if *file == "" {
			return errors.New("discovery keywords import: --file is required")
		}
		seed, err = readKeywordFile(*file)
	case "list":
		ks, err := keywords.List(ctx, db, keywords.Filter{Language: *lang, Group: *group, Limit: 100000})
		if err != nil {
			return err
		}
		printJSON(map[string]any{"keywords": ks})
		return nil
	case "enable", "disable":
		if fs.NArg() != 1 {
			return fmt.Errorf("discovery keywords %s: want one keyword id (from list)", action)
		}
		id, err := strconv.ParseInt(fs.Arg(0), 10, 64)
		if err != nil {
			return fmt.Errorf("discovery keywords %s: id %q: %w", action, fs.Arg(0), err)
		}
		on := action == "enable"
		k, err := keywords.Set(ctx, db, id, keywords.Update{Enabled: &on})
		if err != nil {
			return err
		}
		printJSON(k)
		return nil
	default:
		return fmt.Errorf("discovery keywords: unknown action %q", action)
	}
	if err != nil {
		return err
	}
	n, err := keywords.ImportSeed(ctx, db, seed)
	if err != nil {
		return err
	}
	fmt.Printf("%d keywords read, %d new\n", len(seed), n)
	return nil
}

// readKeywordFile reads a seed YAML (.yaml, .yml) or a CSV of
// term,language[,group[,enabled]] (a header row starting "term" is skipped).
func readKeywordFile(path string) ([]keywords.SeedKeyword, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return keywords.ParseSeed(b)
	}
	r := csv.NewReader(strings.NewReader(string(b)))
	r.FieldsPerRecord = -1
	var out []keywords.SeedKeyword
	for line := 1; ; line++ {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if line == 1 && len(rec) > 0 && strings.EqualFold(strings.TrimSpace(rec[0]), "term") {
			continue
		}
		if len(rec) < 2 {
			return nil, fmt.Errorf("%s line %d: want term,language[,group[,enabled]]", path, line)
		}
		k := keywords.SeedKeyword{Term: strings.TrimSpace(rec[0]), Language: strings.TrimSpace(rec[1]), Enabled: true}
		if len(rec) > 2 {
			k.Group = strings.TrimSpace(rec[2])
		}
		if len(rec) > 3 && strings.TrimSpace(rec[3]) != "" {
			if k.Enabled, err = strconv.ParseBool(strings.TrimSpace(rec[3])); err != nil {
				return nil, fmt.Errorf("%s line %d: enabled %q: %w", path, line, rec[3], err)
			}
		}
		out = append(out, k)
	}
	return out, nil
}
