// Command igsctl is igs's operations CLI (fbctl's counterpart on schema ig
// and the ig topics).
//
//	igsctl migrate up|down|status        schema ig's migrations, registry seed and partitions
//	igsctl kafka apply                    create/grow topics from configs/ig/topics.yaml
//	igsctl schemas apply|check            register the ig.v1 schemas (BACKWARD_TRANSITIVE) / dry run
//	igsctl apikey create --name N --scopes jobs:write,profiles:write,data:read
//	igsctl apikey revoke --prefix P
//	igsctl registry list
//	igsctl registry set --file query.json
//	igsctl registry import --file phase0/out/registry.yaml   make every query of a registry file active
//	igsctl dlq replay [--reason R] [--max N] [--persist]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: igsctl [--config file] <command> [args]
commands:
  migrate up|down|status
  kafka apply [--partitions-cap N] [--replicas N]
  schemas apply | schemas check
  apikey create --name NAME [--scopes s1,s2] [--rate N]
  apikey revoke --prefix PREFIX
  registry list | registry set --file query.json | registry import --file registry.yaml [--keep-promoted]
  dlq replay [--reason R] [--max N] [--idle 10s] [--persist]
  discovery import --file accounts.txt [--origin google|facebook|youtube|manual]
  profiles import --file accounts.txt [--client NAME] [--interval 3h] [--paused] [--no-backfill]
  profiles pause --file accounts.txt | profiles resume --file accounts.txt`)
	os.Exit(2)
}

func main() {
	global := flag.NewFlagSet("igsctl", flag.ExitOnError)
	cfgPath := global.String("config", os.Getenv("IGS_CONFIG"), "config file")
	_ = global.Parse(os.Args[1:])
	args := global.Args()
	if len(args) < 1 {
		usage()
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	plog.Setup("igsctl", cfg.Log.Level, "text")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cmd, rest := args[0], args[1:]
	sub := ""
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		sub, rest = rest[0], rest[1:]
	}
	switch cmd + " " + sub {
	case "migrate up", "migrate down", "migrate status":
		err = migrate(ctx, cfg, sub)
	case "kafka apply":
		err = kafkaApply(ctx, cfg, rest)
	case "schemas apply", "schemas check":
		err = schemasApply(ctx, cfg, sub == "check")
	case "apikey create":
		err = apikeyCreate(ctx, cfg, rest)
	case "apikey revoke":
		err = apikeyRevoke(ctx, cfg, rest)
	case "registry list":
		err = registryList(ctx, cfg)
	case "registry set":
		err = registrySet(ctx, cfg, rest)
	case "registry import":
		err = registryImport(ctx, cfg, rest)
	case "dlq replay":
		err = dlqReplay(ctx, cfg, rest)
	case "discovery import":
		err = discoveryImport(ctx, cfg, rest)
	case "profiles import":
		err = profilesImport(ctx, cfg, rest)
	case "profiles pause", "profiles resume":
		err = profilesEnable(ctx, cfg, rest, sub == "resume")
	default:
		usage()
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "igsctl:", err)
	os.Exit(1)
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
