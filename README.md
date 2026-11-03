# securebrand-microservices-igs

**igs** scrapes public Instagram profiles while logged out, for SecureBrand. It was seeded from a copy of
[`securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices) (fbs, the Facebook
page scraper) at commit `f1377a9`. The neutral parts of that copy (scraping, crawl framework, platform)
became igs's own; the copied Facebook pipeline was removed once igs's suites covered the same code
(ADR 0004 §2, M10). fbs lives on in its own repository. The plan is in
[`docs/instagram/build-brief.md`](docs/instagram/build-brief.md) and progress in
[`docs/instagram/build-status.md`](docs/instagram/build-status.md); what was copied and ported is in
[`UPSTREAM.md`](UPSTREAM.md).

For each registered profile, igs collects:
- the profile (name, biography, counts, verification), with a snapshot each time it changes;
- every post in the age window: caption, hashtags, mentions, location, tagged users, coauthors, music,
  topics, AI label, like and comment counts, carousel children, video duration, and each reel's views
  from the reels tab;
- every top-level comment with its reply count (logged-out Instagram serves no replies);
- highlights, and media files once the analysis service consumes them (D5).

It reads every profile's newest posts every 3 hours (D2), the profile itself daily, and re-reads the
comments and counts of the last 7 days' posts. Profile discovery finds more profiles to register, with no
cap: Iraqi accounts, or accounts addressing an Iraqi audience, among those the registered profiles tag,
co-author with, mention, or hear from in comments
([how](docs/runbooks/igs-operations.md#profile-discovery)).

## Status

| Proven | Not yet proven |
|---|---|
| The whole pipeline (API → crawler → Kafka → persister → Postgres, finalizer, schedules, post refresh, media download and expiry, erasure) end to end against **fakeig**, a synthetic logged-out Instagram, compared field by field with its ground truth, also under crashes, rate limits and restarts (`make e2e`, `make soak`) | Behaviour against **real Instagram** at volume. The owner's probe (C2) and Phase 0 (C3, [`igs-phase0.md`](docs/runbooks/igs-phase0.md)) ran on ISP proxies: 348 profiles, 37.6 requests a profile, 0.62% blocks |
| The Helm chart's hooks and workloads in Docker (`make chart-smoke`), Kubernetes schemas (`make chart-check`) | Production sizing of ISP exits: igs reads at most exits × `ig.exit_rps` ([sizing](docs/runbooks/igs-operations.md#capacity-and-sizing)) |
| Rotation and failover under per-exit rate limits and flagged exits (`TestExitFailoverEqualsTruth`) | How long Instagram's per-IP limit lasts; `ig.rate_limit_rest` and `ig.exit_rps` are tuned in production |

## Components

| Binary | Role |
|---|---|
| `igs-api` | REST API (`api/openapi/igs.v1.yaml`): on-demand jobs, profile registration, data, erasure, admin |
| `igs-scheduler` | Leader-elected: the 3-hourly planner, post refreshes, job finalizer, partitions, AIMD, consumer lag, erasure |
| `igs-crawler --stages profile,post,retry` | Crawl stages: profile (profile query, grid, reels, highlights), post (post page, comments), retry relay |
| `igs-persister` | Writes `ig.*.v1` records to Postgres schema `ig`, quarantining what it cannot write |
| `igs-media --roles downloader,janitor` | Downloads media and expires it once acknowledged or at its TTL |
| `igs-registry-sync` | Follows Instagram's doc_id rotations on canary profiles ([runbook](docs/runbooks/igs-registry-sync.md)) |
| `igsctl` | Operations CLI: migrations, topics, schemas, API keys, registry, DLQ replay |
| `igs-spike` | The owner's probe (C2) and Phase 0 kit (C3), run without the pipeline |
| `fakeig`, `devs3` | A synthetic logged-out Instagram and an in-memory S3, for tests and rehearsals |

Data flows through Kafka: tasks on `igs.task.*`, entities on the public `ig.*.v1` topics (protobuf,
`api/proto/securebrand/ig/v1`), jobs, media and erasure events on `securebrand.common.v1` messages.
Valkey holds job accounting, leases, checkpoints and rate limits (every key starts with `igs:`).

## Quick start (Docker Compose, against fakeig)

```bash
make docker
```

```bash
make compose-up
```

```bash
docker compose -f deploy/compose/docker-compose.yml exec api igsctl apikey create --name dev
```

Then register fakeig's profiles (`profile_01` to `profile_05`) with the key printed above:

```bash
curl -H "Authorization: Bearer $KEY" -d '{"profile":"profile_01"}' localhost:8080/v1/profiles
```

The backfill starts at once; `GET /v1/jobs` shows it, and `GET /v1/profiles/profile_01/posts` the data.

## Configuration

Every key is in [`configs/default.yaml`](configs/default.yaml); igs reads its `ig:` section and the shared
`postgres` (schema `ig`), `redis`, `kafka` and `s3` sections. Files given with `--config` or `IGS_CONFIG`
(comma separated, later files win) override it, and environment variables override both:
`IGS_IG__IDENTITY_RPS=0.2` sets `ig.identity_rps`. Proxy tiers and credentials go in a separate file
(`phase0/proxies.example.yaml`); igs runs on ISP proxies only.

## Phase 0, before scraping real Instagram at volume

`make igs-phase0` checks each proxy tier on about 150 profiles: bootstrap, doc_ids from the page's
bundles, captured fixtures, a smoke run, the full run, and a GO or NO-GO report per tier
([`igs-phase0.md`](docs/runbooks/igs-phase0.md)). `make igs-phase0-dryrun` rehearses it against fakeig.
Its registry goes live with `igsctl registry import --file phase0/out/registry.yaml`.

## Running in Kubernetes

`deploy/helm/igs` installs every component, with KEDA scaling on consumer lag, hooks that run
`igsctl migrate up` and `igsctl kafka apply`, and optional alerts, dashboard, PodMonitor, ingress and
network policy. `examples/fakeig.yaml` rehearses an install; `examples/production.yaml` starts a real one.
A version tag (`v1.2.3`) publishes the images (`ghcr.io/abdullah95k/igs`) and the chart
(`oci://ghcr.io/abdullah95k/charts/igs`). Going live: [`igs-go-live.md`](docs/runbooks/igs-go-live.md);
operations, alerts and sizing: [`igs-operations.md`](docs/runbooks/igs-operations.md).

## Privacy and legal

igs reads only what Instagram shows logged out, and keeps what it collects forever
([retention](docs/runbooks/igs-operations.md#data-retention)). `ig.privacy.pii_mode: pseudonymized` replaces
commenters' identities with keyed hashes; `POST /v1/erasure-requests` deletes a subject's data and media
and keeps later crawls from storing it again. Legal signed off (C4, 2026-11-03).

## Development

| Command | What it does |
|---|---|
| `make test` | Unit tests (integration tests skip without infrastructure) |
| `make infra-up`, then `make test-integration` | Integration and end-to-end tests against Postgres 18, Redpanda and Valkey |
| `make e2e` | The end-to-end suite against fakeig, with ground truth |
| `make loadtest`, `make soak` | Opt-in load and soak tests ([numbers](docs/runbooks/igs-operations.md#capacity-and-sizing)) |
| `make chart-check`, `make chart-smoke` | The Helm chart's schemas, and its workloads in Docker |
| `make lint`, `make generate`, `make golden` | Linters, protobuf code, the parsers' golden files |

## Repository layout

| Path | Holds |
|---|---|
| `internal/ig/*` | Instagram: client, parsers, detection rules, crawl stages, persister, API, scheduler, media, erasure, registry sync, Phase 0 |
| `internal/scrape/*` | Neutral scraping: fingerprinted transport, detection engine, proxies, rate limits, sessions, GraphQL client, query registry, doc_id scanner, browser |
| `internal/crawl/*` | Crawl framework: job accounting, checkpoints, lanes, raw capture |
| `internal/platform/*` | Kafka, Postgres, Valkey, S3, metrics, config, leader election, schema registry |
| `internal/fakeig`, `internal/testenv` | The synthetic Instagram and the test infrastructure |
| `api/proto`, `gen/go` | Protobuf contracts and generated code |
| `migrations/ig` | Schema `ig` |
| `deploy/` | Helm chart, Docker image, Compose stack |
| `docs/` | ADRs, the Instagram build brief and status, runbooks |
