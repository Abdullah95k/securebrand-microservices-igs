# igs operations and integration questionnaire

Written by the igs session from `Abdullah95k/securebrand-microservices-igs` `main` (`d50f3ee`), on 5 November 2026. Times are Iraq time (UTC+3). Secrets are named, never given. There are no proxy addresses and no real data in this file.

**Measured** numbers carry their date and conditions. Everything else is an **estimate** or **unknown**.

The production-readiness review I gave the owner on 4 November is included, unchanged, as the **Appendix** at the end of this file. It isn't repeated in the sections above. Where this file differs from it, **this file is newer**: one of its cost figures was too low (see section 13).

---

## 1. Components

The same image holds every binary. Ops port `:9090` on each (`/metrics`, `/healthz`, `/readyz`).

| Binary (chart component) | What it does | Long-running or job | Copies (chart default) | Scales out | State it holds |
|---|---|---|---|---|---|
| `igs-api` (api) | REST API on `:8080`: profiles (one at a time and bulk), jobs, posts, comments, metrics, media, highlights, discovery admin, keywords, erasure. API keys with scopes. | Long-running | 2 | Yes | None of its own (Postgres) |
| `igs-scheduler` (scheduler) | Leader-elected through a Postgres advisory lock. Runs the planner (due profiles by tier, daily profile reads, post re-reads), the finalizer (job status and completeness, `ig.job.v1`), the reaper (stalled jobs), partition maintenance, the daily budget, tier placement, discovery vetting and registration, and the eraser. | Long-running | 2 (1 active) | No: one leader, one standby | None (Postgres, Valkey) |
| `igs-crawler --stages profile` (crawler-profile) | Profile, grid, reels tab and highlights. | Long-running | 2, KEDA 2 to 12 | Yes | Identities, checkpoints and leases in Valkey |
| `igs-crawler --stages post` (crawler-post) | Post pages and comments. | Long-running | 4, KEDA 4 to 24 | Yes | Same |
| `igs-crawler --stages retry` (crawler-retry) | Moves tasks from the 30 s and 5 min retry topics back to work topics when due. | Long-running | 1 | Yes | None |
| `igs-persister` (persister) | `ig.*` to Postgres schema `ig`. Writes history only when a value changes. Bad records go to `igs.persist.dlq`. | Long-running | 2, KEDA 2 to 8 | Yes | None |
| `igs-normalizer` (normalizer) | `ig.*` to igs's draft `content.*` topics. | Long-running | 2, KEDA 2 to 8 | Yes | None |
| `igs-content-writer` (content-writer) | `content.*` to ClickHouse. | Long-running | **Off** until ClickHouse exists | Yes | None |
| `igs-media --roles downloader` (media-downloader) | Downloads media files, but only for jobs that ask (`include_media`, **off**). Announces `ig.media.ready.v1`. | Long-running | 2, KEDA 2 to 12 | Yes | None (S3) |
| `igs-media --roles janitor` (media-janitor) | Deletes files once analysis acknowledges them (`ig.media.processed.v1`), or after an optional TTL (`media.ttl: 0` means none). | Long-running | 1 | No need | None |
| `igs-registry-sync` (registry-sync) | Keeps Instagram's GraphQL query IDs current. It reads canary profiles' pages and scripts, then tests and promotes new IDs. | Long-running | 1 (Valkey lock) | No | Registry rows in Postgres |
| `igsctl` (Helm hook Jobs) | `migrate up/down/status`, `kafka apply`, `schemas apply/check`, `clickhouse migrate`, `apikey`, `registry`, `dlq replay`, `profiles`, `keywords`. | Jobs and CLI | Per install or upgrade | - | - |
| `igs-spike` | Phase 0 kit: the probe, `phase0` runs, the tagged-tab test. **Not deployed.** The owner runs it by hand. | One-off | - | - | Local output files |
| `fakeig`, `devs3` | Fake Instagram and local S3, for tests and staging. | Test only | - | - | - |

---

## 2. Packaging and deployment

1. **Images.**
   - One image for all binaries, `ghcr.io/abdullah95k/igs:<version>` (distroless, non-root), plus `<version>-browser` with Chromium.
   - Both are built by `.github/workflows/release.yml` on a `v*.*.*` tag, together with the chart (`oci://ghcr.io/abdullah95k/charts/igs`).
   - CI builds both images on every change, but doesn't push them.
   - **No release has been tagged yet, so no image is published.**
2. **Deployment.**
   - Helm chart `deploy/helm/igs`:
     - a Deployment per component;
     - hook Jobs;
     - KEDA scaling on consumer lag;
     - PrometheusRule (29 alerts) and a Grafana dashboard, both off by default;
     - a NetworkPolicy, off by default and ingress only.
   - Examples: `examples/fakeig.yaml` (rehearsal) and `examples/production.yaml`.
   - Docker Compose: `deploy/compose/igs.yml`.
   - Tested in CI:
     - `make chart-check` validates the rendered chart with kubeconform, CRDs included;
     - `make igs-chart-smoke` runs the chart's hooks and Deployments in Docker against fakeig, with three profiles' backfills.
   - **Never installed on a real Kubernetes cluster.**
3. **Start-up.** The Helm hooks run, in order:
   1. `igsctl migrate up` (goose, schema `ig`, migrations `00001` to `00008`, partitions created ahead);
   2. `igsctl kafka apply` (`configs/ig/topics.yaml`);
   3. `igsctl schemas apply` (`hooks.schemas`, off by default but **on in `examples/production.yaml`**; see section 4);
   4. `igsctl clickhouse migrate`, when ClickHouse is enabled.

   `paused: true` stops every hook and scales the components down, which is used for restores.
4. **Where it runs today.**
   - **Nowhere as a service.** No igs component runs anywhere long-term.
   - The owner ran the Phase 0 tools (`igs-spike`) by hand on the shared pilot server, the "Phase 0 droplet":
     - probe and Phase 0 runs, 31 October to 1 November;
     - the tagged-tab test, 5 November.
   - These were one-off runs of an hour or two at most, with no Postgres, Kafka or Valkey of igs's own.
   - **That server still holds the runs' output** (CSV and JSON under `phase0/out-ig/`), which is real collected data. The owner also has the CSV files.

---

## 3. Dependencies

| Dependency | Version | Used for | Shared with another service? | Size today, and growth |
|---|---|---|---|---|
| Postgres | 18 in CI (16 works locally) | Schema `ig`: content history, schedule, jobs, registry, discovery, API keys, erasure | Can share a server with fbs: igs uses its own schema `ig` and its own advisory-lock IDs | 0 (not deployed). **Estimate:** 20 to 200 MB a day per 1,000 accounts; backfill 1.5 to 3 GB once |
| Kafka or Redpanda, with Schema Registry | Redpanda in CI; any Kafka 3.x | Work topics `igs.*` (tasks, retries, dead letters), platform topics `ig.*`, draft `content.*` | Can share a cluster: topic names never collide with fbs's (a test checks this); `kafka.topic_prefix` is available. Plain protobuf on the wire; the registry is used for compatibility checks only | 0. **Note:** `configs/ig/topics.yaml` creates **27 topics, 915 partitions, replication 3** (2,745 partition replicas). That is sized for 300,000 accounts and too many for a small managed cluster. Set lower for the pilot and the first months |
| Valkey or Redis | Valkey 8 in CI | Exit pacing and rests, kept identities, checkpoints, leases, done markers, rate limits, budget | Can share: every key starts with `igs:` | Small. Done markers at 300,000 accounts: about 58 million keys, about 5 GB (sized in `igs-operations.md`) |
| Object storage (S3) | Any S3 API (minio client) | `igs-raw` (every raw response, kept forever); `igs-media` (media until analysed) | Separate buckets per service recommended | 0. **Estimate:** raw about 2 to 4 GB a day per 1,000 accounts, kept forever |
| ClickHouse | 25.8 in CI | Shared content store (`migrations/clickhouse`), via `igs-content-writer` | Meant to be the platform's shared store | 0, off |
| Browser (Chromium) | In the `-browser` image | Fallback for page loads only | No | **Off** (`ig.browser.enabled: false`) |
| GPU or AI APIs | None | - | - | - |
| Other | None | KEDA and Prometheus Operator are optional | - | - |

---

## 4. Kafka

Replication is 3 on every topic. Partitions are as in `configs/ig/topics.yaml`.

| Topic | Produce or consume | Key | Value (message type) | Partitions | Retention | Consumer group |
|---|---|---|---|---|---|---|
| `igs.task.profile.p0`, `.p1`, `.p2` | Scheduler and API produce; crawler-profile consumes | Profile | `igs.task.v1.Task` | 24, 48, 24 | 72 h | `igs-ig-crawler-profile-p0`, `-p1`, `-p2` |
| `igs.task.post.p0`, `.p1`, `.p2` | Crawler-profile produces; crawler-post consumes | Post | `Task` | 48 each | 72 h | `igs-ig-crawler-post-<lane>` |
| `igs.task.media` | Crawlers produce; media-downloader consumes | Media | `Task` | 24 | 72 h | `igs-ig-media-downloader` |
| `igs.task.retry.30s`, `.5m` | Crawlers produce; crawler-retry consumes | Task | `Task` | 12 each | 48 h | `igs-ig-retry-relay` |
| `igs.task.dlq` | Crawlers produce; `igsctl dlq replay` consumes | Task | `Task` | 6 | 336 h (14 days) | `igs-ig-dlq-replay` |
| `igs.persist.dlq` | Persister produces; `igsctl dlq replay --persist` consumes | Original key | The original record | 3 | 336 h | `igs-ig-persist-dlq-replay` |
| `ig.profile.v1` | Crawlers produce; persister and normalizer consume | user pk | `securebrand.ig.v1.Profile` | 24 | 168 h | `igs-ig-persister`, `igs-ig-normalizer` |
| `ig.post.v1` | Same | media pk | `Post` | 48 | 168 h | Same |
| `ig.post_metrics.v1` | Same | media pk | `PostMetrics` | 48 | 168 h | Same |
| `ig.comment.v1` | Same | **post** media pk | `Comment` | 96 | 168 h | Same |
| `ig.media.v1` | Same | media ID | `Media` | 24 | 168 h | Same |
| `ig.highlight.v1` | Same | user pk | `Highlight` | 12 | 168 h | Same |
| `ig.media.ready.v1` | Media-downloader produces; **analysis** consumes | media ID | `securebrand.common.v1.MediaReady` | 24 | 168 h | (analysis) |
| `ig.media.processed.v1` | **Analysis** produces; media-janitor consumes | media ID | `common.v1.MediaProcessed` | 24 | 168 h | `igs-ig-media-janitor` |
| `ig.media.deleted.v1` | Janitor produces | media ID | `common.v1.MediaDeleted` | 12 | 168 h | (none today) |
| `ig.job.v1` | Scheduler produces | job ID | `common.v1.JobEvent` | 12 | 168 h | (none today) |
| `ig.erasure.v1` | Scheduler (eraser) produces; persister consumes | Subject | `common.v1.Erasure` | 6 | 720 h | `igs-ig-persister` |
| `content.post.v1`, `.comment.v1`, `.account.v1`, `.metrics.v1`, `.media.v1` | Normalizer produces; content-writer consumes | `instagram:<native id>` (a comment's key is its post) | igs's draft `securebrand.content.v1` messages | 48, 96, 24, 96, 24 | 168 h | `igs-content-writer` |

**Schema Registry.**
- `igsctl schemas apply` registers one subject per public topic, `<topic>-value`, as `BACKWARD_TRANSITIVE`, then checks every later change against it.
- **That list includes the five `content.*` topics.** So running it against a registry shared with another service would register `securebrand.content.*`. That's against the standing rule, and it would conflict with analysis's `content.item.v1` schema, which uses the same package name.
- **Nothing is registered anywhere today**, because igs isn't deployed.
- `hooks.schemas` is off by default, **but `examples/production.yaml` turns it on.**
- Proposed fix (not done): leave the `content.*` subjects out of `schemas apply` unless a flag asks for them, and set `hooks.schemas: false` in the production example until the master publishes v1. The fix is in `igs-gaps.md`, E3.

**Expected rate per 1,000 accounts** (estimates, 3-hour tier):
- tasks: about 20,000 a day;
- `ig.post_metrics.v1`: **one event per reel per visit**, because the reels tab is re-read a year back each time. For accounts with about 100 to 1,000 reels a year, that's 0.8 to 8 million events a day (10 to 90 a second). The same number goes to `content.metrics.v1`;
- every other `ig.*` topic together: about 15,000 to 250,000 events a day (posts, comments, profiles, media);
- `content.*` otherwise mirrors `ig.*`.

Postgres stores a reel's metrics only when they change. **But `igs-content-writer` writes every observation to ClickHouse `post_metrics`**, so at 10,000 accounts that's 8 to 80 million rows a day, most of them unchanged. The reels fix in `igs-gaps.md` cuts both by about 20 times.

**Idempotency:**
- Every event ID is a hash of the entity and its content, so a repeat gets the same ID.
- The persister upserts, and only writes when something changed.
- ClickHouse uses `ReplacingMergeTree(version)`, so a redelivered event has no extra effect.
- Crawler tasks carry done markers, so a redelivered task isn't run twice within 96 h.

---

## 5. Storage

**Postgres (schema `ig`).** Partitioned by month where noted; partitions are created ahead by the scheduler. Rows today: 0, since nothing is deployed. Growth figures are estimates per 1,000 accounts.

| Table | Purpose | Partitioned | Growth per day (estimate) |
|---|---|---|---|
| `profiles`, `profile_snapshots` | Account (latest) and its changes | Snapshots: yes | 1,000 rows; snapshots only on change |
| `posts`, `post_index`, `post_owners`, `post_edits` | Posts (and highlight items), lookup, owners and co-authors, replaced captions | `posts`: yes | About 2,100 posts |
| `post_metrics` | Reels' views, likes and comments on each change | Yes | Up to the account's reels × visits; only changes are stored |
| `comments`, `comment_authors`, `comment_edits`, `comment_metrics` | Comments, their authors, replaced texts, likes and replies on change | Yes (not authors) | 10,000 to 200,000 comments |
| `media`, `media_acks` | Media metadata and file lifecycle; acknowledgements | No | About 5,000 rows |
| `highlights` | The highlights list | No | Small |
| `jobs`, `crawl_cursors`, `post_refresh`, `profile_schedule`, `profile_registrations` | Working state | No | About 8,000 jobs a day (kept forever since REQ-008 fix 10) |
| `queries`, `client_params`, `registry_sync_runs` | GraphQL registry | No | Small |
| `discovery_candidates`, `discovery_vets`, `discovery_sources`, `discovery_keywords` | Discovery | No | Depends on discovery being on |
| `api_clients`, `erasure_requests`, `suppression` | API keys, privacy | No | Small |

Retention:
- Content and history are **kept forever** (owner, 3 November): `privacy.retention.comments_days`, `metrics_days` and `authors_inactive_days` are 0.
- What still expired: jobs after 90 days, deleted media rows after 30 days. *Since REQ-008 fix 10 (6 November), nothing expires by age: `jobs_days` and `media_rows_days` are 0.*

**Valkey.** Every key starts with `igs:`.

| Keys (after `igs:`; `<ns>` is the topic prefix, empty by default) | Purpose | Lifetime |
|---|---|---|
| `<ns>exit:rests` | Exit rests shared by every pod | As long as each rest (10 to 30 min, longer when repeated) |
| `<ns>identities:<exit>` | Parked identities: cookies and tokens, never proxy credentials. **`<exit>` is `tier\|host:port`** (D4) | 1 h |
| `rl:<key>` | Rate limiters (per exit, per identity) | Seconds |
| `<ns>aimd:w:<time>`, `<ns>aimd:global_rps` | Adaptive fleet-wide rate | Minutes |
| `<ns>budget:d:<day>`, `<ns>budget:s:<slice>` | Daily budget counters | 1 day |
| `ckpt:<lineage>:<seq>` | Task checkpoints | Until the task ends |
| `done:<task>` | Done markers | 96 h |
| `lease:<scope>` | Task leases | 120 s, renewed |
| `pc:<post>`, `tc:<comment>` | Last comment and reply counts seen, to skip unchanged re-reads | 45 days |
| `{job:<id>}:*`, `jobs:finalize` | Job progress and the finalizer's queue | Until the job is finalized |
| API client rate limits | Per API key | A minute |
| `ig-registry-sync` | Lock | Short |

**Object storage.**

| Bucket and prefix | Holds | Size today | Growth (estimate, per 1,000 accounts) | Lifecycle rules |
|---|---|---|---|---|
| `igs-raw`, `raw/` and `raw-failures/` | Every raw response, as zstd JSON lines | 0 | 2 to 4 GB a day | None: kept forever |
| `igs-media`, `ig/` | Media files until analysed | 0 (`include_media` off) | Unknown until media is turned on | Expiry rule only if `media.ttl` is set (0 means none) |

**Which state must be backed up, and which can be rebuilt.**
- Postgres schema `ig`: **must be backed up.** It holds the history and schedule, and can't be re-collected.
- The raw bucket: **must be backed up.**
- The author HMAC key: **must be kept in two places.**
- Valkey: rebuilds itself in minutes; losing it costs fresh page loads.
- Kafka: a pipe; its 7-day retention bridges a restore.
- ClickHouse: can be rebuilt from Kafka for 7 days, but should still be backed up.

The runbook is `docs/runbooks/igs-backups.md`, and the restore drill is tested in CI (`internal/ig/e2e/backup_test.go`).

---

## 6. Network

**Inbound:**
- `:8080` is the REST API (`igs-api` only).
- `:9090` is the ops port on every binary: `/metrics`, `/healthz`, `/readyz`.

**Outbound.** Through the proxy tiers unless noted:

| Host | Why |
|---|---|
| `www.instagram.com` | Profile and post pages, web GraphQL, the reels tab |
| `static.cdninstagram.com` | JavaScript bundles, read by registry-sync only |
| `*.cdninstagram.com`, `*.fbcdn.net` (for example `scontent.cdninstagram.com`) | Media and profile pictures, only when media download is on |
| `l.instagram.com` | Appears only inside bio-link data; **igs never calls it** |

Inside the cluster, igs also calls Postgres, Kafka, the Schema Registry, Valkey, S3 and ClickHouse.

**Proxies.**
- Tiers are set in `proxies.yaml` (`ig.proxy.tiers`); the values themselves are secret.
- The Phase 0 runs used the pilot's 10 ISP IPs; a residential tier was within 10% on every cost.
- 3 of the 10 ISP IPs were flagged: each page load on them went to the login page.
- **Measured safe rate** (1 November, ISP): one IP was rate-limited after 1,100 to 1,300 requests at about 0.3 a second. igs therefore paces each exit at 0.25 requests a second fleet-wide (`exit_rps`) and retires identities after 500 to 750 requests, so exits rest in turn.
- How long one exit can keep up any given rate over days is **unknown**: the pilot must measure it.
- The provider is the owner's. igs doesn't know the monthly cost.

---

## 7. Configuration and secrets

**Configuration.**
- Read from `configs/default.yaml` (embedded), then optional files (`--config`), then environment variables with the prefix `IGS_` (`__` separates levels).
- `Config.Validate` refuses to start on a bad value and says which one.

Important settings and their defaults (`ig:` section):

| Setting | Default |
|---|---|
| `schedule.tiers` | hot 10m, warm 1h, long_tail 3h; `min_interval` 5m; auto warm at 2 or more posts a day over 14 days |
| `schedule.profile_refresh` | 24h |
| `schedule.post_refresh_window` | 168h, with `post_refresh_per_run` 30 |
| `limits.max_age_days` | 365 |
| `limits.include_comments` | true |
| `limits.include_reels` | true |
| `limits.include_highlights` | true |
| `limits.include_media` | false |
| `limits.max_comments_per_post` | 0 (no cap) |
| `exit_rps` | 0.25 |
| `identity_rps` | 0.33 |
| identity life | 500 to 750 requests |
| rests | `rate_limit_rest` 10m, `checkpoint_rest` 30m, `page_wall_rest` 30m |
| `block_hops` | 2 |
| `browser.enabled` | false (`max_concurrent` 2, `max_share` 0.02) |
| `capture_raw` | all |
| `budget` | enabled; the requests and bytes limits are 0, meaning none set |
| `discovery.enabled` | **false in `default.yaml`, true in the chart's values**. `threshold` 4, `min_priority` 2, `vet_in_flight` 50, **`max_new_per_day` 0, meaning no cap** |
| `privacy.pii_mode` | full |
| `crawler.max_attempts` | 5 (`max_total_attempts` 50) |
| `lease_ttl` | 120s |
| `scheduler.reaper_after` | 30m |

**Secrets (names only).** All are read from environment variables or files mounted from Kubernetes secrets:
- `IGS_POSTGRES__DSN` and `IGS_POSTGRES__READ_DSN`;
- `IGS_REDIS__PASSWORD`;
- `IGS_KAFKA__SASL__USERNAME` and `IGS_KAFKA__SASL__PASSWORD`;
- `IGS_KAFKA__SCHEMA_REGISTRY__USERNAME` and `IGS_KAFKA__SCHEMA_REGISTRY__PASSWORD`;
- `IGS_S3__ACCESS_KEY` and `IGS_S3__SECRET_KEY`;
- `IGS_IG__PRIVACY__AUTHOR_HMAC_KEY`;
- `IGS_CLICKHOUSE__PASSWORD`;
- `proxies.yaml`, a file holding `ig.proxy.tiers`;
- the KEDA Kafka authentication secret;
- TLS certificate files, if used.

Are any logged?
- Proxy credentials are redacted in logs.
- **But exits are named by `host:port`**, in logs ("session: exit rests", "proxy: exit"), in a metric label (`breaker_state{name="proxy:<tier>|<host:port>"}`) and in Valkey keys. That breaks D4 ("never by address") and needs a fix (`igs-gaps.md`, D4).
- No other secret is logged.

---

## 8. Resources

**Measured (fakeig, not Instagram):**
- Load test, 2 November, on 4 CPUs: 150 profiles × 40 posts read at once in 21 s. That's 5,250 posts and 155,652 comments, at 231 GraphQL requests a second.
- The persister alone took 15,200 new comment rows a second; it is the first stage to back up.
- Soak: 4 minutes locally, 55 jobs, goroutines flat, ending equal to fakeig's ground truth.
- The `soak` workflow (90 minutes by default) **has never been run in CI**.
- **Memory and CPU per component were not recorded in either run.**

**Not measured under real traffic.** There are no real-traffic numbers for CPU, memory, disk or network per component.

**Limits set in the chart** (requests and limits, chosen by design and **not measured**):

| Component | Requests | Memory limit |
|---|---|---|
| api | 250m CPU, 256Mi | 512Mi |
| scheduler | 100m, 128Mi | 256Mi |
| crawler-profile | 500m, 512Mi | 2Gi |
| crawler-post | 500m, 512Mi | 2Gi |
| crawler-retry | - | 256Mi |
| persister | - | 1Gi |
| normalizer | - | 512Mi |
| content-writer | - | 1Gi |
| media-downloader | - | 1Gi |
| registry-sync | - | 1Gi |

- Every pod has a memory limit, so a runaway pod is killed alone.
- Pods run non-root (65532) with a read-only root filesystem and every capability dropped.

**Growth per 1,000 accounts:** network as in section 13. CPU is expected to be small (fbs's crawler is about 80 MB; igs uses the same HTTP client), but **must be measured** in the pilot.

**How to measure:** see `igs-gaps.md`, "Plans": memory and soak.

---

## 9. Observability

**Health.** `/healthz` reports the process is alive. `/readyz` checks each binary's own dependencies: Postgres, Kafka, Valkey, S3 or ClickHouse, as that binary uses them.

**Metrics.**
- Prefix `igs_`, 58 metric variables.
- Key ones for "is it working":
  - `igs_ig_requests_total` by outcome and query;
  - `igs_ig_response_bytes` and `igs_proxy_bytes_total`;
  - `igs_exit_rests_total`;
  - `igs_identity_hops_total`, `igs_identity_retired_total` and `igs_identity_unparked_total`;
  - `igs_parse_missing_field_total` (drift);
  - `igs_kafka_consumer_lag`;
  - `igs_persist_rows_total` and `igs_persist_quarantined_total`;
  - `igs_schedule_lag_seconds`;
  - `igs_discovery_sightings_total` and `igs_discovery_vets_total`;
  - `igs_content_written_total`;
  - `igs_media_download_total`;
  - `igs_registry_sync_last_success_timestamp_seconds`.
- There are also 10 recording rules (`igs:ig_*`).

**Logs:**
- Structured JSON (`slog`).
- They never contain content, comment text, credentials or cookies: only IDs, counts and reasons.
- They do contain exit `host:port` (see section 7) and profile IDs or usernames in some lines (for example "profile", and the error of a failed read).

**Alerts and dashboards:**
- 29 PrometheusRule alerts, each linked to a section of `docs/runbooks/igs-operations.md`:

  | Area | Alerts |
  |---|---|
  | Blocks and exits | block rate high and critical; exits mostly resting; identities retiring fast; proxy bucket open; query breaker open |
  | Pipeline | DLQ growing; persister, normalizer and content-writer lag; persister quarantine; tasks parked; crawlers idle with backlog |
  | Schedule and jobs | schedule lag; on-demand lag; scheduler without a leader; jobs failing; jobs partial |
  | Parsing and registry | schema drift; optional-field drift; registry stale; registry query failing; registry-sync not running |
  | Other | media downloads failing; media TTL expiries; budget holding backfill; browser fallback saturated and failing; API errors |

- Dashboard: "igs / Instagram overview" (`igs-ig-overview.json`).
- Both are off by default in the chart's values.

**Health check for the owner.**
- `docs/runbooks/igs-operations.md`, "Key signals", lists what good looks like:
  - block rate under 5%;
  - no exit resting most of the time;
  - schedule lag under one interval;
  - DLQ near 0;
  - persister lag under a minute.
- There is no single read-only command list like fbs's `pilot.md` step 4 yet. That's a gap (F5): `igs-gaps.md`.

---

## 10. Reliability

- **Retries:**
  - A task retries through the 30 s and 5 min retry topics, at most 5 times (`max_attempts`) and 50 times in all across yields (`max_total_attempts`). Then it goes to `igs.task.dlq`.
  - Persister failures go to `igs.persist.dlq`.
  - `igsctl dlq replay` re-queues tasks under new IDs that rejoin their running job.
- **Stuck jobs.** The reaper ends jobs with no progress for 30 min (`scheduler.reaper_after`) and cancels their remaining tasks.
- **Circuit breakers:**
  - per proxy bucket (`tier|host:port`, or the /24 for static IPs);
  - per GraphQL query.
  - A failing exit or query is paused, with doubling cooldowns up to 30 min.
- **What survives a restart:**
  - checkpoints (cursor and phase) under a lease in Valkey, so another pod resumes the task;
  - exit rests (shared fleet-wide in Valkey);
  - kept identities: a stopping pod parks its idle identities for 1 h, and the next pod on the same exits picks them up instead of loading pages again;
  - the schedule, tiers and cursors (in Postgres).
- **Graceful shutdown.** On SIGTERM, a crawler stops taking tasks, finishes or checkpoints in-flight work and parks its identities, within the drain time of 75 s (pod grace period 90 s).
- **Backpressure:**
  - Kafka consumers only take more work as they finish; KEDA adds pods on lag.
  - The persister retries Postgres, then quarantines bad records.
  - The scheduler doesn't start new runs past the lane caps (p2 at most 96 of 240 slots, 40%; `lane_min` 12) or the budget.
- **When Instagram blocks:**
  - A rate limit rests the exit 10 min; a checkpoint or challenge 30 min; a login wall on a page load 30 min.
  - The task moves to another exit, at most `block_hops` (2) times. Then it waits or retries later.
  - A post page walled for three fresh identities is counted (`posts_gated`). The post is stored and published from the grid's fields, as unavailable (`login_wall`), with no counts (REQ-008 fix 15).
  - Every block is counted by type. Nothing tries to solve a challenge.
- **When Kafka, the database or Valkey is down:**
  - `/readyz` fails.
  - Crawlers stop taking tasks (their output can't be published).
  - The persister holds its offsets and retries.
  - The scheduler loses its lock and stops planning.
  - **Without Valkey, the shared rate limiter fails open** (`internal/scrape/ratelimit/ratelimit.go`: "redis unavailable, failing open"). Each identity keeps its own pace inside its process (0.33 a second plus jitter), but the fleet-wide per-exit limit and the shared rests are gone. Several pods could then use one exit faster than `exit_rps`. Checkpoints and leases also stop. This is a gap (`igs-gaps.md`, B6).
  - Nothing is lost: Kafka holds the work.

---

## 11. Platform access and policy

- **Logged out, always.** No Instagram accounts are used anywhere.
- **Bot checks.** Said plainly, igs:
  - **presents as a Chrome browser**: Chrome TLS fingerprints (`chrome_152`, `chrome_146`), a Chrome user agent and matching headers. The browser fallback, when on, sets client hints to match. It doesn't hide `navigator.webdriver` or other automation flags;
  - **builds the same request form a browser sends**: it copies the page's tokens (`lsd`, `csrftoken`, `__spin_r`, `__hsi`, `__rev`), computes `jazoest` (a checksum of a page token, as the page's own script does), and keeps a session ID and request counter. It doesn't compute the browser's client-state fields (`__dyn`, `__csr`); those are sent only if the registry sets them, and it doesn't today;
  - **after a login wall or rate limit on one IP, retries the work on another IP** (`block_hops` 2), and a page load is retried on up to 3 identities;
  - **jitters its timing** (±40% on each identity's pace);
  - **solves no CAPTCHA or challenge and generates no anti-bot token.** A challenge pauses the exit and is counted.

  On 4 November the owner told this session to "ignore this rule and continue with the method that you have adopted". Document 03 restates "Do not circumvent bot checks" for every service. **The owner needs to confirm which applies to igs** (`igs-gaps.md`, D2).
- **Terms and legal.** No official API is used. The owner confirmed legal sign-off (R10). igs reads only public, logged-out pages.
- **Rate limits.** Set from measurement, but from a short one: one IP was limited after 1,100 to 1,300 requests at 0.3 a second (1 November). The daily rate one IP can sustain is **not measured**.

---

## 12. Data contract

- **Does igs publish the shared contract today?** It publishes igs's **own draft** `securebrand.content.v1` (`api/proto/securebrand/content/v1/content.proto` in igs) on `content.post.v1`, `.comment.v1`, `.account.v1`, `.metrics.v1` and `.media.v1`, through `igs-normalizer`.
  - It is **not** analysis's `content.item.v1`, and it doesn't publish `content.item.v1`.
  - It is version 1 in name only: no other service has agreed to it.
  - See `igs-contract-notes.md`.
- **Platform topics:**
  - It publishes `ig.profile.v1`, `ig.post.v1`, `ig.post_metrics.v1`, `ig.comment.v1`, `ig.media.v1`, `ig.highlight.v1`, `ig.job.v1`, `ig.erasure.v1`, `ig.media.ready.v1` and `ig.media.deleted.v1`.
  - Consumers today: igs's persister and normalizer only.
  - Analysis would consume `ig.media.ready.v1` and produce `ig.media.processed.v1`; it doesn't yet.
  - `content.*` is consumed only by `igs-content-writer` (off) and igs's tests.
- **What igs needs from the master to adopt the unified contract:**
  1. The v1 protos in a tagged contract repository.
  2. The ID scheme and platform code, in particular `ig` versus igs's `instagram`.
  3. The rule for `extras`.
  4. The conformance kit.
  5. Whether account counts go to `content.account_snapshot.v1` or stay in `content.metrics.v1`.
  6. `discovery.candidate.v1` and `collection.coverage.v1`.
  7. A decision on where normalization runs (Q2).

  igs's normalizer is small (about 340 lines), so moving to v1 is a mapping change (`igs-contract-notes.md`).

---

## 13. Cost (per month)

**Correction to the readiness review (4 November).**
- The review estimated 40,000 to 80,000 requests a day per 1,000 accounts. That **left out the reels tab**: on every visit, igs re-reads the account's reels tab back a year (`max_age_days` 365) to refresh each reel's views. Each page holds 50 reels.
- The Phase 0 accounts averaged about 1,070 reels a year. That's a mean; the median isn't recorded here, and 25 accounts hit the 5,000-reel cap.
- So that can be about 20 extra requests per visit, about 160 a day on the 3-hour tier.
- Post re-reads also load the 118 KB post page each time, about 5 times per new post.

**Revised estimates per 1,000 accounts per day (3-hour tier):**

| Account type | Requests per account per day | Data per account per day | Per 1,000 accounts per day |
|---|---|---|---|
| Few reels (about 100 a year) | About 45 | About 2.0 MB | About 45,000 requests and 2 GB |
| Many reels (about 1,000 a year) | About 190 | About 4.4 MB | About 190,000 requests and 4.4 GB |

- Each account's day is 8 grid pages, 2 to 20 reels pages per visit, about 2 new post pages, about 10 post re-reads, comment pages, 1 profile read, and page loads.
- **IPs:** 5 to 15 busy at 1,000 accounts, if one IP sustains about 13,000 to 20,000 requests a day; that's unmeasured.
- A proposed fix (`igs-gaps.md`, Plans) would cut the reels re-read to a recent window, with a weekly full pass. That brings requests near 70,000 a day per 1,000 accounts and IPs to about 4 to 7.

**Monthly estimates.** These are not quotes: the proxy price is the owner's contract, and the review assumed static ISP IPs at about $1 to $5 each.

| Item | 1,000 accounts | 10,000 accounts | Basis |
|---|---|---|---|
| Proxies (ISP IPs) | 5 to 15 IPs (with the reels fix, 4 to 7) | 40 to 150 IPs (with the fix, 30 to 60) | Estimate from the requests above. If billed per GB (residential): 60 to 130 GB a month at 1,000 accounts |
| Compute (igs's share of the cluster) | About $50 to $100 | About $100 to $250 | Estimate; memory and CPU not measured |
| Postgres (igs's schema, managed) | About $30 to $60 | About $100 to $300 by the end of year one | Estimate (0.2 to 2 GB a day at 10,000 accounts) |
| Valkey | About $15 to $30 | About $15 to $30 | Estimate |
| S3: raw responses, kept forever | About $5 to $10 in month one, growing by about 60 to 130 GB a month | About $30, growing by about 0.6 to 1.3 TB a month | Estimate. **The review's "under $5" assumed raw responses weren't all kept;** they are now (`capture_raw: all`) |
| AI or API fees | None | None | - |
| Kafka, ClickHouse, cluster base | Shared, not included | Shared | - |

Hot accounts:
- one account visited every 10 minutes is 144 visits a day;
- with the reels re-read as it is today, that's about 300 to 3,000 requests a day per hot account;
- with the fix, about 300.

The pilot must measure all of this (D5).

---

## 14. Status and risks

1. **Where the build stands.**
   - Milestones M1 to M12i are done and merged; the details are in `docs/instagram/build-status.md`.
   - Proven against fakeig:
     - the whole pipeline end to end (profile, grid, reels, highlights, post pages, comments, persister, normalizer, ClickHouse writer, API, scheduler, discovery, media, erasure);
     - chaos and failover tests;
     - the backup drill.
   - Proven against Instagram:
     - one-off Phase 0 runs (31 October to 1 November): 348 accounts, 269,703 posts, 229,353 comments and 344,491 reels, with the per-request costs;
     - the tagged-tab test (5 November: needs a login).
   - **Not proven:**
     - the deployed system against Instagram;
     - memory and CPU;
     - a 24-hour soak;
     - highlight items on real data;
     - how many requests an IP sustains per day.
2. **Open issues and known bugs.** Each is in `igs-gaps.md` with a fix:
   - The linked Facebook page ID is dropped, and its name is put in `Link.url`.
   - Comment deletions are never published. *(Fixed: REQ-008 fix 14.)*
   - Walled posts (2.6%) publish nothing. *(Fixed: REQ-008 fix 15.)*
   - Photos and carousels have no metrics series.
   - Exits are named by address.
   - `schemas apply` would register `content.*` subjects.
   - Discovery has no daily cap by default.
   - The reels tab is re-read a year back on every visit.
   - The shared rate limiter fails open when Valkey is down.
   - The topics file has 915 partitions.
   - Raw lines carry no parser version.
   - There's no gitleaks or image scanning in CI.
3. **Top three risks in production:**
   - **Instagram's per-IP limits and walls.** 3 of 10 ISP IPs were flagged in Phase 0, and the daily rate per IP is unknown. Collection shrinks if the pool shrinks; alerts exist, but nobody has seen them fire on real traffic.
   - **The bot-check rule.** If it is applied strictly (identify honestly, no moving to another IP after a wall), logged-out collection may not work at all.
   - **Cost and load from the reels re-read and post-page refreshes** until the proposed fix is built. They could need two to three times the IPs first estimated.
4. **What igs needs from the owner:**
   - The bot-check decision for igs (D2).
   - Approval of the fixes and plans in `igs-gaps.md`, in the order given.
   - A pilot machine (not the shared pilot server), or approval of strict limits there.
   - Replacements for the 3 flagged ISP IPs.
   - The proxy price per IP or per GB.
   - A daily discovery cap.
   - Whether `content.*` from igs should keep flowing before v1. Proposed: yes on igs's own topics, but no Schema Registry registration.
   - What media analysis needs (Q8).

---

# Appendix: the production-readiness review (4 November 2026), unchanged

Unchanged except that the shared pilot server is named rather than given by its address.

> **Note (added 5 November 2026):** this is the production-readiness review as answered on 4 November 2026 at 12:19 Iraq time, unchanged. Since then the gaps it listed were built and merged (M12a to M12i: shared exit rests and kept sessions, bulk import, hot/warm/long-tail tiers, edit history and comment metrics, highlights as posts, the shared envelope and raw responses, `content.*` and igs-normalizer, the ClickHouse writer, backups and the restore drill, the tagged-tab test, the shared Iraq place list). `igs-gaps.md` gives the current state against the service standard.

I made no changes for this review: no code, config or servers. I only read the code and docs.

## Summary

1. **Overall readiness:** igs is fully built and tested against a fake Instagram, but it has never run as a whole system against real Instagram, and it isn't connected to the shared architecture yet.
2. **Top three blockers:**
   - **Your bot-check rule.** igs presents itself as a Chrome browser, and when one IP gets a login wall it moves to another IP. As written, the rule conflicts with both.
   - **No real-traffic run of the full system.** Only one-off test runs have touched real Instagram. This review also found a gap: when one crawler pod pauses a blocked IP, the other pods don't know and keep using it.
   - **Not on the shared design yet.** There is no `content.*` normalizer or ClickHouse path, and no hot tier (the shortest revisit interval is 1 hour).
3. **Main risk:** Instagram's per-IP limits. One IP was limited after about 1,100 to 1,300 requests at roughly one every 3 seconds, and 3 of the 10 ISP IPs were flagged. If the bot-check rule requires igs to identify itself honestly, logged-out collection may not work at all.
4. **Time to production:** about 2 to 3 weeks to a standalone production canary (your decisions, about 1 week of fixes, a 48-hour pilot and its report). Joining `content.*` and ClickHouse needs the shared phase 1 infrastructure first, then about 1 to 2 more weeks of igs work.
5. **First thing I'd do:** get your decision on the bot-check rule, because it decides whether the current approach stands.

---

## Part 1: Is everything there?

**None of these components runs anywhere today** (see Part 2).

| # | Component | Exists | Where in the code | How it's tested |
|---|---|---|---|---|
| 1 | Account list | **Partly** | `internal/ig/api/profiles.go`. API to add, list, change and remove accounts (`/v1/profiles`). You can pause an account or change how often it's visited. Each account is marked active, private, unavailable or suppressed when it's read. **There's no bulk import of accounts to monitor** (one per call), and no CLI for it | End-to-end tests |
| 2 | Discovery | **Yes** | `internal/ig/discovery`. Finds accounts in the posts we read (tags, co-authors, mentions, commenters), checks each one, scores how Iraqi it is, and adds it if public. Accounts from elsewhere come in through the intake. No keyword or hashtag search | Unit and end-to-end tests |
| 3 | Scheduler | **Partly** | `internal/ig/scheduler`. Every account every 3 hours (per account between 1 hour and 30 days), the profile itself once a day, and comment refreshes for posts up to 7 days old. On-demand jobs, scheduled visits and the first backfill each have their own queue. **There are no automatic hot, warm and long-tail tiers** (the database columns exist but nothing uses them), and nothing can run more often than hourly | End-to-end schedule tests |
| 4 | Collector | **Yes** | `internal/ig/stage`, `internal/ig/client`. **Logged out; no Instagram accounts are used anywhere.** It calls Instagram's own data API with a lightweight HTTP client. It fetches the profile, every post (50 per page), each reel's views, likes and comments from the reels tab, post pages (details plus the comments embedded in them), top-level comments, and highlights. No replies, no stories, no tagged tab. A browser fallback exists but is off | Unit tests, 15 fixtures from real responses (personal data removed), end-to-end tests against the fake Instagram, and the real Phase 0 runs |
| 5 | Sessions and proxies | **Yes, with a gap** | `internal/scrape/session`, `internal/scrape/proxy`. Each IP sends at most one request every 4 seconds across all pods (tracked in Valkey). A rate limit pauses the IP for 10 minutes; a checkpoint or login page pauses it for 30. Afterwards one session tests the IP, and a failed test doubles the pause, up to a cap. A blocked task moves to another IP. **Sessions are not kept across restarts.** **The pause is known only to the pod that hit the limit.** ISP proxies only; Phase 0 used 10 IPs, 3 of them flagged | Unit tests, and end-to-end failover tests with per-IP limits in the fake Instagram |
| 6 | Parsers and change detection | **Yes** | `internal/ig/parse`, `internal/ig/registrysync`. When Instagram changes its query IDs, a sync job finds the new ones on known test profiles. Unexpected response shapes raise schema-drift metrics and alerts | Fixtures, and the fake Instagram changing its query IDs (fixed itself in one run). **Never tested against a real Instagram change** |
| 7 | Storage | **Yes** | Postgres, schema `ig`. **Posts and comments are in separate tables.** Post counts are recorded whenever they change, profile counts daily. Edits are flagged and counted but the text is overwritten. Deletions are marked, not removed. Kept forever | Integration tests |
| 8 | Media | **Built, off** | `internal/ig/media`. Downloads to S3, hands off to the analysis service, and deletes once the analysis confirms it's done, or after a time limit. Off until the analysis service exists | End-to-end media tests |
| 9 | Event output | **Yes, in igs's own format** | Kafka `ig.*.v1` topics in Protobuf: profile, post, post metrics, comment, media, highlight, job. No mapping to `content.*` (see Part 3) | Contract and schema-registry tests |
| 10 | Jobs | **Yes** | Jobs in Postgres plus counters in Valkey. Retries wait 30 seconds, then 5 minutes. Tasks that keep failing go to a dead-letter queue and can be replayed. A cleanup marks a job partial after 30 minutes without progress | Chaos test (crawler crashes, rate limits, login walls) |
| 11 | API | **Yes, for operations** | `igs-api`: REST with keys, scopes and rate limits, and an OpenAPI document. It's a data and operations API, not the client app's API, which the architecture puts over ClickHouse | End-to-end API tests |
| 12 | Monitoring | **Yes** | Prometheus metrics, health and readiness endpoints, 27 alerts each linked to a runbook section, and a Grafana dashboard. Proxy credentials are removed from logs | A consistency test (each metric an alert or dashboard uses exists, and every alert links to a runbook section). **Never seen real traffic** |
| 13 | Deployment | **Yes** | Docker images, a Helm chart with **CPU and memory requests and memory limits for every component**, KEDA autoscaling, disruption limits, a network policy and Kubernetes secrets. A release workflow publishes to GHCR | Chart validated in CI and started in Docker against the fake Instagram. **Never deployed to a real cluster** |
| 14 | Tests | **Yes** | Unit, integration (Postgres, Kafka, Valkey), 18 end-to-end tests against the fake Instagram with chaos tests, a load test, and a soak test. **The soak defaults to 20 minutes; nothing has run for 48 hours** | — |
| 15 | Docs and runbooks | **Yes** | Go-live (sizing, install, canary, ramp, rollback), operations (a section per alert), query-ID sync, Phase 0 | — |
| 16 | Security and privacy | **Yes** | Credentials removed from logs. Test fixtures are refused if any personal value survives. Commenters can be stored in full or as keyed hashes (`pii_mode`). Reading commenter identities needs a separate API permission. Erasure and suppression exist | Unit and end-to-end erasure tests |
| 17 | Backups and restore | **Missing** | Nothing in igs. It would rely on managed Postgres backups. No restore runbook or drill | — |

### igs against the six fbs pilot incidents

| fbs incident | What igs does | Verdict |
|---|---|---|
| 1. A retry loop filled the disk | **Retries:** 30 seconds, then 5 minutes, then the dead-letter queue after about 15 minutes without progress. **When every IP is paused**, tasks wait 5 minutes at a time, at most 50 times (about 4 hours). **Kafka:** retry topics keep messages for 48 hours | **Limited for each task.** In a long outage of every IP at 10,000 accounts, the number of waiting messages still grows. Test this in the pilot |
| 2. A browser froze the shared server | The browser is off by default. When on: at most 2 per pod, at most 2% of session starts, a 45-second timeout, and a 2 GB pod memory limit, so a runaway browser kills its pod, not the machine | OK |
| 3. Sessions lost at every restart | **Sessions are lost on restart too.** But every page load waits for its IP's shared pace, so a restart can't make every IP load pages at once | Mostly OK |
| 4. The session pool ran dry | Short pauses (10 or 30 minutes, capped doubling), and alerts for most IPs paused, parked tasks and sessions retiring fast. **Gap:** a pause isn't shared between pods, so each pod hits a limited IP once more before pausing it. Phase 0 found 3 of 10 IPs flagged | **Needs work** |
| 5. Stuck jobs | The cleanup ends a job after 30 minutes without progress. The chaos test found and fixed two ways a task could hang | OK |
| 6. Disk grew about 1 GB a day | Kafka and Valkey are capped by retention. Postgres grows forever, which is your retention choice (my estimate is in Part 3). **Not measured yet** | Measure in the pilot |

---

## Part 2: Is it running now?

### 1. Where igs runs

**Nowhere.** No igs service is deployed or running. Only one-off command-line runs have touched real Instagram: the `igs-spike` probe and the Phase 0 runs, which you ran on the Phase 0 droplet from 31 October to 1 November. The last one was the retry run on 1 November, 23:06 to 23:44 Iraq time.

The repository doesn't record that droplet's address, so I can't tell whether it is the shared pilot server. If it is:
- igs never left anything running there;
- the probe's output folder `phase0/out-ig/` holds real data (CSV files and profile pictures). Keep it: the owner keeps all collected data (6 November 2026).

CI runs igs only against the fake Instagram, on GitHub's machines.

### 2. Commands for you

**On the pilot server now**, to confirm nothing of igs is there. Each command prints only a count or folder sizes.

1. Count igs processes. Good: `0`.

```bash
pgrep -fc 'igs-'
```

2. Count igs containers. Good: `0`.

```bash
docker ps --format '{{.Names}}' | grep -c igs
```

3. Size of the probe output kept on disk (it is real data: keep it). Good: a size you expect.

```bash
find / -maxdepth 5 -type d -name out-ig 2>/dev/null -exec du -sh {} \;
```

**After igs is deployed** to its own namespace:

4. Pod names and states. Good: every pod `Running`, `READY` full, few restarts.

```bash
kubectl -n igs get pods
```

5. Memory and CPU per pod. Good: crawlers well under 2 GiB, the persister under 1 GiB.

```bash
kubectl -n igs top pods
```

6. Autoscalers. Good: every one `READY True`.

```bash
kubectl -n igs get scaledobjects
```

7. Number of failed jobs (counts up to 100). Good: `0` or close to it. Set `KEY` in your shell first, so it never appears on screen.

```bash
curl -s -H "Authorization: Bearer $KEY" "https://<igs-host>/v1/jobs?status=failed&limit=100" | jq '.jobs | length'
```

### 3. Latest numbers

These come from Phase 0 only: real Instagram and ISP proxies, but run with the test kit, not the production system.

| Measure | Value |
|---|---|
| Accounts | 348, read once (not monitored over time) |
| Collected | 253,973 posts and 222,421 comments in 1 hour 42 minutes. That's backfill speed, not a steady rate |
| Blocked requests | 0.62%; page loads 100% successful; slowest 5% of requests 4.4 seconds or more |
| Cost per account | 37.6 requests and 1.16 MB for a year back |
| Memory and CPU | **Not measured** on real traffic |
| Proxy data per day | **Not measured** in steady state |
| Storage growth per day | **Not measured** |

---

## Part 3: How ready is it?

### 1. Scores

| Area | Score | Why | Blocks production? |
|---|---|---|---|
| Account list | Needs work | No bulk import of accounts | No |
| Discovery | Ready | Network discovery and intake are tested | No |
| Scheduler | Needs work | No tiers; nothing faster than hourly | **Yes, for hot accounts.** A long-tail-only start can go ahead |
| Collector | Ready | Matched Phase 0 on real Instagram | Depends on the bot-check decision |
| Sessions and proxies | Needs work | Pauses aren't shared between pods; 3 of 10 IPs flagged | **Yes** |
| Parsers and change detection | Ready | Never tested against a real change | No |
| Storage | Ready (for igs) | Text history is lost on edits | No |
| Media | Ready, off | Waits for the analysis service | No |
| Event output | Needs work | No `content.*` mapping | Yes, for the shared design; no, for standalone |
| Jobs | Ready | — | No |
| API | Ready | — | No |
| Monitoring | Ready | Unproven on real traffic | No |
| Deployment | Ready | Never deployed to a real cluster | No |
| Tests | Needs work | No 48-hour run | **Yes (the pilot)** |
| Docs | Ready | — | No |
| Security and privacy | Ready | — | No |
| Backups | Missing | No restore runbook or drill | **Yes** (small job) |

### 2. Estimates per 1,000 accounts

These are on the 3-hour schedule, worked out from Phase 0 and the design, **not measured**.

| Measure | Estimate |
|---|---|
| Requests per day | 40,000 to 80,000 (8 visits a day at 5 to 10 requests each). The first backfill adds about 37,600 once |
| Proxy data per day | 0.6 to 2.4 GB (backfill once: about 1.2 GB). For comparison, fbs uses about 59 GB a day per 1,000 pages |
| IPs needed | 2 to 4 busy. **Buy 5 to 6** to cover flagged IPs and the backfill |
| Memory and CPU | Not measured. The chart's minimum replicas request about 5.5 vCPU and 5.75 GiB, which is generous for 1,000 accounts. A pilot needs about 2 vCPU and 3 GB. igs uses the same HTTP client as fbs, so a crawler should be near fbs's 80 MB, but this must be measured |
| Storage growth per day | About 20 to 200 MB in Postgres (about 2,000 posts and 10,000 to 200,000 comments a day; comment counts vary widely). The first year's backfill adds about 1.5 to 3 GB once |
| Hot tier | One account visited every 5 minutes needs 1,400 to 2,900 requests a day, so one IP carries only 7 to 15 hot accounts |

### 3. Risks

**Blocks:**
- Instagram limits each IP. Phase 0 hit the limit after about 1,100 to 1,300 requests at roughly one every 3 seconds; igs now paces each IP at one every 4 seconds.
- 3 of 10 ISP IPs sent every page load to the login page.
- Residential proxies failed every page load in the last run.

**Data Instagram hides from logged-out visitors:**
- comment replies (top-level comments only);
- stories (highlights are available);
- views on photos and carousels, and shares (reels do have views);
- follower lists;
- the tagged tab (untested);
- private accounts beyond their profile.

Also:
- 2.6% of post pages were walled when logged out.
- 7 of 348 accounts were refused (restricted).
- Where an account hides its likes, the like count is meaningless (about 3% of posts).

**Reliability:**
- the full system has never run on real traffic;
- query-ID healing has never been seen against a real Instagram change.

**Legal and terms of service** (I'm not a lawyer):
- Meta's terms forbid automated collection without permission.
- As I understand it, a 2024 US court ruling (Meta v. Bright Data) went against Meta for logged-out scraping of public data, but that's one US case.
- Storing commenters' usernames is personal data. The architecture's "no deletion" rule may clash with any erasure rights that apply to you.
- You signed off legal for the current design. Re-check it against the architecture's retention rules.

### 4. The bot-check rule

Plainly: **igs as built does several things that conflict with "no hiding automation"**. igs copied this approach from fbs, so fbs very likely does the same; it's worth checking how fbs squares this with the rule.

| What igs does | Conflicts? | Cost of stopping |
|---|---|---|
| Presents a Chrome on Windows: Chrome's network fingerprint, user agent and browser hints | **Yes** | Unknown and possibly fatal: Instagram may wall an honest client. Needs a test |
| Adds random timing variation of up to ±40% per request | Mildly | Nothing |
| The browser fallback (off) presents a normal Chrome instead of a headless one | Yes, if switched on | Nothing while it stays off |
| After a login wall or checkpoint, pauses that IP and moves the task to another IP; retries a walled post page on two fresh sessions | **Arguably yes** ("getting around a login wall") | About 2.6% of post pages lost. Replace flagged IPs instead of routing around them |
| Spreads requests over many IPs and replaces each session after 500 to 750 requests | Grey area. Each IP stays under Instagram's limit, but rotation is how capacity grows | If forbidden: one IP carries only 270 to 540 accounts in total |

igs never logs in, uses no Instagram accounts, solves no CAPTCHAs and answers no challenges.

**My recommendation:**
- Drop the move-to-another-IP after login walls and the post-page retries now. Both are cheap to remove.
- In the pilot, run one or two IPs with honest identification next to the current setup, and compare walls and blocks.
- If honest identification works, switch everything to it. If it doesn't, the choice is yours: accept the current approach, or go official. Business Discovery covers business and creator accounts only, without other people's comment text.

### 5. Fit with the shared architecture

| Topic | igs today | What needs to change |
|---|---|---|
| Event contract | Platform topics `ig.*.v1` match the architecture. The envelope has event ID, `observed_at` and publish time, but **no `version`, `collector`, `collector_version` or `raw_ref`**. IDs are numbers, not text | Add the missing fields (additive only). Build the igs normalizer to `content.*` once the shared schema repository exists. This also gives the `content.account` events for discovered accounts |
| ClickHouse | igs stores content in its own Postgres (the persister reads from Kafka, so it does publish once) | Use the shared ClickHouse writer. igs's Postgres keeps what the crawler needs (newest stored post, comment counts). Slimming it is a later decision |
| Posts and comments in separate tables | Yes. Reels are posts; highlights are separate | The normalizer maps highlight items to posts with `kind = story` |
| Metrics and snapshots | Post counts on change; profile counts daily | Add comment metrics (comment like counts are overwritten today) |
| Append, don't overwrite | Captions and comment text are overwritten in Postgres. Old versions survive only in Kafka for 7 days | Archive events, or keep raw responses, so edits aren't lost |
| Keep raw responses | Only failed responses, for 7 days | Switch to keeping everything (a setting already exists) with no expiry, plus `raw_ref` in events. Adds 0.6 to 2.4 GB a day per 1,000 accounts before compression |
| Entity layer | Bio, bio links, linked Facebook page, category, business flag and profile picture URL are captured from day one | Image hashing later |
| Retention | Kept forever. Media is deleted after analysis, **or after a time limit**. Erasure exists | Turn the media time limit off. Keeping or removing erasure is your decision with legal. I'd keep it |
| Tiers | Fixed 3 hours, with manual per-account changes no faster than hourly | Automatic tiers by posting rate, sub-hour hot visits, and a hook for client watchlists |

---

## Part 4: My plan

### 1. Steps to production

| # | Step | Why | Effort | You need to decide or provide |
|---|---|---|---|---|
| 1 | Decisions | They shape everything after | Your time | The bot-check reading, tier mix and budget, whether to keep erasure, hosting region |
| 2 | Share IP pauses across pods (stored in Valkey) | Blocker found in this review | 1 day | — |
| 3 | Bot-check changes | Follows your decision | 0.5 to 2 days | — |
| 4 | Pilot report command (counts only) | fbs has one; igs doesn't | 1 day | — |
| 5 | Backup and restore runbook, plus a restore drill | Missing | 0.5 day | Managed Postgres |
| 6 | Event fields and raw responses kept | Can't be recovered later | 1 to 2 days | S3 bucket |
| 7 | **48-hour pilot** and report | First real run of the full system | About 4 days in total | Droplet, IPs, account list (below) |
| 8 | Fix pilot findings | — | 1 to 3 days | — |
| 9 | Production canary, then the ramp, following the go-live runbook | — | 1 to 2 weeks of ramp | Kubernetes cluster (shared phase 1) and IPs |
| 10 | Tiers and the hot tier | Near-real-time alerts | 2 to 4 days | Watchlists (phase 2) |
| 11 | `content.*` normalizer and ClickHouse | The shared design | 3 to 5 days | Shared schema repository (phase 1) |

Steps 2 to 8 don't need the shared infrastructure, so they can start now.

### 2. The pilot

- **Machine:** its own droplet, not the shared pilot server. 4 vCPU and 8 GB, with k3s and the Helm chart, so every component has its memory limit. Browser off.
- **Accounts:** 100 to 200 from your Phase 0 list, with discovery on and the daily cap at 50.
- **IPs:** 10 ISP IPs, with the 3 flagged ones replaced.
- **Tests during the run:** a pod restart, and every IP paused for 1 hour.
- **Report (counts only):**
  - posts and comments per hour;
  - blocked requests and paused IPs over time;
  - proxy data per account per day;
  - peak memory and CPU per pod;
  - Postgres growth per day;
  - dead-letter and stuck jobs;
  - completeness against 10 accounts checked by hand in a logged-out browser;
  - cost per 1,000 accounts.
- **Pass criteria:**
  - fewer than 5% of requests blocked;
  - no out-of-memory kills;
  - no account waiting more than 3 hours past its due time;
  - the dead-letter queue near 0;
  - the 10 manual checks matching.

### 3. Monthly costs (rough)

These are my rough estimates, not quotes. **I don't know your proxy price**: static ISP IPs commonly cost about $1 to $5 each a month, so check your contract.

| Item | 1,000 accounts | 10,000 accounts |
|---|---|---|
| ISP IPs | 5 to 6 (about $5 to $30) | 30 to 50 (about $30 to $250) |
| igs's share of cluster compute | About $50 to $100 | About $100 to $250 |
| igs's own Postgres (managed) | About $30 to $60 | About $100 to $300 by the end of year one (grows 0.2 to 2 GB a day) |
| Valkey | About $15 to $30 | About $15 to $30 |
| S3 (raw responses) | Under $5 | About $10 to $30 |
| **Total for igs** | **About $100 to $250** | **About $300 to $850** |

- Kafka, ClickHouse and the cluster's base cost are shared with the other collectors and not included.
- Each 100 hot accounts visited every 5 minutes add about 7 to 14 IPs.

I can publish this review as a shareable page if you want to pass it on.
