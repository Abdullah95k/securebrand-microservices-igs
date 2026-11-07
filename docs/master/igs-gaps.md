# igs against the service standard: gaps and plans

By the igs session, 5 November 2026 (Iraq time), from `Abdullah95k/securebrand-microservices-igs` `main` (`d50f3ee`). Every item of `04-service-standard.md` is marked **Met**, **Partly**, **Missing** or **N/A**, with evidence and, for each gap, a fix and its effort.

**Nothing here has been built.** Every fix is a proposal for the owner to approve. Efforts are igs-session time, tests included. All fixes are additions: none renames or removes a topic, field, schema or endpoint. Where one changes a value or a key, it says so.

## Summary

| Area | Met | Partly | Missing | N/A |
|---|---|---|---|---|
| A. Packaging and deployment | 4 | 2 | 0 | 0 |
| B. Runtime behaviour | 2 | 5 | 0 | 0 |
| C. Reliability | 6 | 1 | 0 | 0 |
| D. Platform access | 1 | 4 | 0 | 0 |
| E. Data output and contract | 3 | 7 | 0 | 0 |
| F. Observability | 1 | 4 | 0 | 0 |
| G. Testing and CI | 2 | 2 | 1 | 0 |
| H. Operations | 2 | 2 | 0 | 0 |
| I. Security | 1 | 1 | 2 | 0 |

**The fixes to do before a pilot** (about 7 days in all) are listed at the end, in "Fix order".

---

## A. Packaging and deployment

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| A1 | **Partly** | `.github/workflows/release.yml` builds `ghcr.io/abdullah95k/igs:<version>` and `<version>-browser` (and the chart) on a `v*.*.*` tag; CI builds both images on every change (`ci.yml`, job deploy) | One image holds every binary, not one image per binary. **No release has been tagged, so nothing is published.** Fix: tag the first release when the owner approves. If the master wants an image per binary, add a build matrix | Tag: minutes. Per-binary images: 0.5 day |
| A2 | **Met** | Distroless `nonroot` image. The chart runs as user 65532, with `readOnlyRootFilesystem: true` and every capability dropped (`deploy/helm/igs/templates/deployments.yaml`) | - | - |
| A3 | **Met** | `deploy/helm/igs`: every setting under `config:` in values, rendered to the config file. Hosts, ports and credentials come from values and secrets. `make chart-check` (kubeconform) and `make igs-chart-smoke` in CI | (A sizing note, not an A3 gap: `configs/ig/topics.yaml` makes 915 partitions with replication 3. Lower it for the pilot; see C2) | - |
| A4 | **Met** | `internal/config`: the embedded defaults, then files, then `IGS_*` variables. `Config.Validate` refuses bad values and names them | - | - |
| A5 | **Met** | Secrets come from Kubernetes secrets as environment variables, and `proxies.yaml` as a mounted file (`docs/runbooks/igs-go-live.md` §4). Nothing is baked in; fixtures are scrubbed | - | - |
| A6 | **Partly** | goose migrations `migrations/ig/00001` to `00008`, run by the `igsctl migrate up` hook. All are additive so far | Nothing **checks** that the previous release still runs on the new schema. Fix: a CI job that runs the last release's e2e smoke against the new migrations, and a rule in the PR template | 1 day |

## B. Runtime behaviour

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| B1 | **Met** | `:9090` `/healthz`, `/readyz` (per-binary dependency checks) and `/metrics` on every binary | - | - |
| B2 | **Partly** | Requests and memory limits on every component (chart values; see `igs-operations.md` §8), so a runaway pod is killed alone | **Not measured** under load or soak. Fix: measure (Plans: memory and soak), then set each limit at about 2× the measured peak | 0.5 day, plus a 24 h run |
| B3 | **Partly** | No browser in normal operation (`ig.browser.enabled: false`). When on: at most 2 per process, at most 2% of page loads, images, video and fonts blocked (`BlockMedia`), and a fresh Chromium per render (`internal/scrape/browser/render.go`) | When on, Chromium runs **inside the crawler's container** (sharing its 2 GiB limit), not in a container of its own. Fix: a separate `crawler-browser` component with its own memory limit, used only for page loads | 0.5 day |
| B4 | **Met** | On SIGTERM a crawler stops taking work, checkpoints and parks its identities within 75 s (grace 90 s). Restart chaos tests are in `internal/ig/e2e` | - | - |
| B5 | **Partly** | Checkpoints under leases (`internal/crawl/checkpoint`), so another pod resumes a task. Exit rests are shared in Valkey. Idle identities are parked on shutdown and reused by the next pod (M12a) | Identities are parked **only on a graceful stop** (`internal/scrape/session/park.go`, called from `internal/ig/app/crawler.go`). After a crash or an out-of-memory kill they're lost, and every exit loads a page again. Fix: park idle identities every few minutes as well as at shutdown | 0.5 day |
| B6 | **Partly** | Scales out (KEDA on lag). Fleet-wide limits are in Valkey: per-exit rate `exit_rps` 0.25, AIMD global rate, exit rests, the daily budget | **The shared limiter fails open** when Valkey is unavailable (`internal/scrape/ratelimit/ratelimit.go`, "redis unavailable, failing open"). Several pods could then drive one exit faster than `exit_rps`. Fix: fail closed (wait and retry), or fall back to a local per-exit limit of `exit_rps` divided by the crawler pod count | 0.5 day |
| B7 | **Partly** | Internally everything is UTC; runbooks state Iraq time | Owner-facing output from `igs-spike` and `igsctl` prints UTC (RFC 3339) times and doesn't say Iraq time. Fix: print Iraq time, labelled, in owner-facing summaries | 0.5 day |

## C. Reliability

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| C1 | **Met** | `crawler.max_attempts` 5 and `max_total_attempts` 50; retry topics at 30 s and 5 min; `igs.task.dlq` and `igs.persist.dlq`; `igsctl dlq replay`; parks bounded (`TestHighlightsAfterParks`, `TestHighlightsOutlastRefusedRuns`) | - | - |
| C2 | **Met** | Every topic has a retention (`configs/ig/topics.yaml`): tasks 72 h, retries 48 h, DLQ 14 days, `ig.*` and `content.*` 7 days, erasure 30 days | Optional: tasks to 24 h like fbs (jobs are reaped after 30 min without progress). Also lower the partition counts for the first months: 915 partitions × 3 replicas is sized for 300,000 accounts | Config only: 0.25 day |
| C3 | **Met** | The reaper ends jobs with no progress for 30 min and cancels their tasks (`scheduler.reaper_after`; runbook "Stuck jobs") | There's no absolute maximum job age, so a job that keeps making slow progress isn't cut off. Optional: `scheduler.max_job_age` (for example 24 h) | 0.5 day (optional) |
| C4 | **Met** | Breakers per proxy bucket (`tier\|host:port`, or the /24 for static IPs) and per GraphQL query; cooldowns double up to 30 min | (Bucket names contain addresses: see D4) | - |
| C5 | **Partly** | Alerts `IgsIgExitsMostlyResting`, `IgsIgIdentitiesRetiringFast`, `IgsIgProxyBucketOpen` and `IgsIgTasksParked`. A page load only once per identity, and identities live for 500 to 750 requests | **No gauge of live identities, and no alert on it.** Page loads per IP per day are neither capped nor reported. The tolerated page-load rate per IP is unmeasured (3 of 10 ISP IPs were flagged in Phase 0). Fix: an `igs_identities{state}` gauge and an `IgsIgIdentityPoolLow` alert; a per-exit daily page-load counter and cap; measure in the pilot | 1 day |
| C6 | **Met** | Pull-based consumers; KEDA on lag; lane caps (p2 at most 40%); persister retries, then quarantines | - | - |
| C7 | **Met** | `ig.budget`: daily requests and/or bytes, fleet-wide. It pauses backfill first, then stretches intervals. Alert `IgsIgBudgetHoldingBackfill` | Enabled, but with no limit set (0). The owner sets a figure from the pilot | - |

## D. Platform access (collectors)

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| D1 | **Met** | Logged out everywhere; no Instagram accounts exist in igs (ADR 0001) | - | - |
| D2 | **Partly (owner decision)** | igs solves no CAPTCHA or challenge, and computes no anti-bot token: the browser's client-state fields `__dyn` and `__csr` aren't computed. A challenge or wall pauses the exit and is counted | **Against a strict reading of the rule, igs:**<br>• presents as Chrome (TLS fingerprint, user agent, headers; client hints in the browser fallback);<br>• computes `jazoest` (a checksum of a page token, as the page's script does);<br>• **moves work to another IP after a wall or rate limit** (`block_hops` 2; page loads tried on up to 3 identities; a walled post page tried on 3 fresh identities);<br>• jitters its timing.<br>On 4 Nov the owner told this session to "ignore this rule and continue with the method that you have adopted"; `03` restates the rule for every service. **The owner must say which applies to igs.**<br>Options: (a) confirm the 4 Nov exception for igs in `03` and `04`; (b) a strict-mode switch: no IP hop after a wall (`block_hops` 0), one identity per page load, a walled post skipped at once. Whether a non-Chrome client works at all is unknown (likely not); it can be tested in the pilot with the owner's approval | (a) none; (b) 1 day, and a test |
| D3 | **Partly** | `exit_rps` 0.25 and `identity_rps` 0.33 come from the 1 Nov measurement (one IP limited after 1,100 to 1,300 requests at 0.3 a second); identity life is 500 to 750 requests | How long one IP sustains a given rate over days is unmeasured. Per-account limits: N/A (no accounts). Fix: measure in the pilot (requests and walls per exit per hour) and set the limits from it | In the pilot |
| D4 | **Partly** | Proxy credentials are redacted everywhere (logs, errors, metrics) | **Exits are named by address**, `tier\|host:port`, in logs ("session: exit rests", "proxy: exit", "proxy: breaker"), in the metric label `breaker_state{name="proxy:..."}` and in Valkey keys (`identities:<exit>`). Fix: name each exit `<tier>#<n>` (its place in `proxies.yaml`) in every log, label and key, with the address kept only in memory. **This changes a label value and a key:** the identities parked under old keys are lost once, which is harmless | 1 day |
| D5 | **Partly** | Phase 0 (31 Oct to 1 Nov): per-profile requests and MB for a year's backfill (37.6 requests and 1.16 MB per profile), block rate 0.62%, p95 4.4 s; 348 accounts; per-request sizes | No **deployed** pilot with per-account-per-day steady-state figures and projections. The readiness review's projection missed the reels re-read (`igs-operations.md` §13). Fix: the pilot (Plans) | See Plans |

## E. Data output and contract

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| E1 | **Met** | Crawlers publish each observation once to `ig.*`. The persister (Postgres), normalizer (`content.*`) and content-writer (ClickHouse) consume from Kafka. Raw responses go to S3 as the raw store, not as a second copy of the events | - | - |
| E2 | **Partly** | `ig.*` platform topics, and `content.post/comment/account/metrics/media.v1` from `igs-normalizer` | `content.*` is **igs's own draft**, not an agreed mapping (`igs-contract-notes.md`). Fix: adopt the master's v1 when published | 7 to 8 days (contract notes §3) |
| E3 | **Partly** | `igsctl schemas apply` registers `<topic>-value` subjects as `BACKWARD_TRANSITIVE`; `buf breaking` in CI; fields only added so far | It **also registers the five `content.*` subjects**, and `examples/production.yaml` sets `hooks.schemas: true`. That would register `securebrand.content.*` against the standing rule, and clash with analysis's package of the same name. Nothing is registered today. Fix: leave `content.*` out unless a flag asks; set `hooks.schemas: false` in the production example until v1 | 0.5 day |
| E4 | **Partly** | `ig.*` events carry `Observation`: event ID (a hash of entity and content), platform, native ID, observed-at, version (observation µs), collector and version, `raw_ref`, parser version, query and doc ID, job IDs, content hash. Published-at is in each entity (`taken_at`, `created_at`) | The `content.*` envelope lacks `kind`, `id`, `parser_version`, `job_ids` and `content_hash`. A few derived events have no `raw_ref` (for example a post marked unavailable). Fix: envelope additions, part of E2 | 0.5 day |
| E5 | **Met** | A comment is keyed by its post on `ig.comment.v1` and `content.comment.v1`; others by their own ID | - | - |
| E6 | **Met** | Counts are optional in protos and nullable in tables. Hidden likes are stored as NULL with `counts_hidden` | (`is_business` is a plain bool, so "unknown" reads as `false`; an added optional field is in the field inventory's "Planned") | 0.25 day |
| E7 | **Partly** | Caption and comment edits: the old text goes to `post_edits` and `comment_edits`, and a new version is published. Post deletion: `unavailable` when a re-read (within 7 days) finds it gone | **Comment deletions are never detected or published** (the persister supports `deleted`, but no stage emits it). Posts older than 7 days are never re-read, so their deletion is never seen. Bio and link history is only in the event stream (Postgres keeps the latest). Fix: after a **complete** comment re-read (every page; collected equals reported), publish the comments not returned as deleted; a re-read at 30 days for deletion, like fbs; optionally a `profile_edits` table. **Comment deletions and the 30-day re-read: done (REQ-008 fix 14).** | 2 days |
| E8 | **Partly** | Every raw response kept forever (`capture_raw: all`, no bucket lifecycle) in `igs-raw` (`raw/`, `raw-failures/`), with `raw_ref` on each event | The raw line has **no parser version** (it's on the event). There's no re-parse tool. Fix: Plans, raw responses | 0.25 day for the version; 2 days for re-parse |
| E9 | **Partly** | Bio, every bio link (URL and title) and the website (first link) are captured from the first read | The **linked Facebook page's ID is dropped**, and its name is put in `content.account.v1 Link.url`. Fix: keep the ID (added ig field and column); publish it as a link with platform and ID | 0.5 day |
| E10 | **Partly** | Per job: counters, completeness (posts complete or truncated, median and minimum ratio, comments reported or collected) and errors on `ig.job.v1`; truncation reasons in `crawl_cursors` | Per item: `posts.comments_state` and `comments_collected` are never written. **Walled posts (2.6%) publish nothing.** Nothing goes to a shared coverage topic. Fix: write per-post comment coverage; publish walled posts from grid fields with reason `gated`; `collection.coverage.v1` with E2 | 1.5 days |

## F. Observability

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| F1 | **Partly** | Prefix `igs_`, 58 metric variables: requests by outcome and query, blocks and rests, items persisted per table, proxy and response bytes, consumer lag, job outcomes, drift, discovery, budget | **No session-pool size gauge** (see C5) | 0.25 day (with C5) |
| F2 | **Partly** | `slog` JSON. No content, comment text, credentials or cookies | Lines carry exit addresses (D4) and profile references (`u:<username>`) of monitored accounts. Fix: log the user pk and exit names only | 0.5 day |
| F3 | **Partly** | 29 PrometheusRule alerts, each linked to a runbook anchor (checked by the monitoring consistency test) | Missing from the required list:<br>• "no data for N minutes" (no alert on posts or comments published per interval);<br>• "session pool low" (C5);<br>• "memory near limit" (container memory against its limit).<br>Fix: three alerts | 0.5 day |
| F4 | **Met** | Dashboard "igs / Instagram overview" (`igs-ig-overview.json`), off by default in values | - | - |
| F5 | **Partly** | `docs/runbooks/igs-operations.md`, "Key signals": each signal with what good looks like | Not a list of read-only **commands** like fbs's `pilot.md` step 4. Fix: `igsctl health`, which prints counts (jobs, lag, blocks, exits resting, identities, last post seen), plus a runbook step with one command per block | 1 day |

## G. Testing and CI

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| G1 | **Met** | `ci.yml`: build, vet, buf lint, generate and breaking, `go test -race`, integration with Postgres 18, Valkey, ClickHouse 25.8 and Redpanda, the Phase 0 dry run, chart check and chart smoke | - | - |
| G2 | **Met** | `internal/fakeig`: profiles, grid, reels, highlights, post pages, comments, walls, rate limits, hidden counts, mutations and ground truth; e2e tests compare the store to it | - | - |
| G3 | **Partly** | `make igs-loadtest` (2 Nov, 4 CPUs: 150 profiles in 21 s; persister 15,200 rows a second); `make igs-soak` (4 minutes locally); the `soak` workflow (90 min default) | **No 24-hour soak**, and the workflow **has never been run**; GitHub-hosted runners stop at 6 hours. Memory and disk weren't recorded. Fix: Plans, memory and soak | 0.5 day, plus 24 h |
| G4 | **Partly** | The fixture scrubber and its tests; a CI grep for CDN signatures in fixtures | **No gitleaks** or other secret scanner. Fix: a gitleaks job on every change | 0.25 day |
| G5 | **Missing** | igs's own content tests (`TestContentEventsMatchStore`, `TestContentStoreMatchesPostgres`) | There's no shared conformance kit yet. Fix: run the master's kit in CI once it exists | 0.5 day after the kit |

## H. Operations

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| H1 | **Met** | `docs/runbooks/igs-operations.md`: every alert, block storm, registry stale (a platform change), schema drift, Kafka lag, persister quarantine and DLQ replay, stuck and partial jobs, schedule, budget, discovery, erasure, capacity. `igs-go-live.md`: install, upgrade, canary, ramp, pause and roll back. `igs-backups.md`, `igs-phase0.md`, `igs-probe.md`, `igs-registry-sync.md` | Backfill is described inside "Schedule" and the API, not as its own section. A small doc addition | 0.25 day |
| H2 | **Met** | `docs/runbooks/igs-backups.md` (Postgres point-in-time recovery and a daily schema dump, ClickHouse `BACKUP`, S3 versioning and replication, secrets); a restore drill tested in CI (`internal/ig/e2e/backup_test.go`) | - | - |
| H3 | **Partly** | `igs-go-live.md` §8 to §9: a canary of 50 to 100 profiles **for a day**, then a ramp | The standard asks for 2 to 3 days. Fix: change the runbook to 3 days | 0.1 day |
| H4 | **Partly** | Estimates in the readiness review and `igs-operations.md` §13 (requests, GB, IPs, compute, storage) | **Estimates, not measured**, and the first one missed the reels re-read. Fix: the pilot report | In the pilot |

## I. Security

| # | Status | Evidence | Gap and fix | Effort |
|---|---|---|---|---|
| I1 | **Partly** | One database user for igs. The API can use a read-only DSN (`IGS_POSTGRES__READ_DSN`). The chart exposes no database | Every igs binary shares one Postgres credential, and one Valkey, Kafka and S3 credential. Fix: a role per component (the persister writes content; the scheduler writes schedule and jobs; the API reads), and S3 keys per bucket | 1 day |
| I2 | **Missing** | The chart has a NetworkPolicy, **off** by default and **ingress only** | No egress policy. Fix: egress per component: crawlers and registry-sync to the proxy gateways only; every pod to DNS, Postgres, Kafka, the Schema Registry, Valkey, S3 and ClickHouse as it needs (list in `igs-operations.md` §6) | 0.5 day |
| I3 | **Missing** | Go modules pinned; CI runs `go vet` and builds | No dependency updates (Dependabot), no `govulncheck`, no image scan. Fix: Dependabot for Go modules and Actions; `govulncheck` and an image scanner (for example Trivy) in CI | 0.5 day |
| I4 | **Met** | API keys per client, hashed, with scopes and rate limits (`api_clients`); author identity only with `pii:read`; admin scopes for registry, discovery and erasure | - | - |

---

## Plans

Plans only, not built. Each waits for the owner's approval.

### 1. Discovery limits

**Today:**
- Discovery is off in `configs/default.yaml` but **on in the chart's values**.
- Vets are discovery's one-profile reads, one profile read and 12 captions each. They:
  - run on the backfill lane p2, at most 40% of slots and behind scheduled runs;
  - are paused by the daily budget along with backfill.
- `max_new_per_day: 0` means **no daily cap on registrations**, and nothing caps vets per day.
- Vets use the **same exits and identities** as regular collection.

**Proposal:**
1. **Daily caps.**
   - Set `max_new_per_day` to 50 by default (the pilot figure), so no more than 50 accounts are registered a day.
   - Add `max_vets_per_day` (default 300): vets cost requests even when nothing is registered.
   - Candidates over either cap are kept with status `capped` (a status that already exists) and wait for the next day.
   - The owner can change both caps at any time through config, or through a new admin API call if wanted.
   - Effort: 0.5 day.
2. **Iraq-relevance check.**
   - It exists: `internal/ig/discovery/score.go`. It scores the fbs-shared keyword table (editable via API and `igsctl`), +964 and 07xx phones, `.iq`, Iraqi places in three scripts, Sorani letters, dialect, and the Iraqi audience (how many registered accounts link to the candidate). Another country without Iraq counts against. Threshold 4. Private accounts go to the second list (`private`).
   - What's missing is a **measured precision**. Plan: in the pilot, the owner labels a sample of 100 decisions (registered and below), so we learn how often it is right and set the threshold from that.
   - A `reject` with a reason already exists in the API. Count rejects per day as a quality metric.
   - Effort: 0.5 day for the metric and the sample export.
3. **Discovery can't take regular collection's sessions or proxies.** In order of preference:
   - (a) **A share of each exit for p2.** A separate Valkey rate key gives discovery vets at most 10% of each exit's `exit_rps`. Scheduled and on-demand work keep the rest even when discovery is busy.
   - (b) **Vets only when the schedule is on time.** The leader issues vets only while schedule lag is under one interval and no exit tier is mostly resting.
   - (c) **When the owner can afford it,** a dedicated proxy tier for discovery (`ig.discovery.tier`). Vets then never touch regular collection's IPs or identities.

   I recommend (a) and (b) now: about 1 day. (c) is config and routing, 0.5 day, plus the IPs.

### 2. Profile links

**Confirmed:**
- The bio, **every** bio link (URL and title, `lynx_url` as a fallback) and the website (the first link) are captured from the first read.
- They're stored in `profiles.bio_links` and published on `ig.profile.v1` and `content.account.v1`.

**To fix:**
1. **The linked Facebook page.** Today igs keeps its name and drops its ID, then puts the name in `Link.url`.
   - Plan: an added field `Profile.linked_facebook_page_id` and a column.
   - On `content.account.v1`, a link with `platform = fb` and the page ID instead of the name in `url` (proposed to the master in the contract notes).
   - Effort: 0.5 day.
2. **Link history.** Postgres keeps the latest set; the event stream keeps changes for 7 days.
   - Optional plan: a `profile_edits` table like `post_edits`, holding the replaced bio and links.
   - Effort: 0.5 day.
3. **Bio-link keys dropped today** (`link_type`, `link_id`): keep them in `extras`. Minutes.

### 3. Raw responses

**Kept today:**
- **every** answer, as zstd JSON lines `{at, query, class, vars, body|text}` in `igs-raw` (`raw/`, and `raw-failures/` for failed parses);
- no lifecycle rule, so kept forever;
- every event's `raw_ref` = `s3://bucket/key#line`.

Estimated growth: 2 to 4 GB a day per 1,000 accounts.

**Kept for re-parsing (the plan):**
1. Add the parser version and doc ID to every line, so a re-parse knows which lines a fixed parser affects. Effort: 0.25 day.
2. `igsctl raw reparse --query Q --from T1 --to T2 [--parser-below N]`:
   - reads the lines and runs the current parser;
   - publishes the results as new `ig.*` versions (mode `recrawl`, the original observation time kept in the event);
   - the persister and normalizer handle them as usual.

   Effort: 2 days, with a test on fakeig raw lines.
3. Cost control without deleting (the owner keeps all data): a bucket lifecycle rule that moves raw objects older than 30 days to a cheaper storage class, where the provider has one. Config only.

### 4. Memory and soak

**Today:** not measured. The load test (21 s) and the 4-minute soak didn't record memory. The 90-minute soak workflow has never run.

**Plan:**
1. **A 24-hour soak on a machine of our own.** Use the pilot droplet before the pilot, not GitHub's runners, which stop at 6 hours.
   - Set-up: k3s, the chart with `examples/fakeig.yaml`, and fakeig changing its world (posts, comments, edits, deletions, rate limits) as `make igs-soak` does.
   - Load: 500 profiles.
   - Chaos: a crawler pod killed every 2 hours; Valkey restarted once.
   - Every minute, record each pod's memory and CPU (`kubectl top`, metrics-server), plus Postgres, S3 and Valkey sizes.
   - **Pass:**
     - no pod's memory rises steadily after the first hour;
     - no out-of-memory kill;
     - goroutines flat;
     - the store ends equal to fakeig's ground truth;
     - disk growth matches the row counts.
   - Effort: 0.5 day to script, 24 h to run, and a short report.
2. **Real traffic, in the pilot.** The same per-pod figures under Instagram traffic: typical, 95th percentile and peak. Each memory limit is then set at about 2× the peak.

### 5. A pilot like fbs's

**Before the pilot** (the "Fix order" below, about 7 days), at least:
- D4, exit names;
- E3, no `content.*` registration;
- the reels window (below);
- B6, the limiter not failing open;
- discovery caps;
- C5, the pool gauge and alert;
- the soak above.

**The reels window.**
- Today every visit re-reads the reels tab a year back.
- Proposal: on scheduled visits, read reels only as far as the post re-read window (7 days), with a full year pass once a week.
- This cuts reels requests about 20 times on accounts with many reels. Reel views older than 7 days are then refreshed weekly instead of every visit.
- **This is an owner decision on freshness.**
- Effort: 0.5 day.

**Machine.**
- **Its own droplet**, not the shared pilot server: 4 vCPU and 8 GB, k3s, the igs chart (every component with its memory limit), in-cluster Postgres, Redpanda, Valkey and MinIO. Browser off.
- If the owner wants the shared server instead, **strict limits**:
  - Docker Compose with a memory limit per service (all of igs at most 2.5 GB, a crawler at most 512 MB);
  - 50 accounts;
  - one crawler per stage;
  - the run stops if free memory falls under 1 GB.

**Accounts and IPs:**
- 150 accounts from the owner's Phase 0 list (bulk import): 10 hot (10 min), the rest auto-placed (warm or long tail).
- Discovery off for the first 24 hours, then on with caps of 50 registrations and 300 vets a day.
- IPs: the 10 ISP IPs, with the 3 flagged ones replaced.
- `ig.budget` set to the expected day's requests plus 50%.

**Duration:** 48 hours.

**Tests during the run:**
- at about 12 h, a crawler pod restarted;
- at about 24 h, every exit paused for 1 hour (how is to be agreed; for example, the proxy tiers removed for an hour and put back);
- at about 36 h, Valkey restarted.

**Report** (counts only, Iraq time; per account per day and in total):
- requests, by query;
- proxy GB;
- page loads per IP;
- blocks by type, and IPs resting over time;
- identities alive over time;
- posts, comments and reels per hour;
- completeness: comments collected against reported, and walled posts;
- schedule lag (the hot tier included);
- dead-letter queue and stuck jobs;
- peak and 95th-percentile memory and CPU per pod;
- Postgres, S3 and Kafka growth per day;
- discovery: candidates, vets, registered, private and capped, and precision on a labelled sample;
- 10 accounts checked by hand against a logged-out browser;
- **a projection to 1,000 and 10,000 accounts:** requests, GB, IPs, CPU and memory, storage, and cost per month.

**Pass criteria:**
- under 5% of requests blocked;
- no out-of-memory kill;
- no account more than one interval past due (3 h for long tail, 10 min for hot);
- the DLQ near 0;
- the 10 manual checks match;
- no IP flagged that wasn't flagged before.

**The owner starts and stops it.** I'll give the commands, one per block, once the plan is approved.

---

## Fix order (proposed)

Before the pilot, about 7 days:

| Order | Item | Effort |
|---|---|---|
| 1 | E3: no `content.*` registration; production example `hooks.schemas: false` | 0.5 day |
| 2 | D4 and F2: exits named by tier and number; no usernames in logs | 1.5 days |
| 3 | B6: the limiter doesn't fail open | 0.5 day |
| 4 | Reels window (owner decision) | 0.5 day |
| 5 | Discovery caps and the exit share (Plans 1) | 1.5 days |
| 6 | C5 and F1: pool gauge and alert, page loads per exit | 1 day |
| 7 | B5: park identities every few minutes | 0.5 day |
| 8 | E9: linked Facebook page ID | 0.5 day |
| 9 | The 24-hour soak | 0.5 day, plus 24 h |

After the pilot:
- E7, comment deletions and 30-day re-reads;
- E10, walled posts and per-post coverage;
- E8, parser version and re-parse;
- post metrics from post pages;
- F3 and F5, alerts and `igsctl health`;
- I1 to I3;
- G4, gitleaks;
- A6, migration check;
- B3, browser container;
- B7, Iraq time in output;
- H3, three-day canary.

Then adopting the master's v1 (E2, G5): about 7 to 8 days, once v1 is published.
