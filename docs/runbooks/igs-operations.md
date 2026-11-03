# igs operations runbook

igs is the logged-out Instagram scraper. Every binary exposes `/metrics`, `/healthz` and `/readyz` on `:9090`, and every metric name starts with `igs_`.

The binaries:
- **`igs-api`:** the REST API, `/v1`, described in `api/openapi/igs.v1.yaml`.
- **`igs-scheduler`:** leader-elected. It runs the 3-hourly planner, the post refresh, the finalizer, partitions, AIMD, consumer lag and erasure.
- **`igs-crawler --stages profile|post|retry`:** the crawl stages.
- **`igs-persister`:** writes the results to Postgres schema `ig`.
- **`igs-media --roles downloader|janitor`:** downloads media and expires it.
- **`igs-registry-sync`:** keeps the query registry's doc_ids current.

`igsctl` is the operations CLI.

Deployment is the `deploy/helm/igs` chart. Its hooks run `igsctl migrate up` and `igsctl kafka apply` before each install and upgrade. Runbooks for the other jobs:
- going live (release, secrets, install, canary, ramp): [`igs-go-live.md`](igs-go-live.md);
- the Phase 0 kit: [`igs-phase0.md`](igs-phase0.md);
- the probe: [`igs-probe.md`](igs-probe.md);
- doc_id rotations: [`igs-registry-sync.md`](igs-registry-sync.md).

## Key signals

| Signal | Metric | Healthy |
|---|---|---|
| Request outcomes | `igs_ig_requests_total{query,class,tier}` | `ok` ≥ 95% |
| Block rate | classes `rate_limited`, `login_wall`, `checkpoint`; `igs:ig_block_ratio:rate5m`; `igs_fleet_block_rate` | < 5% (Phase 0's threshold) |
| Fleet ceiling | `igs:ig_leader:aimd_ceiling_rps`, `igs:ig_leader:fleet_rps` | ceiling stable or rising; fleet rate close to it while there is backlog |
| Identity churn | `igs_identity_retired_total{tier,reason}` | mostly `max_requests` (identities retire after 500 to 750 requests by design) |
| Exits | `igs_exits_resting{tier}` against `igs_exits{tier}`; `igs_exit_rests_total{tier,reason}`; `igs_identity_hops_total{stage,class}` | few exits resting; rests and failovers flat |
| Parked tasks | `igs_tasks_total{outcome="parked"}` | 0: a task parks only while every exit rests |
| Daily budget | `igs_budget_projected{kind}` against `igs_budget_limit{kind}`; `igs_budget_backfill_paused` | under the limit; 0 |
| Breakers | `igs_breaker_state{name="proxy:..."}`, `{name="query:..."}` (2 = open) | all 0 |
| Parser health | `igs_parse_total{outcome="drift"}`, `igs:ig_parse_missing_per_response:rate1h` against 7 days ago | drift ≈ 0; optional fields as last week |
| Registry | `igs_registry_query_healthy{query}`, `igs_registry_sync_last_success_timestamp_seconds` | all 1; last success < 1 h ago |
| Lanes | `igs_lane_slots{stage,lane,state}` | `waiting` on p0 ≈ 0 |
| Consumer lag | `igs:ig_leader:kafka_consumer_lag{group,topic}` (groups `igs-ig-*`) | p0 groups ≈ 0; persister < 1 min |
| Schedule | `igs:ig_leader:schedule_lag_seconds` | below `ig.schedule.interval` (3 h) |
| Jobs | `igs_jobs_total{status,lane}` | `completed` dominates; `partial` low |
| Retries / DLQ | `igs_retries_total{stage}`, `igs_dlq_total{stage,reason}` | low, flat |
| Persister | `igs_persist_rows_total{entity}`, `igs_persist_batch_seconds`, `igs_persist_quarantined_total` | quarantine 0 |
| Media | `igs_media_download_total{kind,outcome}`, `igs_media_deleted_total{reason}` | `ttl_expired` ≈ 0 |
| API | `igs_api_requests_total{code}` | 5xx < 1% |

The leader scheduler alone publishes the AIMD, schedule-lag and consumer-lag values. Every igs binary exports those gauges at 0, and a former leader keeps its last values, so the rules read them through the `igs:ig_leader:*` recording rules, which join on `igs_scheduler_leader == 1`. Do the same in ad-hoc queries.

## Alerts and dashboards

- **Alerts:** the alerting and recording rules are in `deploy/helm/igs/files/alerts.yaml`. Set `alerts.enabled` to install them as a PrometheusRule. Every alert links to the section of this runbook that handles it.
- **Dashboard:** **igs / Instagram overview** (uid `igs-ig-overview`) is in `deploy/helm/igs/files/dashboards/`. Set `dashboards.enabled` to install it for the Grafana sidecar.
- **Thresholds:** they are starting points; tune them against the Phase 0 baseline (C3).
- **Consistency test:** `internal/platform/metrics/monitoring_test.go` checks three things:
  - every metric in the rules and the dashboard exists and is one igs emits;
  - every alert links to a heading of this page;
  - the dashboard's uid differs from the fbs chart's.

## Rotation, retries and failover

Instagram limits each IP, not each identity: the owner's probe met error 1675004 after 1,100 to 1,300 requests on one exit at 0.3 a second, and the next exit read normally (U4). The crawler is built around that.

**Pacing.** Every request waits on three limits:
- its identity's (`ig.identity_rps`, 0.33 a second, with jitter);
- its exit's, fleet-wide in Valkey however many identities and pods share the exit (`ig.exit_rps`, 0.25 a second: one request every 4 seconds);
- the fleet ceiling (`ig.global_rps`, moved by AIMD).

The fleet therefore reads at most exits × `ig.exit_rps`: 2.5 requests a second on 10 exits. Adding exits is how igs scales; adding crawler pods is not.

**Rotation.**
- Identities spread over the exits. A task gets an idle identity on an exit no other task uses; failing that, a new identity on such an exit; failing that, the idle identity on the least used exit.
- An identity retires after 500 to 750 requests (`ig.identity_max_requests_min`/`_max`), at its fifth rate limit in a row, at once on a login wall or a checkpoint, and when its exit starts resting.
- A static exit is one IP: its host and port (Decodo names its exits by port). A gateway session is its own exit.

**Exit rests.** One answer is enough to rest an exit, since the limit is per IP:

| Answer | Rest | Setting |
|---|---|---|
| rate limit (429, 1675004) | 10 min | `ig.rate_limit_rest` |
| checkpoint (challenge) | 30 min | `ig.checkpoint_rest` |
| a fresh visitor's page load sent to the login page | 30 min | `ig.page_wall_rest` |

- While an exit rests, its identities send nothing more. A request already waiting on the exit's pacing is dropped unsent and counts as the exit's rate limit, so a limited IP sees one 429, not one per identity queued on it. `TestExitFailoverEqualsTruth` checks this.
- After the rest, one identity probes the exit. If the probe is blocked again, the exit rests twice as long, up to `ig.proxy.breaker.max_cooldown` (30 min). Three good answers put it back in use.
- Separately, the exit's bucket (its /24, or its host and port) opens its breaker at a 15% failure rate over a minute.
- Rests are per crawler pod: each pod learns of a limited exit from its own first 429.

**In-task failover.** When a request is blocked (rate limit, checkpoint, login wall), the task takes an identity on another exit, bootstraps it if new, and carries on from the same page. It does this up to `ig.block_hops` (2) times a run before it waits for a retry. A login wall at the same grid or comment cursor on three fresh identities still ends that stream as truncated (U1), whether the three came in one run or across retries.

**Retries.** A task that could not finish goes to a retry topic with its checkpoint:
- The first retry without progress waits 30 s; later ones wait 5 min. Each delay gets up to 25% added, so tasks blocked together do not return together.
- A run that saved a new page starts the count again.
- After `crawler.max_attempts` (5) retries without progress, about 15 minutes, or `crawler.max_total_attempts` (50) in all, the task goes to the DLQ (`igs_dlq_total{reason}`). [Replay it](#stuck-jobs) with `igsctl dlq replay` once the cause is gone.
- Proxy tiers escalate on retries without progress, cheapest first, skipping tiers whose exits all rest. igs routes everything to `isp`, so this matters only if more tiers are added.

**Every exit resting.** A task that finds no usable exit parks: it waits 5 minutes, without counting toward `max_attempts`, and tries again. `max_total_attempts` bounds parking at about 4 hours. Parked tasks show as `igs_tasks_total{outcome="parked"}`. `IgsIgExitsMostlyResting` and `IgsIgTasksParked` fire before tasks start reaching the DLQ.

What to do when exits keep resting:
1. Compare `igs_exit_rests_total` by reason with `igs_exits_resting` and `igs_exits`.
2. **Rate limits:** a steady stream of `rate_limited` rests means `ig.exit_rps` is above what Instagram tolerates per IP. Lower it, or add exits to keep the throughput.
3. **Checkpoints and page walls:** these mean the provider's IPs are flagged. Replace those endpoints in the `igs-proxies` Secret.
4. **Every exit resting:** the crawl stops until rests end. If it lasts, add exits from another provider or subnet.

## Daily budget

`ig.budget` (D9) caps the fleet's daily requests and wire bytes (page loads and queries); both are 0, no budget, by default. Exit pacing already bounds the request rate (exits × `ig.exit_rps`), so the byte budget is the one to set when the proxy plan bills per GB.

- Crawlers count requests and bytes per lane in Valkey. Every `ig.budget.interval`, the leader scheduler projects the day's total (UTC) from the last `projection_span`.
- When the day would exceed the budget, backfill (p2, new profiles' first reads) pauses: crawlers take its slots away, and running tasks finish.
- When on-demand and scheduled traffic alone would exceed it, every scheduled run's interval is multiplied by `ig.budget.stretch` (2: every 6 hours instead of 3).
- On-demand (p0) work is never held. The state expires if the leader stops publishing it, so crawlers never stay paused on a stale decision.
- Watch `igs_budget_used{kind,lane}`, `igs_budget_projected{kind}`, `igs_budget_limit{kind}`, `igs_budget_backfill_paused` and `igs_budget_stretch`.
- `IgsIgBudgetHoldingBackfill` fires after 2 hours of held backfill.

## Block storm (block rate > 10%)

The fleet reacts on its own at four levels:

- **Identity:**
  - A rate-limited identity cools down, then retires at the fifth limit in a row (`reason="blocked"`).
  - A login wall or a checkpoint retires it at once.
  - Otherwise it retires after 500 to 750 requests (`ig.identity_max_requests_min`/`_max`). The owner's probe met a limit after 1,100 to 1,300 requests on one exit.
- **Exit:** a rate limit rests the exit for `ig.rate_limit_rest` (10 min), a checkpoint for `ig.checkpoint_rest` (30 min), and a fresh visitor's page load sent to the login page for `ig.page_wall_rest` (30 min). The blocked task fails over to another exit. See [Rotation, retries and failover](#rotation-retries-and-failover).
- **Exit bucket:** a bucket opens its breaker at `ig.proxy.breaker.open_rate` (15%) over `window` (1 min). It gets no new identities until a probe passes after `cooldown`.
- **Fleet:** every `ig.aimd.interval`, AIMD on the leader scheduler reads the fleet's block rate:
  - above `ig.aimd.high_block_rate` (6%) the ceiling drops to 70% of the rate actually sent;
  - below `low_block_rate` (2%) it grows by 2% a window.

What to check:

1. `igs:ig_leader:fleet_block_rate` and `igs:ig_leader:aimd_ceiling_rps`. The ceiling should be falling while blocks are high.
2. `igs_breaker_state{name=~"proxy:.*"}` and `igs:ig_requests_by_class:rate5m` by tier. igs runs on ISP proxies only, so a provider-wide problem is one for the provider. Remove dead endpoints from the `igs-proxies` Secret.
3. `igs_identity_retired_total` by reason. `IgsIgIdentitiesRetiringFast` fires when most identities retire on blocks rather than at their budget. If it does, lower `ig.exit_rps` or add exits.
4. Login walls on page loads mean exits are resting: watch `class="login_wall"` on the `page` queries. A wall at the same grid depth everywhere is coverage, not blocking (U1): jobs end `partial` (see [Partial jobs](#partial-jobs)).
5. If Instagram answers with a code or page the rules do not know, responses show as `soft_error`. Add the marker under `ig.detect` (appended to the built-in rules) and roll the crawlers.

To pin the ceiling by hand during an incident, set the key the crawlers read. They pick it up within 5 s; the key starts with `igs:` and then the topic prefix:

```bash
redis-cli SET "igs:${KAFKA_TOPIC_PREFIX}aimd:global_rps" 10
```

AIMD keeps adjusting from that value. Set `ig.aimd.enabled: false` to stop it; the crawlers then use the fixed `ig.global_rps`.

## Registry stale (doc_id rotated)

Instagram rotates the doc_ids of its GraphQL operations with its deploys. Symptoms:
- requests classed `registry_stale`;
- the query breaker opens (`IgsIgQueryBreakerOpen`);
- `igs_registry_query_healthy{query} == 0`.

`igs-registry-sync` handles it every `ig.registry_sync.interval` (30 min), on the canary profiles in `ig.registry_sync.canary_pages`. It finds doc_ids in the profile page's bundles, or in a headless render when the bundles declare none. It checks each candidate with real requests chained profile → grid → comments → reels → highlights, and promotes one that parses, keeping the former entry as a fallback. [`igs-registry-sync.md`](igs-registry-sync.md) has the details.

1. `GET /v1/admin/registry-sync/runs` shows the last runs: candidates, promotions, `failing`, `not_found`.
2. If no run succeeds (`IgsIgRegistrySyncNotRunning`), check three things:
   - the canary profiles load (public, not deleted, not walled);
   - at least three are set;
   - their posts have comments.
3. If nothing parses, Instagram may have renamed the operation. Capture a HAR on the probe ([`igs-probe.md`](igs-probe.md)). Then set the entry with `PUT /v1/admin/queries/{name}`, or with `igsctl registry set --file query.json`.
4. After Phase 0, `igsctl registry import --file phase0/out/registry.yaml` makes its doc_ids active. Add `--keep-promoted` to leave the sync's promotions alone.

## Browser fallback

When page loads stop carrying the tokens and doc_ids the client needs (JavaScript-only pages), the crawlers can bootstrap through headless Chromium:
- set `ig.browser.enabled: true` and `image.browserTag`;
- the components marked `browser: true` then use the browser image;
- `ig.browser.max_share` and `max_concurrent` bound how much traffic it takes.

- `IgsIgBrowserFallbackSaturated`: more page loads need it than the caps allow; those bootstraps fail and their tasks retry. Raise the caps (and the crawlers' memory) or find what changed in the page.
- `IgsIgBrowserFallbackFailing`: Chromium does not start (check the pod's logs; the chart sets `no_sandbox`, since pods drop every capability) or the rendered page has no tokens either. That is a new wall: capture the page on the probe.

## Schema drift

`IgsIgSchemaDrift` fires when responses of a query miss required fields. Instagram changed the shape.

1. With `ig.capture_raw: failures` (the default), the failing responses are in the raw bucket under `raw-failures/<query>/`.
2. Fix the parser in `internal/ig/parse` against them. Add the scrubbed response as a golden fixture (`internal/ig/fixtures`), then release.
3. `IgsIgOptionalFieldDrift` is the quiet version: an optional field went missing far more often than a week ago. Nothing fails, but the data loses the field.

## Kafka lag

- **On-demand lag (`IgsIgOnDemandLag`):** p0 tasks wait.
  - Check that KEDA scales the stage (`components.crawler-*.autoscale.max`);
  - check the lane slots (`igs_lane_slots{state="waiting"}`);
  - check the AIMD ceiling.
- **Idle crawlers with backlog (`IgsIgCrawlersIdleWithBacklog`):** nothing is processed.
  - Check the crawler pods;
  - check for a pinned ceiling of 0;
  - check whether every exit bucket's breaker is open.
- **Persister lag (`IgsIgPersisterLag`):** results reach Postgres late.
  - Check `igs_persist_batch_seconds` and the database (locks, I/O).
  - KEDA scales the persister on `ig.comment.v1`, `ig.post.v1` and `ig.post_metrics.v1` lag.

The leader scheduler publishes the consumer groups' lag every 30 s, so no Kafka exporter is needed. The crawler groups are `igs-ig-crawler-<stage>-<lane>`, the persister's is `igs-ig-persister` and the downloader's is `igs-ig-media-downloader`. All of them carry `kafka.topic_prefix`.

## Persister quarantine

The persister quarantines what it cannot write on `igs.persist.dlq`, with `x-reason` and `x-error` headers, instead of stalling: undecodable records, or rows Postgres rejects. That data is missing from Postgres until it is fixed and replayed.

1. Read the reason in the logs (`persist: record quarantined`) or in the DLQ headers.
2. Fix the cause (a migration, a parser emitting an invalid value), deploy, then replay:

```bash
igsctl dlq replay --persist
```

The replay puts the records back on their entity topics unchanged. It commits as it goes, so running it again does not repeat records.

## Stuck jobs

- **No leader (`IgsIgSchedulerNoLeader`):** nothing plans runs, finalizes jobs or runs AIMD. Check the scheduler pods and their Postgres connection. The leader lock is an advisory lock.
- **Jobs failing (`IgsIgJobsFailing`):** look at the failed jobs' error (`GET /v1/jobs/{id}`) and at the request classes of their queries.
- **DLQ growing (`IgsIgDLQGrowing`):** tasks dead-lettered after their retries, for example a wall on every attempt, or a private profile. Fix the cause, then:

```bash
igsctl dlq replay --reason <reason>
```

Replayed tasks run under new ids with fresh retry counts, and rejoin their job if it is still running. Without `--reason` every dead letter is replayed.

## Partial jobs

A job ends `partial` when posts failed, or when a grid ended before the job's age window. Two causes:
- a login wall mid-grid;
- Instagram's logged-out grid depth (U1).

`IgsIgJobsPartial` fires when a quarter of jobs end so. The job's completeness (`GET /v1/jobs/{id}`) says which posts and grids. If the grids end at the same depth everywhere, it is the depth limit: raise it with the owner (C1's recordings) rather than with retries.

## Schedule

The planner runs every scheduled profile every `ig.schedule.interval` (3 h, D2), and its profile query every `ig.schedule.profile_refresh` (24 h). The post refresh re-reads the comments and counts of posts from the last `ig.schedule.post_refresh_window` (7 days), `post_refresh_per_run` posts at a time.

`IgsIgScheduleLag` fires when profiles are a full interval overdue: the fleet does not keep up. Then:
- add crawler capacity (KEDA max replicas) or ISP exits;
- check the block rate;
- or lengthen the interval.

## Media backlog / TTL expiries

Media files are downloaded only with `ig.limits.include_media` (D5: metadata always, files once the analysis service exists). Instagram's CDN URLs expire within hours, so a backlog on `igs.task.media` turns into failed downloads (`IgsIgMediaDownloadsFailing`). Scale the downloader, or check the CDN responses.

The janitor deletes a file once every `media.required_consumers` acknowledged it (`ig.media.processed.v1`), or at `media.ttl`. `IgsIgMediaTTLExpiries` means the analysis service does not keep up with `ig.media.ready.v1`, or its acks do not arrive.

## Capacity and sizing

**Pipeline ceilings.** `make igs-loadtest` measures the in-process pipeline against fakeig, on one machine, every component in one process. On 4 CPUs (2026-11-02):

| Run | Wall time | Stored | Rates |
|---|---|---|---|
| 30 profiles × 40 posts, read in full at once | 4.4 s | 1,050 posts, 31,595 comments (all of fakeig's) | 221 GraphQL requests/s, 7,100 comments/s |
| 150 profiles × 40 posts | 21.0 s | 5,250 posts, 155,652 comments | 231 requests/s, 7,400 comments/s |
| Persister alone, 200,000 comments | 13.2 s new, 11.6 s unchanged | | 15,200 rows/s new, 17,200 rows/s no-op upsert |

The persister is the first stage to back up: its backlog peaked at 136,716 records in the 150-profile run, while the crawl stages' stayed near 0. KEDA scales it on `ig.comment.v1`, `ig.post.v1` and `ig.post_metrics.v1` lag.

**What bounds production.** Production's pace is set by Instagram, not by these ceilings:
- Each exit sends at most `ig.exit_rps` (0.25 a second, 900 an hour), so the fleet reads at most exits × `ig.exit_rps`.
- A scheduled run is 5 to 10 requests, so one exit carries roughly 90 to 180 profile runs an hour, or 270 to 540 profiles on the 3-hour schedule.
- Phase 0's full backfill took 37.6 requests a profile: each profile added costs about 150 s of one exit's time once.
- 300k profiles every 3 hours (about 28 runs a second, 150 to 300 requests a second) needs 600 to 1,200 exits at 0.25 a second each.
- Size the exit pool from `igs_exits`, `igs_exits_resting` and the schedule lag: when `igs:ig_leader:schedule_lag_seconds` grows while few exits rest, add exits.

**Valkey.** Most keys are per task:
- **Done marker:** 88 B, kept `stage.DoneTTL` (96 h, longer than the task topics' 72 h retention).
- **Checkpoint:** 344 B, deleted when its task finishes, so only in-flight segments hold one.
- **Job keys:** kept 24 h after the job finishes.

A scheduled run is one profile task plus up to `ig.schedule.post_refresh_per_run` (30) post tasks, typically 5 to 9. At 300k profiles every 3 hours (2.4M runs a day, about 6 tasks each), the done markers alone are about 58M keys, around 5 GB: plan Valkey's memory for that. The soak (`make igs-soak`) prints the key count every 30 s.

## Erasure requests

`POST /v1/erasure-requests` records a request; the scheduler's eraser carries it out within seconds:
- it deletes the subject's rows and media;
- it adds the subject to the suppression list, so later crawls drop or redact them (`igs_persist_suppressed_total`);
- it publishes `ig.erasure.v1`.

`GET /v1/erasure-requests/{id}` shows the progress.
