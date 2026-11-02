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

## Block storm (block rate > 10%)

The fleet reacts on its own at four levels:

- **Identity:**
  - A rate-limited identity cools down, then retires at the fifth limit in a row (`reason="blocked"`).
  - A login wall or a checkpoint retires it at once.
  - Otherwise it retires after 500 to 750 requests (`ig.identity_max_requests_min`/`_max`). The owner's probe met a limit after 1,100 to 1,300 requests on one exit.
- **Exit:** an exit that sends a fresh page load to the login page rests for `ig.page_wall_rest` (30 min).
- **Exit bucket:** a bucket opens its breaker at `ig.proxy.breaker.open_rate` (15%) over `window` (1 min). It gets no new identities until a probe passes after `cooldown`.
- **Fleet:** every `ig.aimd.interval`, AIMD on the leader scheduler reads the fleet's block rate:
  - above `ig.aimd.high_block_rate` (6%) the ceiling drops to 70% of the rate actually sent;
  - below `low_block_rate` (2%) it grows by 2% a window.

What to check:

1. `igs:ig_leader:fleet_block_rate` and `igs:ig_leader:aimd_ceiling_rps`. The ceiling should be falling while blocks are high.
2. `igs_breaker_state{name=~"proxy:.*"}` and `igs:ig_requests_by_class:rate5m` by tier. igs runs on ISP proxies only, so a provider-wide problem is one for the provider. Remove dead endpoints from the `igs-proxies` Secret.
3. `igs_identity_retired_total` by reason. `IgsIgIdentitiesRetiringFast` fires when most identities retire on blocks rather than at their budget. If it does, lower `ig.identity_rps` or add exits.
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

## Erasure requests

`POST /v1/erasure-requests` records a request; the scheduler's eraser carries it out within seconds:
- it deletes the subject's rows and media;
- it adds the subject to the suppression list, so later crawls drop or redact them (`igs_persist_suppressed_total`);
- it publishes `ig.erasure.v1`.

`GET /v1/erasure-requests/{id}` shows the progress.
