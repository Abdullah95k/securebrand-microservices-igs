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
| Discovery | `igs_discovery_vets_total{outcome}`, `igs_discovery_candidates{status}` | vets flowing; `failed` rare |
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
- **Rests hold for every pod** (`ig.share_exit_rests`): a pod that rests an exit records it in Valkey, and every crawler and registry-sync pod reads the shared rests every `ig.exit_rest_sync` (2 s). So a limited IP sees one 429 for the whole fleet, not one per pod. A failed probe's longer rest is shared the same way. After a rest each pod may probe the exit once, but the exit's fleet-wide pacing spaces those probes 4 s apart, and the first failed one rests it again for all. `TestIdentitiesAndRestsOutliveAPod` checks a rest taken in one crawler holds in another.

**Sessions across restarts.** A crawler pod that stops (a rollout, a scale-down) keeps its idle identities in Valkey for `ig.keep_identities` (1 h): their cookies and tokens, never the proxy credentials. The pods that start next take them up exit by exit (`igs_identity_unparked_total`), so a rolling restart does not load a page on every exit again. A pod that crashes loses its identities; every page load still waits for its exit's pacing, so even then the IPs do not all load pages at once.

**IPs shared with fbs, yts and tks.** igs paces and rests an exit only for its own requests. When the same IPs also carry the other collectors' traffic:
- Instagram's limit counts only requests to Instagram, so igs's pacing stays right.
- Meta may judge an IP by its Facebook and Instagram traffic together. A run of Facebook blocks on an IP can show up in igs as page walls or checkpoints on that exit; igs then rests it like any other.

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

## Data retention

igs keeps what it collects forever (the owner, 2026-11-03): profiles and their snapshots, posts (with each highlight's items as posts of kind `highlight`), comments and their authors, post and comment metrics, highlights, and discovery candidates with every vet. `privacy.retention.comments_days` and `metrics_days` are 0: no partition is dropped by age.

**Append, don't overwrite** (architecture §2.4):
- A caption's or a comment's text before each edit is kept (`ig.post_edits`, `ig.comment_edits`), with when the new text was seen.
- A post's likes, comments and views (`post_metrics`) and a comment's likes and replies (`comment_metrics`) are kept each time they change.
- A post, comment or highlight item that disappears is marked `unavailable` or `deleted`, never removed.

**Raw responses are kept** (`ig.capture_raw: all`, architecture §2.5): every GraphQL answer and post page goes to the raw bucket as zstd JSONL (`raw/<query>/<yyyy/mm/dd/hh>/...`, failures under `raw-failures/`). Every event carries the reference of the response it came from (`raw_ref`: `s3://bucket/key#line`), so a parser fix or a new field can be applied to past data without collecting again. They take about as much as the proxy traffic before compression, a fifth to a tenth of it after: plan the raw bucket for 0.1 to 0.5 GB a day per 1,000 long-tail profiles, more with hot and warm ones.

These still expire, being operational:
- finished jobs after `jobs_days` (90);
- media rows after `media_rows_days` (30) once their files are deleted.

Media files are deleted once the analysis service acknowledged them (or after `media.ttl`, if set).

An erasure request (`POST /v1/erasure-requests`) still deletes a person's data, including discovery rows, and suppresses them from later crawls.

## Profile discovery

`ig.discovery` (M11; on in the chart's values) grows the registered set without a cap. It adds profiles that are Iraqi, or that address an Iraqi audience, found from the profiles already registered.

1. **Sightings.** Crawlers count the accounts they meet in the posts they read: tagged accounts, co-authors, mentions, and commenters (commenters only with `pii_mode: full`). Erased people are never counted.
   - They also record which registered accounts each one was met in (`ig.discovery_sources`). How many different registered accounts link to an account is its Iraqi audience (`sources`).
   - Each sighting adds to the account's priority: a co-author 3, a tag or a mention 2, a comment 1, a sighting on a post located in Iraq 2 more, and each new registered account it is met in 3.
2. **Vetting.** Every `interval`, the leader scheduler vets the highest-priority accounts at `min_priority` (2) or above, up to `vet_in_flight` (50) at once. A vet is a backfill-lane (p2) job of kind `discovery`:
   - it reads the profile and its newest 12 posts' captions, 2 or 3 requests;
   - it writes them to `ig.discovery_candidates`;
   - it publishes nothing, so accounts that are not registered never reach the analysis.
3. **Score.** The vetter scores what the vet read ([`internal/ig/discovery/score.go`](../../internal/ig/discovery/score.go)). The account's own words count most:

   | Signal | Points |
   |---|---|
   | Iraq named, in any script, or 🇮🇶 | 3 |
   | An Iraqi phone number (+964, 07xx) | 3 |
   | An `.iq` link | 2 |
   | An Iraqi city or province in the name or bio | 2; 3 for two or more; 1 if only in captions |
   | Kurdistan named | 1.5 |
   | Sorani Kurdish letters | 1.5 |
   | Iraqi dialect | 1; 2 for three or more markers |
   | Mostly Arabic script | 1 |
   | Another country named without any of the above | −2 |
   | **Iraqi audience:** different registered accounts linking to it | 1 for one; 1.5 for two; 2 for three; 2.5 for five; 3 for ten or more |
   | Seen on posts located in Iraq | 1.5 |
   | A co-author of a registered account | 0.5 |

   - **Network cap:** the last three rows (network evidence) add up to at most 4. That is enough alone only with ten or more registered accounts linking to the account from posts located in Iraq. So an Iraqi influencer whose profile says nothing of Iraq is added once a real Iraqi audience surrounds them. The same goes for a foreign account that targets Iraqis.
   - **World stars:** an account with a million followers or more and nothing of Iraq of its own gets at most 2 from the network. A world star Iraqi fans tag is not added.
   - **Examples** (`TestInfluencers`):
     - an Iraqi influencer with Baghdad, 🇮🇶 or an 07xx number in the bio is added at the first link;
     - one writing Iraqi dialect is added at the first link too;
     - a Kurdish one writing Sorani, one with an Arabic bio and no place, one writing only in English, and an Arabic-language foreigner are added once ten Iraqi accounts link to them from Iraq;
     - a Gulf account naming Dubai and a global English star are not added.
4. **Decision.**
   - Public and at the threshold or above: registered for the 3-hourly schedule under the API client `discovery`, and backfilled.
   - With a daily cap (`max_new_per_day`, below) that is used up for the day: `capped`. Capped candidates are registered on the next days, highest score first, before any new one.
   - Otherwise: `below`, `private` or `gone` (no such account). These are vetted again if seen again after `revet_after` (30 days).
   - Every vet is kept: the latest read on the candidate (shown as `read` in the API), and each one with its score, signals and decision in `ig.discovery_vets`. Erasure requests delete a person's rows.
   - A `below` candidate whose audience grows is scored again from its stored read, with no new read of Instagram (`igs_discovery_vets_total{outcome="rescored_registered"}`).

5. **Intake.** Accounts found elsewhere join as candidates, vetted and scored like the ones the crawl meets: the Google discovery tool, links on Facebook pages and YouTube channels, or a list by hand.
   - Each candidate's `origins` says where it came from: `network` (met in posts igs reads) or the intake's name.
   - Intake accounts are vetted ahead of single sightings. Coming from an intake adds nothing to the score: only what the vet reads, and the Iraqi audience, decide.
   - An account already a candidate keeps its status; the intake only adds its origin.

Operations:
- `GET /v1/discovery/candidates?status=registered` (admin) lists candidates with their scores, parts and origins.
- `POST /v1/discovery/candidates/{username}/reject` takes a discovered profile off the schedule and keeps it from being discovered again. An admin's `DELETE /v1/profiles/u:{username}` does the same.
- `POST .../accept` registers a candidate whatever its score.
- `POST /v1/discovery/candidates` (admin) takes up to 1,000 accounts a call, as usernames or profile links, with their origin:

  ```bash
  curl -H "Authorization: Bearer $ADMIN_KEY" -d '{"origin":"google","accounts":["<username>","https://www.instagram.com/<username>/"]}' https://<igs.example.com>/v1/discovery/candidates
  ```

  It answers how many were `added`, how many were `known` already, and which were `invalid`.
- `igsctl discovery import` does the same from a file, one account a line (`#` starts a comment; `--file -` reads standard input):

  ```bash
  kubectl -n igs exec -i deploy/igs-api -- igsctl discovery import --origin facebook --file - < accounts.txt
  ```

- **Metrics:**
  - `igs_discovery_sightings_total{source}`;
  - `igs_discovery_vets_total{outcome}`;
  - `igs_discovery_candidates{status}`, where `new` counts the accounts due a vet.
- **Tuning:**
  - Raise `threshold` if registered profiles turn out not to be Iraqi; lower it, or `min_priority`, to find more.
  - Vets share the exits with crawling (about 10 s of one exit's time each) and wait behind scheduled runs (lane p2). The daily budget holds them with backfill.
  - To slow discovery, lower `vet_in_flight`.
  - **Daily cap:** `max_new_per_day` (0, no cap, by default) is the most profiles discovery registers a day. The day starts at 00:00 UTC, 03:00 Iraq time. Over it, candidates wait as `capped` (`igs_discovery_candidates{status="capped"}`) and are registered on the next days. Profiles registered by hand or with `accept` neither count towards it nor wait for it. Setting the cap back to 0 lets every capped candidate through on the next vetter tick.

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
- **Normalizer lag (`IgsIgNormalizerLag`):** `content.*` events reach ClickHouse, the analysis and the alerts late (the shared contract below).
  - KEDA scales igs-normalizer on the same topics' lag; check its pods and Kafka.

The leader scheduler publishes the consumer groups' lag every 30 s, so no Kafka exporter is needed. The crawler groups are `igs-ig-crawler-<stage>-<lane>`, the persister's is `igs-ig-persister`, the normalizer's `igs-ig-normalizer` and the downloader's `igs-ig-media-downloader`. All of them carry `kafka.topic_prefix`.

## The shared content contract

`igs-normalizer` maps igs's own topics to the shared `content.*` contract (architecture §5) that storage (ClickHouse), the analysis and the alerts read. Its schema is `api/proto/securebrand/content/v1/content.proto`, kept in igs until the shared schema repository exists.

| igs topic | content events |
|---|---|
| `ig.profile.v1` | `content.account.v1` (handle, name, bio, website, every profile link and the linked Facebook page, for the entity layer) and a `content.metrics.v1` account sample |
| `ig.post.v1` | `content.post.v1` (kind post, video or reel) |
| `ig.post_metrics.v1` | `content.metrics.v1` (target post) |
| `ig.comment.v1` | `content.comment.v1` (keyed by its post) and a `content.metrics.v1` comment sample |
| `ig.media.v1` | `content.media.v1` |
| `ig.highlight.v1` | a `content.post.v1` of kind `highlight` per item |

Every event carries the envelope: `platform` (`instagram`), `native_id`, `observed_at`, `published_at`, `version` (newest wins), `collector`, `collector_version` and `raw_ref`. Records are keyed `instagram:<native_id>`. A redelivered record maps to the same event ids, so consumers stay idempotent.

## The shared content store (ClickHouse)

`igs-content-writer` fills ClickHouse (architecture §6) from the `content.*` topics. It knows no platform, so the other collectors' normalized events land in the same tables. Its schema is `migrations/clickhouse/`:

| Table | One row per | Engine |
|---|---|---|
| `posts` (highlight items as `kind = 'highlight'`) | post | ReplacingMergeTree(version, is_deleted), by publication month, ordered by (platform, post_id) |
| `comments` | comment or reply | the same, ordered by (platform, post_id, comment_id) |
| `accounts` (bio, website, `link_urls` for the entity layer) | account | ReplacingMergeTree(version, is_deleted) |
| `authors` | comment author (by id, or keyed hash) | ReplacingMergeTree(version) |
| `media` | image or video | ReplacingMergeTree(version, is_deleted) |
| `post_metrics`, `comment_metrics`, `account_snapshots` | observation | ReplacingMergeTree on (id, observed_at), by observation month: a redelivered row collapses |
| `entities`, `entity_accounts` | entity, and each account linked to it (how, confidence, who confirmed) | ReplacingMergeTree(version, is_deleted); filled by entity linking and the client app |

- Ids are text; `*_key` columns hold `cityHash64(platform, id)` for joins.
- Read the newest version of an entity with `FINAL` (and `WHERE is_deleted = 0`), or `argMax(..., version)`.
- **Turning it on:**
  1. set `config.clickhouse` (addresses, database, user; the password in `IGS_CLICKHOUSE__PASSWORD`);
  2. on a replicated cluster set `cluster`: every table is created `ON CLUSTER` with `Replicated*` engines (at least two replicas and three Keeper nodes);
  3. with `storage_policy` and `cold_after`, partitions older than `cold_after` move to the policy's `cold` volume (object storage), still queryable;
  4. enable `components.content-writer`; the install hook runs `igsctl clickhouse migrate`.
- **Lag:** `IgsContentWriterLag`; `igs_content_written_total{table}` counts rows written. A failed insert is retried until it succeeds; nothing is committed past unwritten rows.

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

The planner runs each scheduled profile at its tier's interval, and its profile query every `ig.schedule.profile_refresh` (24 h). The post refresh re-reads the comments and counts of posts from the last `ig.schedule.post_refresh_window` (7 days), `post_refresh_per_run` posts at a time; each post's refreshes start at its profile's interval and double.

**Tiers** (`ig.schedule.tiers`, architecture §8):

| Tier | Interval | Who |
|---|---|---|
| hot | 10 min | Pinned: client brands, their competitors, trending accounts |
| warm | 1 h | Not pinned and posting 2 or more times a day over 14 days |
| long_tail | 3 h | Not pinned, everyone else; the default |
| custom | its own | A profile given its own interval (at least `min_interval`, 5 min) |

- A profile not pinned is placed by its posting rate as soon as its backfill gives its user pk, then checked daily (`auto_every`). Re-registering it keeps its placement.
- Pin a client watchlist's accounts to hot, and release them with `auto`:

  ```bash
  kubectl -n igs exec -i deploy/igs-api -- igsctl profiles tier --tier hot --file - < watchlist.txt
  ```

  The API does the same per profile (`PATCH /v1/profiles/{profile}` with `{"schedule":{"tier":"hot"}}`) or in bulk (`POST /v1/profiles/bulk` with `"schedule":{"tier":"hot"}`).
- **Cost:** each run is 5 to 10 requests, and one exit sends 900 an hour. So one exit carries 15 to 30 hot profiles, 90 to 180 warm ones, or 270 to 540 long-tail ones. `igs_profiles_scheduled{tier}` counts the profiles per tier; size the exits from it.

`IgsIgScheduleLag` fires when profiles are a full interval overdue: the fleet does not keep up. Then:
- add crawler capacity (KEDA max replicas) or ISP exits;
- check the block rate;
- or lengthen the interval.

## Media backlog / TTL expiries

Media files are downloaded only with `ig.limits.include_media` (D5: metadata always, files once the analysis service exists). Instagram's CDN URLs expire within hours, so a backlog on `igs.task.media` turns into failed downloads (`IgsIgMediaDownloadsFailing`). Scale the downloader, or check the CDN responses.

The janitor deletes a file once every `media.required_consumers` acknowledged it (`ig.media.processed.v1`): media is kept until its analysis is complete (architecture). With `media.ttl` set (0 by default), it also deletes a file not acknowledged within that time; `IgsIgMediaTTLExpiries` then means the analysis service does not keep up with `ig.media.ready.v1`, or its acks do not arrive. With no TTL, watch the media bucket's size instead: files waiting for analysis stay.

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
