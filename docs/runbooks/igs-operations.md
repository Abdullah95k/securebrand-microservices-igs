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

Deployment is the `deploy/helm/igs` chart. Its hooks run `igsctl migrate up` and, on a Kafka of igs's own, `igsctl kafka apply --apply` before each install and upgrade ([topics and schemas](#topics-and-schemas)). Runbooks for the other jobs:
- going live (release, secrets, install, canary, ramp): [`igs-go-live.md`](igs-go-live.md);
- the Phase 0 kit: [`igs-phase0.md`](igs-phase0.md);
- the probe: [`igs-probe.md`](igs-probe.md);
- doc_id rotations: [`igs-registry-sync.md`](igs-registry-sync.md).

## Health

**What to run first** (REQ-008 fix 19). One read-only command reports igs's state:

```bash
kubectl -n igs exec deploy/igs-scheduler -- igsctl health
```

It prints one line per check, then what to do for each line that isn't OK, with times in Iraq time. It exits 1 when a line is `BAD`. Valkey or Kafka that don't answer give a `?` line rather than stopping the report.

| Line | What it reads | `OK` | Not OK, and where to look |
|---|---|---|---|
| profiles on the schedule | enabled profiles by tier, and those paused | some profiles | `WARN` with none: nothing to collect; register profiles |
| schedule | profiles overdue by their own interval (3 h long tail, 10 min hot), the oldest | none overdue | `BAD`: [Schedule](#schedule) |
| jobs, last hour | jobs created in the last hour by outcome, plus those still running or queued | failures under a fifth | `WARN`: a failed job's reason in `GET /v1/jobs/{id}`; [Stuck jobs](#stuck-jobs) |
| last data stored | the newest profile read and post seen | a profile read within 3 h | `BAD` while profiles are on the schedule: [No data](#no-data) |
| proxy exits resting | per static tier, exits resting now (shared in Valkey) | at most half | `WARN`: [Block storm](#block-storm-block-rate--10) |
| Kafka lag | the five igs consumer groups furthest behind | each under 10,000 | `WARN`: [Kafka lag](#kafka-lag) |

The scheduler's pod has the proxy tiers when the `proxies` Secret exists, so its exits line names each tier. The command works from any igs pod and changes nothing.

## Key signals

| Signal | Metric | Healthy |
|---|---|---|
| Request outcomes | `igs_ig_requests_total{query,class,tier}` | `ok` ≥ 95% |
| Block rate | classes `rate_limited`, `login_wall`, `checkpoint`; `igs:ig_block_ratio:rate5m`; `igs_fleet_block_rate` | < 5% (Phase 0's threshold) |
| Fleet ceiling | `igs:ig_leader:aimd_ceiling_rps`, `igs:ig_leader:fleet_rps` | ceiling stable or rising; fleet rate close to it while there is backlog |
| Identity churn | `igs_identity_retired_total{tier,reason}` | mostly `max_requests` (identities retire after 500 to 750 requests by design) |
| Identity pool | `igs_identities{tier,state}`; `igs_identity_shortfall_total{tier,reason}` | `waiting` 0; shortfalls flat ([Identity pool](#identity-pool)) |
| Page loads per exit | `increase(igs_exit_page_loads_total[1d])` by `exit` | even across exits; under `ig.exit_page_loads_per_day` when set |
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
- A static exit is one IP: its host and port (some providers name their exits by port on one host). A gateway session is its own exit.
- **Exits are named by tier and position, never by address:** the nth endpoint of tier `isp` in `proxies.yaml` is `isp#n` in logs, metrics and Valkey keys, and its breaker bucket is `isp/b<k>` (endpoints in one /24 share a bucket). A gateway session is `<tier>#s<session>`, its bucket `<tier>/gw-<country>`. To find an exit, count its position in `proxies.yaml`; keep the order stable.

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

**Sessions across restarts.** A crawler pod that stops (a rollout, a scale-down) keeps its idle identities in Valkey for `ig.keep_identities` (1 h): their cookies and tokens, never the proxy credentials. The pods that start next take them up exit by exit (`igs_identity_unparked_total`), so a rolling restart does not load a page on every exit again.
- **A pod that dies** (killed, out of memory, its node lost) can't park. So every running crawler also checkpoints every identity it holds, idle or in use, every 2 minutes, under a 5-minute lease.
- Once a dead pod's lease ends, another crawler adopts its last checkpoint within a minute and parks those identities exit by exit (`igs_identity_adopted_total`, and a log line with their age). So at most 6 minutes after a crash, the dead pod's sessions are available again.
- A clean stop parks and drops the checkpoint, so nothing is handed out twice.
- Every page load still waits for its exit's pacing, so even a crash with no checkpoint does not make the IPs all load pages at once.

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

## Identity pool

An identity is made when a task needs one: a fresh browser identity on an exit, bootstrapped by its first page load. There is no fixed pool to fill; what limits it is the exits and each pod's cap (1.3 identities per task slot).

- **`igs_identities{tier,state}`** is each pod's identities: `idle` (ready for the next task), `leased` (in use by a task) and `waiting` (tasks waiting at the pod's cap). Sum over pods for the fleet.
- **`igs_identity_shortfall_total{tier,reason}`** counts the tasks that got no identity:
  - `exits_cooling`: every exit of the tier rests or cools down; the task parks;
  - `no_other_exit`: a failover found no exit but the one it left; the task retries;
  - `at_cap`: the pod was at its cap; the task waited.
- **`IgsIgIdentityPoolLow`** fires when `exits_cooling` or `at_cap` shortfalls go on for 15 minutes.
  - `exits_cooling`: see "What to do when exits keep resting" in [Rotation, retries and failover](#rotation-retries-and-failover); `IgsIgTasksParked` fires as well.
  - `at_cap`: the pod has more tasks in flight than identities allowed. That shouldn't happen with the default cap; check for identities leaking (`leased` growing while `igs_tasks_total` is flat) and restart the pod.

**Page loads per exit.** Each page load (a profile or post page; a fresh identity's first one included) counts in `igs_exit_page_loads_total{tier,exit}`: a static exit by name (`isp#3`), a gateway's sessions by bucket. Instagram flags IPs that load too many pages for fresh visitors (3 of 10 ISP exits in Phase 0), and how many a day an IP tolerates is not yet measured. The daily count per exit is `increase(igs_exit_page_loads_total[1d])`.

- **The daily limit, `ig.exit_page_loads_per_day`** (0, off): once a static exit reaches it in a UTC day, counted fleet-wide in Valkey, it rests until the day ends at 00:00 UTC (03:00 Iraq time), reason `page_load_limit`. Its tasks move to other exits. Pods learn of the rest within `ig.exit_rest_sync`, so the fleet can go a few loads over.
- It stays off until the pilot measures what an IP tolerates (the owner, 6 November 2026). Set it from the pilot's numbers: the most page loads a day on exits that were never walled, with a margin.
- While it rests, the exit also sends no GraphQL request, so a limit set too low cuts throughput: watch `igs_exits_resting` after setting it.

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

**Raw responses are kept** (`ig.capture_raw: all`, architecture §2.5): every GraphQL answer and post page goes to the raw bucket as zstd JSONL, `<prefix><query>/<yyyy/mm/dd/hh>/...`. Every event carries the reference of the response it came from (`raw_ref`: `s3://bucket/key#line`), so a parser fix or a new field can be applied to past data without collecting again. Following D-004, each kind has a prefix of its own:

| Kind | Prefix (igs's bucket / the platform's) | Kept |
|---|---|---|
| Failures: any class but OK, and answers a parser found drifted | `s3.raw_keep_prefix`: `raw-failures/` / `igs/keep/failures/` | for good |
| A sample of the OK answers (`ig.raw_sample`, 1%) | `s3.raw_sample_prefix`: `raw-sample/` / `igs/keep/sample/` | for good |
| The other OK answers (`capture_raw: all`) | `s3.raw_prefix`: `raw/` / `igs/30d/` | until a bucket rule expires them (30 days on the platform's bucket) |

Each line holds `at`, `query`, `doc_id` (GraphQL answers), `parser` and `parser_version` (the version of the parser that read it, 0 if unknown), `class`, `vars`, and `body` (JSON) or `text` (HTML). A re-parse picks the lines a fixed parser affects by `parser` and `parser_version`. With `capture_raw: failures`, failures and the sample are still kept. They take about as much as the proxy traffic before compression, a fifth to a tenth of it after: plan the raw bucket for 0.1 to 0.5 GB a day per 1,000 long-tail profiles, more with hot and warm ones.

Nothing expires by age, the job log included (the owner, 6 November 2026): `jobs_days` and `media_rows_days` are 0. A media row stays after its file is deleted, as the record of what was downloaded. Plan Postgres for about 8,000 jobs a day per 1,000 long-tail profiles (a few KB each).

Media files are deleted once the analysis service acknowledged them (or after `media.ttl`, if set).

An erasure request (`POST /v1/erasure-requests`) still deletes a person's data, including discovery rows, and suppresses them from later crawls.

## Profile discovery

`ig.discovery` (M11; on in the chart's values) grows the registered set without a cap. It adds profiles that are Iraqi, or that address an Iraqi audience, found from the profiles already registered.

1. **Sightings.** Crawlers count the accounts they meet in the posts they read: tagged accounts, co-authors, mentions, and commenters (commenters only with `pii_mode: full`). Erased people are never counted.
   - They also record which registered accounts each one was met in (`ig.discovery_sources`). How many different registered accounts link to an account is its Iraqi audience (`sources`).
   - Each sighting adds to the account's priority: a co-author 3, a tag or a mention 2, a comment 1, a sighting on a post located in Iraq 2 more, and each new registered account it is met in 3.
2. **Vetting.** Every `interval`, the leader scheduler vets the highest-priority accounts at `min_priority` (2) or above, up to `vet_in_flight` (50) at once, [while regular collection keeps up](#discovery-waits-for-regular-collection). A vet is a backfill-lane (p2) job of kind `discovery`:
   - it reads the profile and its newest 12 posts' captions, 2 or 3 requests;
   - it writes them to `ig.discovery_candidates`;
   - it publishes nothing, so accounts that are not registered never reach the analysis.
3. **Score.** The vetter scores what the vet read ([`internal/ig/discovery/score.go`](../../internal/ig/discovery/score.go)). The account's own words count most:

   | Signal | Points |
   |---|---|
   | Iraq named, in any script, or 🇮🇶 | 3 |
   | An Iraqi phone number: +964 or 00964, or 07xx xxx xxxx (the Iraqi grouping) | 3 |
   | Any other 07 mobile number, unspaced or grouped otherwise (07700900123 is also a UK mobile's shape) | 1 |
   | An `.iq` link | 2 |
   | A place in an Iraqi governorate in the name or bio ([the place list](#the-place-list)) | 2; 3 for two or more governorates; 1 if only in captions |
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
     - an Iraqi influencer with Baghdad, 🇮🇶 or an Iraqi-grouped 07xx number in the bio is added at the first link;
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
  - `igs_discovery_candidates{status}`, where `new` counts the accounts due a vet;
  - `igs_discovery_held{reason}`, 1 while vets wait for regular collection (below);
  - `igs_exit_share_requests_total{class="discovery"}` and `igs_exit_share_wait_seconds_total{class="discovery"}`: vets' requests, and the time they waited for discovery's share of their exit (below).
- **Tuning:**
  - Raise `threshold` if registered profiles turn out not to be Iraqi; lower it, or `min_priority`, to find more.
  - Vets share the exits with crawling (about 10 s of one exit's time each) and wait behind scheduled runs (lane p2). They take at most a tenth of each exit's rate (`exit_share`, below). The daily budget holds them with backfill.
  - To slow discovery, lower `vet_in_flight`.
  - **Daily cap:** `max_new_per_day` (0, no cap, by default) is the most profiles discovery registers a day. The day starts at 00:00 UTC, 03:00 Iraq time. Over it, candidates wait as `capped` (`igs_discovery_candidates{status="capped"}`) and are registered on the next days. Profiles registered by hand or with `accept` neither count towards it nor wait for it. Setting the cap back to 0 lets every capped candidate through on the next vetter tick. Production runs without a cap (the owner, 6 November 2026); tests, staging and a canary may set one.

### Discovery waits for regular collection

Discovery ranks below regular collection (the owner, 6 November 2026). Each vetter tick, before starting vets, the leader checks two things:

| Check | Setting (default) | Vets wait while |
|---|---|---|
| The schedule is on time | `ig.discovery.hold_when_behind` (true) | an enabled, unpaused profile is overdue by its own interval (3 h for the long tail, 10 min for hot), or a scheduled run is still queued or running its full interval after it started (its next slot gets skipped) |
| No tier is mostly resting | `ig.discovery.hold_resting_share` (0.5) | more than half of a static proxy tier's exits rest (rate limit, checkpoint or login page), as the crawlers share them in Valkey (`ig.share_exit_rests`). Gateway tiers are left out: their exits are sessions, with no fixed number |

- `igs_discovery_held{reason}` is 1 while vets wait: `schedule_behind`, `exits_resting`, or `rests_unknown` (Valkey did not answer; vets wait rather than guess). The scheduler logs each change.
- Vets already running finish. Deciding, rescoring and registering go on: they read nothing from Instagram.
- While discovery waits, `igs_discovery_candidates{status="new"}` grows. That is expected; once collection keeps up again, the vetter works through the queue, highest priority first.
- If it waits for hours, collection itself is behind: see [Schedule](#schedule) (`IgsIgScheduleLag`) and [Block storm](#block-storm-block-rate--10).
- `hold_when_behind: false` and `hold_resting_share: 0` turn the checks off.

**Discovery's share of each exit** (D-027). Vets send at most `ig.discovery.exit_share` (0.1) of each proxy exit's rate (`ig.exit_rps`), counted across every crawler pod in Valkey (key `exit:<exit>:discovery`, beside the exit's own `exit:<exit>`). A vet's request waits for discovery's share of its exit first, then for the exit like any other request. So with `exit_rps` 0.25, vets send at most one request every 40 s per exit, and scheduled and on-demand work keep the other nine tenths even while discovery is busy. It is a cap, not a reservation: when no vet is running, regular collection uses the whole exit.
- It covers vets only: discovery's sightings come from what regular collection already reads and send nothing. Backfills, also lane p2, are not capped by it.
- `igs_exit_share_wait_seconds_total{class="discovery"}` rising faster than `igs_exit_share_requests_total` means vets queue for their share: discovery is held back by the cap, as intended. To let it go faster, raise `exit_share` (at most 1); `0` removes the cap.
- Discovery's ceiling follows: exits × `exit_rps` × `exit_share` requests a second. With 10 exits at 0.25 and 0.1, that is 0.25 a second, about 21,600 vet requests a day.
- A vet waiting for its share keeps its crawler slot in lane p2, which backfills also use, and its identity. `vet_in_flight` bounds that. Set it to about 2 per exit: production runs 20 for 10 exits. More in flight doesn't vet faster, since the share is the limit; it only holds more slots and identities idle. When exits are added, raise it with them.
- With `exit_rps: 0` (no exit pacing), there is no share either.

### The place list

The places discovery looks for are fbs's Iraq keyword list, shared rather than kept twice (M12i):
- the country, the Kurdistan Region, the 19 governorates and their cities and towns;
- in English, Arabic, Sorani Kurdish and Kurmanji Kurdish;
- folded the same way fbs folds them, so أربيل and اربيل, هەولێر and هه‌ولێر, and Hewlêr and Hewler are one spelling.

The seed is [`configs/discovery/iraq.yaml`](../../configs/discovery/iraq.yaml), copied unchanged from fbs, plus [`iraq-igs.yaml`](../../configs/discovery/iraq-igs.yaml): spellings igs matches that fbs's list lacks, while they wait for fbs. It is empty since fbs took igs's earlier additions (fbs PR #49, 5 November 2026), so both services use one list.
- `igsctl migrate up` loads both into `ig.discovery_keywords`. Loading again adds only new spellings and never undoes an edit.
- Crawlers (posts located in Iraq) and the scheduler (scores) reload the enabled spellings every 5 minutes.
- Spellings that are also everyday words or names are loaded off: Hit, حديثة ("modern"), القائم, Kut, Amara, Zubair, حلة ("outfit"), the surname سليماني. Turn one on if it proves useful.

**How igs matches.** igs matches whole words, where fbs's searches match text anywhere in a post. igs scores names and bios, where a place inside a longer word would score wrongly: عراقة ("heritage") holds عراق. Arabic-script spellings may carry an attached prefix (وبغداد, بالبصرة) or a nisba ending (بغدادي, العراقيين).

Editing the list:
- `GET /v1/discovery/keywords?group=erbil` (admin) lists it. It also filters by `language` and `enabled`.
- `PATCH /v1/discovery/keywords/{id}` with `{"enabled": false}` turns a spelling off. Prefer this to `DELETE`, which a later seed load would undo.
- `POST /v1/discovery/keywords` with `{"term": "...", "language": "ar", "group": "erbil"}` adds a spelling.
- `igsctl discovery keywords list|enable ID|disable ID|import --file F` does the same from the command line. `import` takes a YAML seed or a CSV of `term,language,group,enabled`.

```bash
kubectl -n igs exec deploy/igs-api -- igsctl discovery keywords list --group erbil
```

**Keeping fbs and igs in step:**
- a change to `iraq.yaml` or to the spelling fold (`internal/keywords/normalize.go`, fbs's `internal/discovery/normalize.go`) belongs in both repositories;
- a spelling igs needs first goes in `iraq-igs.yaml`; propose it to fbs as a patch ([`UPSTREAM.md`](../../UPSTREAM.md)), and delete it from `iraq-igs.yaml` once fbs has it.

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
- the components marked `browser: true` (the crawlers, registry-sync) then get a `chromium` container of their own beside them (`browserContainer`, on by default; REQ-008 fix 21). It runs the browser image with its own memory limit, 1 GiB by default, so a heavy page can't take the crawler down with it;
- each render opens a browser context in that Chromium, with the identity's cookies, user agent and exit, and closes it afterwards (`ig.browser.remote`, which the chart sets);
- with `browserContainer.enabled: false`, Chromium starts inside the crawler's container instead, sharing its limit;
- `ig.browser.max_share` and `max_concurrent` bound how much traffic it takes.

- `IgsIgBrowserFallbackSaturated`: more page loads need it than the caps allow; those bootstraps fail and their tasks retry. Raise the caps (and the crawlers' memory) or find what changed in the page.
- `IgsIgBrowserFallbackFailing`: Chromium isn't answering, or the rendered page has no tokens either.
  - Chromium: check the `chromium` container's logs and restarts. Was it `OOMKilled`? Then raise `browserContainer.resources.limits.memory`. The chart runs it without its sandbox, since pods drop every capability.
  - No tokens: that is a new wall. Capture the page on the probe.

## Schema drift

`IgsIgSchemaDrift` fires when responses of a query miss required fields. Instagram changed the shape.

1. The failing responses are in the raw bucket under `s3.raw_keep_prefix` (`raw-failures/<query>/`, or `igs/keep/failures/<query>/` on the platform's bucket), whatever `ig.capture_raw` is, short of `none`.
2. Fix the parser in `internal/ig/parse` against them. Add the scrubbed response as a golden fixture (`internal/ig/fixtures`), then release.
3. `IgsIgOptionalFieldDrift` is the quiet version: an optional field went missing far more often than a week ago. Nothing fails, but the data loses the field.

## No data

`IgsIgNoData` fires when the persister stored no profile, post or comment for 3 hours (one long-tail interval) while profiles are on the schedule. Unchanged rows count, so a quiet account doesn't trigger it.

1. Run `igsctl health` ([Health](#health)): it shows whether the schedule runs, whether jobs finish and when data was last stored.
2. **Jobs aren't created** (`jobs, last hour` shows none): the scheduler has no leader (`IgsIgSchedulerNoLeader`) or can't reach Postgres. Check the scheduler's logs.
3. **Jobs are created but don't finish:** the crawlers aren't running (replicas at 0, waiting for the `proxies` Secret), every exit rests (`IgsIgExitsMostlyResting`), or Kafka refuses igs's topics (`TOPIC_AUTHORIZATION_FAILED` in the logs).
4. **Jobs finish but nothing is stored:** the persister is down or behind ([Kafka lag](#kafka-lag)), or it quarantines everything ([Persister quarantine](#persister-quarantine)).

## Memory near limit

`IgsIgMemoryNearLimit` fires when a container's working set has stayed above 90% of its memory limit for 10 minutes. Past the limit the kernel kills it (`OOMKilled`), losing what it held: a crawler's raw responses waiting for upload, a persister's open batch.

1. Look at the container's memory over the last day. A steady climb that never falls back is a leak: note the component and the time it started, and restart the pod meanwhile. A level that follows load is sizing.
2. For sizing, raise the component's `components.<name>.resources.limits.memory` in the chart values. On the production cluster, check the namespace's quota first.
3. Crawlers with the browser fallback on (`ig.browser.enabled`) use far more: a headless Chromium per pod.

## Raw responses not kept

`IgsIgRawResponsesNotKept` fires when a crawler did not keep raw responses.

1. **Why:** uploads to the raw bucket fail (`igs_raw_upload_failures_total`, with the crawler logging `rawsink: upload failed`). A failed object waits in the pod and is tried again every minute, so its `raw_ref`s still come good once S3 answers. `igs_raw_backlog_bytes` shows what waits.
2. **`reason="backlog"`:** the waiting bytes reached 256 MB. New responses get no `raw_ref`, rather than one that might never be written. Crawling and publishing go on.
3. **`reason="lost"`:** a crawler stopped (a rollout, an eviction) while S3 refused its uploads, and gave up after 20 s. Each object is logged (`rawsink: object never written; its references dangle`), so the `raw_ref`s naming it are known. A crawler killed outright (OOM) loses up to a minute of lines without that log.
4. **What to do:** check the raw bucket and the crawlers' access to it (credentials, the bucket's existence, the S3 endpoint). Nothing needs replaying; the data itself was published.

## Topics and schemas

**On the shared production Kafka and Schema Registry, the platform creates igs's topics and registers its subjects.** igs's chart creates none there (`hooks.topics: false`, `hooks.schemas: false` in `examples/production.yaml`). The `content.*` topics and subjects always belong to the platform.

`igsctl kafka apply` compares `configs/ig/topics.yaml` with a cluster:

- **By default it is a dry run.** It prints each topic's partitions and retention, on the cluster and in the file, and what applying would do. Nothing changes.
- `--apply` creates the topics that don't exist yet. **Existing topics stay exactly as they are**, and the output lists each held change with the flag it needs:
  - `--grow-partitions` adds partitions (it moves keys, so ordering breaks across the change);
  - `--set-retention` changes retention, up or down (lowering it deletes records sooner).
- `content.*` is skipped unless `--include-content`, which is only for a Kafka of igs's own (tests, rehearsals, the fake-Instagram soak).
- It never deletes a topic, removes partitions or changes a replication factor.

To see the difference on a cluster igs may change, from a pod with igs's configuration:

```bash
igsctl kafka apply
```

`igsctl schemas apply` registers the `ig.*` subjects (`<topic>-value`, `BACKWARD_TRANSITIVE`); `schemas check` only checks them. Both leave out the `content.*` subjects unless `--include-content` (a registry of igs's own).

## Start-up

Every binary starts its ops server (:9090) first, then opens its stores.
- `/healthz` answers 200 from the start, so the liveness probe doesn't kill a pod that is still waiting for a busy database.
- `/readyz` answers 503 `starting: opening the stores` until they are open.
- Opening PostgreSQL retries for up to 15 minutes while the server is out of connection slots. The log says so: `pg: the database is busy; trying again` before each wait, and `pg: still waiting for the database` every 10 seconds while one attempt hangs, for example in a pooler's queue.

Once running, `/readyz` checks the stores (PostgreSQL, Valkey, Kafka, as the component uses them) **at most once a minute** and answers from that result in between. Kubernetes probes every 10 seconds, and through the platform's PgBouncer a database touched that often never lets its idle server connections close. So a store that goes down shows as NotReady up to a minute later; the work itself fails and retries at once. `/healthz` never touches a store.

So a pod that is Running but not Ready, logging these lines, waits for the database. Look at the database's connections, not at igs. After 15 minutes it exits with the database's error and Kubernetes starts it again. A rollout waits meanwhile: the old pods keep serving until the new ones are Ready, and Kubernetes may mark it as past its progress deadline (10 minutes) without stopping it.

## Schema changes

The migrate hook upgrades the schema before the new pods start. The old pods keep running on the new schema until they are replaced, and again after a rollback. So a migration only adds: new tables, new columns that are nullable or have a DEFAULT, new indexes, new defaults (REQ-008 fix 23).

`scripts/migration-check.py`, run by CI on every change, refuses:
- an edit, rename or removal of a migration already on main (it has run in production; add a new one instead);
- a new migration numbered before an existing one;
- in a new migration's Up section: DROP TABLE or COLUMN, RENAME, a column type change, SET NOT NULL, a NOT NULL column without a DEFAULT, TRUNCATE (ClickHouse: MODIFY COLUMN).

To remove or rename something, use two releases:
1. Stop using it in the code, and release.
2. Then drop it, in a migration that says so: `-- igs:breaking: <why, and the release that stopped using it>`. The check reports the marked migration instead of refusing it.

## Security checks

The `security` workflow (REQ-008 fix 22) runs on every change, on main and on Sundays at 05:17 Iraq time. `make security` runs the first two locally.

- **secrets (gitleaks)** fails when a commit holds something that looks like a credential.
  - If it is real: rotate it first, then remove it from the history. Never add a real credential to `.gitleaksignore`.
  - If it isn't (a key name, a test fake, a fixture token): add the finding's fingerprint to `.gitleaksignore` with a comment saying why.
- **go-vulns (govulncheck)** fails when igs's code calls a function with a known vulnerability. Update the module (`go get <module>@<fixed version>`, then `go mod tidy`) or Go itself (`go.mod`'s `go` line and the Dockerfile's `golang` tag).
- **images (Trivy)** fails on a HIGH or CRITICAL vulnerability with a fix in either image.
  - In the distroless image it is a Go module or the standard library: update as above.
  - In the browser image it is a Debian package. A rebuild picks up the fix (the browser stage runs `apt-get upgrade`); if Debian's fix is newer than the base image's tag, wait for Dependabot's update to `debian:12-slim`.
  - Only when it can't apply to igs: add its ID to `.trivyignore` with the reason and a date to look again.

Dependabot opens weekly pull requests for the Go modules, the Actions and the base images (`.github/dependabot.yml`). Each runs CI and these checks; merge it when they pass.

## Credentials per component

(REQ-008 fix 22b.) To give a component its own Kafka user or Spaces key:
1. Create the Secret `igs-<component>-credentials` in the namespace igs. It holds only the keys that differ from `platform-credentials`, with the same names: `KAFKA_USER` and `KAFKA_PASSWORD`, `S3_ACCESS_KEY` and `S3_SECRET_KEY`. [igs-operations.md, "Credentials per component"](../master/igs-operations.md#credentials-per-component) lists what each component needs.
2. Set `components.<component>.credentialsSecret: igs-<component>-credentials` in the platform's Argo CD application. The component's pods restart and read it after `platform-credentials`.
3. Check the component's log for Kafka authorization errors (`TOPIC_AUTHORIZATION_FAILED`, `GROUP_AUTHORIZATION_FAILED`) or S3 `AccessDenied` in its first minutes. Then check its work in `igsctl health`.

To undo, unset `credentialsSecret`: the component goes back to the shared credentials.

Once every component that uses the object store has its own key, the shared `S3_ACCESS_KEY` and `S3_SECRET_KEY` can leave `platform-credentials`. igs doesn't need them there: a component opens the store only with a key.

## Network policy

In production igs's pods reach only what `networkPolicy.egress` lists (REQ-008 fix 21; the list is in [igs-operations.md §6](../master/igs-operations.md#6-network)). Something missing from it looks like a store or exit that does not answer: connections time out, nothing is refused.

- **Every request through the exits fails** (`igs_ig_requests_total{class="transport"}` climbs while the other classes stay flat; the crawler logs dial timeouts): the proxy gateway's host or ports aren't in `networkPolicy.egress.proxies`. Compare them with the proxies Secret's endpoints and add the missing ports.
- **A pod can't reach Postgres, Kafka or Valkey** after the platform moved a store: add its address to `networkPolicy.egress.services`.
- **A new component crawls through the exits:** add it to `networkPolicy.egress.proxyComponents`; `make igs-chart-check` fails until the pods that mount the proxies and the pods that may reach them are the same.

To check whether the policy is the cause, set `networkPolicy.egress.enabled: false` for a moment through the platform's Argo CD application and see whether the symptom goes.

## Valkey unavailable

**Alert `IgsIgValkeyUnavailable`:** crawlers have been waiting for Valkey for 2 minutes.

The fleet-wide rate limit (each exit's pace, `ig.exit_rps`, and the query limits) lives in Valkey, and it **fails closed**. While Valkey does not answer, no request goes to Instagram: each one waits, trying again with a growing gap (up to 5 seconds), until Valkey answers or the task's time runs out. A crawler never runs on its own pace alone, because several pods would then use one exit faster than its limit.

1. Check Valkey itself: is it up, and is it reachable from the crawler pods? `igs_ratelimit_store_errors_total{kind}` counts the checks it did not answer.
2. Nothing else is needed: crawling resumes by itself once Valkey answers. Tasks that ran out of time go to the retry topics as usual.
3. Valkey loses nothing that matters: exit rests and parked identities rebuild within minutes ([backups](igs-backups.md)).

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
- **A handler keeps panicking (`IgsIgHandlerPanics`, critical):** a record makes its consumer's code panic. The consumer turns the panic into an error, so the record is retried with backoff and its partition waits behind it: that partition's lag grows while the others move.
  - The log line `kafka: handler panicked` names the group, topic, partition and offset, with the stack. `igs_kafka_handler_panics_total{group}` counts them.
  - Fix the bug and roll out; the record goes through once it no longer panics. (The crawler's own tasks are different: a task that panics is dead-lettered at once, so `IgsIgHandlerPanics` comes from the persister, the normalizer, the media downloader or the eraser.)

**Rebalances.** When Kafka moves a partition to another pod (a rollout, a scale-up), the pod that loses it cancels its records still running there and starts none of those still waiting, and never commits them, so the new owner runs them once (ported from fbs `cfc4295`, `ca4769e`, `907e08a`).

**The leader lease.** One scheduler leads: it holds the lease `igs-scheduler` in table `ig.leases` (holder: pod name and a random suffix).
- It renews the lease every 5 s; each renewal is one statement, bounded to 3 s, and keeps the lease 15 s.
- If two renewals miss (Postgres unreachable, or slow), the leader stops its work, 5 s before another scheduler could take over.
- If the lease went to another holder, the work stops at the next renewal.
- A scheduler that stops gives the lease back.
- `igs-registry-sync` takes the lease `igs-registry-sync` for one run. A crashed run's lease expires after the run's timeout and a minute.

Leases replace session advisory locks: the platform's Postgres is a transaction-mode pool, where a session lock can stay on a pooled server session and go to another client.

To see who leads:

```sql
SELECT * FROM ig.leases;
```

The leader scheduler publishes the consumer groups' lag every 30 s, so no Kafka exporter is needed. The crawler groups are `igs-ig-crawler-<stage>-<lane>`, the persister's is `igs-ig-persister`, the normalizer's `igs-ig-normalizer-c1` and the downloader's `igs-ig-media-downloader`. All of them carry `kafka.topic_prefix`.

## The shared content contract

`igs-normalizer` maps igs's own topics to the shared contract, `github.com/abdullah95k/securebrand-contracts` **v1.2.0** (REQ-019; `docs/master/igs-contract-adoption.md`). The platform's writer reads these topics into its ClickHouse; igs writes no ClickHouse rows itself.

| igs topic | content events |
|---|---|
| `ig.profile.v1` | `content.account.v1` (handle, name, bio with hashtags and mentions, bio links, the website field and the linked Facebook page as links, verified, status) and `content.account_snapshot.v1` (followers, following, posts) |
| `ig.post.v1` | `content.post.v1`: photo, carousel, video, igtv, or reel as `SHORT_VIDEO`. A walled post is a partial event, `RESTRICTED`; an unavailable one `UNAVAILABLE` |
| `ig.post_metrics.v1` | `content.metrics.v1`, target the post |
| `ig.comment.v1` | `content.comment.v1` and `content.metrics.v1` (target the comment). A deletion is a partial `DELETED` event. Redacted comments publish nothing |
| `ig.media.v1`, `ig.highlight.v1` | nothing yet (REQ-019 slices 2 and 3) |

- **On the wire:** plain Protobuf, one message type per topic, headers `sb-type` (the full message name) and `sb-contract` (`1.2.0`). Keys: the account ID (`ig:account:<pk>`) for accounts and snapshots; the post ID (`ig:post:<pk>`) for posts, comments and metrics.
- **The envelope:** `platform` `ig`, `id` `ig:<kind>:<native id>`, `version` = `observed_at` in microseconds, `content_hash` (SHA-256 of the content without counts), `event_id` (SHA-256 of `id|observed_at µs|content_hash`), and provenance (collector, version, parser version `<query>:<n>`, the query and its doc_id, job IDs, logged-out scraping, the crawl mode, `raw_ref`).
- **Metrics volume:** a post's or comment's counts are published when they changed, or once a day while it is still read. The memory is in Valkey (`igs:content:metrics:<id>`, 48 h). If Valkey doesn't answer, the counts are published anyway.
- **Every message passes** the contract's `conformance` check as production runs it, in the unit tests and end to end.
- **Not yet set:** `scope` and `exit_country`. igs's own events don't carry the account's registration or the exit; they come with slice 4.
- **Lag:** `IgsIgNormalizerLag` on group `igs-ig-normalizer-c1`. The chart leaves the alert out while the normalizer is off.
- **Publishing again:** move `igs-ig-normalizer-c1` back (`rpk group seek`) within the `ig.*` topics' 7 days. The same observation gets the same `event_id`.

## Persister quarantine

The persister quarantines what it cannot write on `igs.persist.dlq`, with `x-reason` and `x-error` headers, instead of stalling: undecodable records, or rows Postgres rejects. That data is missing from Postgres until it is fixed and replayed.

1. Read the reason in the logs (`persist: record quarantined`) or in the DLQ headers.
2. Fix the cause (a migration, a parser emitting an invalid value), deploy, then replay:

```bash
igsctl dlq replay --persist
```

The replay puts the records back on their entity topics unchanged. It commits as it goes, so running it again does not repeat records.

## Stuck jobs

- **No leader (`IgsIgSchedulerNoLeader`):** nothing plans runs, finalizes jobs or runs AIMD. Check the scheduler pods and their Postgres connection. Then look at the lease: `SELECT * FROM ig.leases WHERE name = 'igs-scheduler'`. A row whose `until` has passed means no scheduler could renew it.
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

**Walled posts** (REQ-008 fix 15). About 2.6% of post pages show only a login wall to logged-out readers. After three fresh identities see the wall, igs:
- stores and publishes the post from what the grid gave: its ID, shortcode, time, type and pinned flag, with status `unavailable` and `status_reason` `login_wall`;
- leaves its counts empty, never 0. A walled reel still gets the views, likes and comment count the reels tab serves;
- reads none of its comments (`comments_state` `none`), and publishes no media and no metrics sample (an unavailable post's page gave no counts);
- counts it in the job's `posts_gated`, and `posts_walled_published` when it was published. A post already stored keeps what was read before.

Walled posts get no refreshes. If a later full run reads the page, the post becomes `visible`. A post that is gone (deleted, archived or made private) has `status_reason` `not_found`. On `content.post.v1`, a walled post isn't `is_deleted`: its extras say `"status": "unavailable", "status_reason": "login_wall"`.

**Per-post comment coverage** (REQ-008 fix 15). When a job ends, each visible post it read gets `posts.comments_state` (in the API as `comments_state`):
- `complete`: the read went from the first comment page to the last, or the post has no comments;
- `truncated`: a login wall stopped the read;
- `partial`: the job's comment cap (`max_comments_per_post`) stopped it, or it didn't finish (a failed task, a cancelled job);
- `none`: never read (walled posts, and posts read before this fix);
- a scheduled refresh reads only the comments since the last read, so it keeps the last state (`none` becomes `partial`); a skipped post (unchanged, or comments not asked for) keeps its state too.

`posts.comments_collected` is the post's visible top-level comments stored. The persister keeps it current as comments arrive or are marked deleted. Logged out, Instagram serves no replies, while its comment count includes them: compare the two only as information. The job's completeness gains `posts_capped`.

## Schedule

The planner runs each scheduled profile at its tier's interval, and its profile query every `ig.schedule.profile_refresh` (24 h). The post refresh re-reads the comments and counts of posts from the last `ig.schedule.post_refresh_window` (7 days), `post_refresh_per_run` posts at a time; each post's refreshes start at its profile's interval and double.

**Deletions** (REQ-008 fix 14). igs marks what disappears and never removes what it stored:
- A **comment** is marked `deleted` (with `deleted_at`; its text and history stay) when a complete read of its post no longer returns it. A read counts only if it went from the first page to the last and the comments it returned, each with its reply count, add up to the post's comment count from the same read (`scheduler.post_refresh.deletion_count_slack`, 0: exactly). Otherwise Instagram may have answered partly and nothing is inferred (`igs_deletion_checks_total{outcome="skipped_partial"}`).
- Such reads happen when a scheduled refresh finds **fewer comments than last time** (it then reads them all), on full runs of known posts, and on each post's **one full re-read at 30 days** (`ig.schedule.post_recheck_after`, at most `post_recheck_per_run` (10) a run, for a week after that age; `posts.rechecked_at`).
- The re-read also marks a **post** whose page is gone `unavailable`.
- The job's end publishes each deleted comment on `ig.comment.v1` (status deleted, no text) and counts them (`comments_deleted`). A comment seen again later becomes visible again.
- `scheduler.post_refresh.detect_deletions: false` turns the check off.

**The reels tab** gives each reel's views (and its likes and comment count). A scheduled run reads it back `ig.schedule.reels_window` (7 days). Once a profile's last full pass is `ig.schedule.reels_full_every` (7 days) old, its next scheduled run reads the tab back to `limits.max_age_days` (a year) instead, so older reels' views are refreshed weekly. A profile's first read (its backfill) and any full run always read it all.
- Each profile's last full pass is `last_full_at` in `crawl_cursors` (scope `profile_reels`); the last windowed pass is `last_incremental_at`. A job's `reels_full_passes` counter counts its full passes.
- To read every profile's tab in full on its next run (for example after an outage longer than a week), clear the full-pass times in the igs database:

  ```sql
  UPDATE ig.crawl_cursors SET last_full_at = NULL WHERE scope_type = 'profile_reels';
  ```

- `reels_window: 0` reads the tab back to `max_age_days` on every run (the behaviour before 6 November 2026): about 20 more requests per visit for a profile with 1,000 reels a year.

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

**Kafka.** `configs/ig/topics.yaml` holds the topics as the platform creates them in production, sized for 10,000 profiles: 99 partitions, replication 3 (REQ-008 fix 20). Crawl tasks keep 24 h, media tasks 72 h, retries 48 h, dead letters 14 days, `ig.*` 7 days and erasure 30 days.
- Each topic's partitions bound the pods that consume it, and the chart's KEDA maxima follow: crawler-profile 6 (`igs.task.profile.p1`), crawler-post 12 (`igs.task.post.p1`), persister and normalizer 12 (`ig.comment.v1`), media-downloader 3. `TestKEDAMaximaFitTopics` checks every values file against the topics.
- At 10,000 profiles the exits, not the pods, set the pace (above). To grow past that, raise the partitions first: the platform changes them in `kafka.tf` (they can only grow), then raise the KEDA maxima to match.

**Valkey.** Most keys are per task:
- **Done marker:** 88 B, kept `stage.DoneTTL` (96 h, longer than the task topics keep a record: 24 h for crawl tasks, 72 h for media).
- **Checkpoint:** 344 B, deleted when its task finishes, so only in-flight segments hold one.
- **Job keys:** kept 24 h after the job finishes.

A scheduled run is one profile task plus up to `ig.schedule.post_refresh_per_run` (30) post tasks, typically 5 to 9. At 300k profiles every 3 hours (2.4M runs a day, about 6 tasks each), the done markers alone are about 58M keys, around 5 GB: plan Valkey's memory for that. The soak (`make igs-soak`) prints the key count every 30 s.

## Erasure requests

`POST /v1/erasure-requests` records a request; the scheduler's eraser carries it out within seconds:
- it deletes the subject's rows and media;
- it adds the subject to the suppression list, so later crawls drop or redact them (`igs_persist_suppressed_total`);
- it publishes `ig.erasure.v1`.

`GET /v1/erasure-requests/{id}` shows the progress.
