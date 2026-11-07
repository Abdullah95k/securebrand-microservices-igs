# igs pilot: 48 hours on real profiles

REQ-008 fix 13 (D-036: the owner started it on 7 Nov 2026, 12:45 Iraq time). igs runs in production on the owner's Phase 0 accounts for 48 hours. The pilot measures what the review could only estimate: what an exit tolerates, data per account, memory, database growth and connections. The report goes in `docs/master/igs-pilot-report.md`, as counts only.

Times are Iraq time. Account lists stay off git: no usernames, names or comment text in the repository.

## 1. Before it starts

All of these must hold:

| | Check | Who |
|---|---|---|
| 1 | ~~The soak passed~~. **Dropped by the owner's D-045** (7 Nov): "dont test on fake instagram or fake tiktok or fake facebook or fake youtube, test on the real thing". The pilot on the real site is the test; the soak against fakeig gates nothing | done |
| 2 | igs's proxies exist: 10 ISP IPs plus 3 spares, in the Secret `proxies` (key `proxies.yaml`, tier `isp`, a `static_list` of endpoints; `phase0/proxies.example.yaml` shows the shape) | the owner buys them; the platform creates the Secret |
| 3 | The proxies' ports are in `networkPolicy.egress.proxies` (production allows 10000–10999). Ports outside that range need the range changed first: a blocked port only shows as timeouts ([network policy](igs-operations.md#network-policy)) | the platform, when it writes the Secret |
| 4 | Three public accounts that post often, as `config.ig.registry_sync.canary_pages` in the platform's private Argo CD application. Without them registry-sync refuses to run, so it can't follow doc_id rotations. **Done:** the owner chose them (D-033), and the platform set them on 7 Nov. At least one must have highlights, or registry-sync can't check the highlights query and records no successful run ([igs-registry-sync.md](igs-registry-sync.md)) | done |
| 5 | PostgreSQL has room. **Done:** `max_connections` is 200 since 7 Nov, 11:32Z (D-037), up from 50; the pilot adds about 6 to 9 server connections (section 5) | done |

The owner keeps two files on their own machine, one username or profile link per line, both cut from their list of 348 Phase 0 accounts:
- `pilot-first-50.txt`: 50 of them, mixed rather than the 50 most active, so the first two hours show ordinary accounts as well as busy ones;
- `pilot-rest.txt`: the other 298.

The repository holds no usernames.

## 2. What the platform changes

In the platform's Argo CD application for igs (`gitops/services/igs.yaml`), in one change, once checks 2 to 4 hold:

- **Crawlers on, at fixed sizes.** Replace the four holds at 0 with:
  - crawler-profile 1;
  - crawler-post 2;
  - crawler-retry 1;
  - registry-sync 1.

  Keep `keda.enabled: false` for the pilot, so the 48 hours run on the same pods from start to end and their memory, CPU and connections compare cleanly. Each crawler pod runs up to 240 tasks at once, so the 10 exits set the pace, not the pod count. KEDA comes back after the pilot; with PostgreSQL at 200 connections (D-037), connections no longer hold it back.
- **Restart the scheduler** so it reads the new proxies file (discovery's hold counts the exits): `components.scheduler.podAnnotations: {igs/proxies-added: "<date>"}`.
- **Nothing else.** These stay as they are:
  - the pace: `exit_rps` 0.25, `identity_rps` 0.33;
  - `exit_page_loads_per_day: 0` (off; the pilot measures page loads per exit);
  - discovery: on, with no daily cap, as the owner decided on 6 Nov.

  Discovery can't crowd out the registered accounts. It vets only while the schedule is on time and no tier is mostly resting (fix 6), and keyword discovery uses at most 10% of each exit (D-027). The report counts discovered accounts apart.

## 3. Commands for the owner

**Start, step 1: the first 50 accounts.** Then watch the signals in section 4 for 2 hours: backfills start, the block rate stays under 5%, and exits rest only now and then.

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles import --file - < pilot-first-50.txt
```

**Start, step 2: the other 298.** Run this only if the first 2 hours were healthy. The 48 hours start now.

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles import --file - < pilot-rest.txt
```

**Check.** Run this each morning and evening, and whenever something looks wrong. It prints, in Iraq time:
- profiles on the schedule;
- how late the schedule is;
- the last hour's jobs;
- when data was last stored;
- the exits resting;
- the consumers' lag.

It exits with 1 when something is BAD, and says what to look at.

```bash
kubectl -n igs exec deploy/igs-crawler-profile -- igsctl health
```

Run it in a crawler pod: the api pod doesn't mount the proxies, so it can't show each tier's exits.

**Stop.** This pauses every pilot account's schedule. Their data stays, and no new visit starts. Tasks already queued still run, a backfill's post pages included: to stop the reading itself, see "Stop all reading at once" below.

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles pause --file - < pilot-first-50.txt
```

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles pause --file - < pilot-rest.txt
```

**Resume.** The same two files:

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles resume --file - < pilot-first-50.txt
```

```bash
kubectl -n igs exec -i deploy/igs-api -- igsctl profiles resume --file - < pilot-rest.txt
```

**Stop all reading at once**, for example when most exits rest: the platform sets crawler-profile, crawler-post and crawler-retry to 0 in the Argo CD application. Queued tasks wait in Kafka. The task topics keep 24 hours, so set them back within about 20 hours, or the queued backfill expires: its jobs end partial, and later visits read the posts again. Leave registry-sync at 1: it loads 3 canary pages every 30 minutes.

On 7 Nov this was needed two hours in: the backfill's post pages walled 9 of the 10 exits (section 4, "Why exits rest").

## 4. What to watch

| Signal | Healthy | Where |
|---|---|---|
| Exits resting | **at most half** of a tier (igsctl warns above that). The first signal of a wall: resting exits send nothing, so the block rate can stay low while most exits rest | `igs_exits_resting` / `igs_exits`, `igsctl health` in a crawler pod |
| Data stored per hour | not down by more than half from the hour before, while there is work queued | `igs_persist_rows_total` by entity |
| Block rate | under 5%, read with exits resting: it covers only the exits still sending | `igs:ig_block_ratio:rate5m` (0 while nothing is blocked; before #108 the rule gave no series then) |
| Exits resting, by tier | as above (`igs_exits_resting` has no series until an exit first rests: read it as 0) | `igs_exits_resting` / `igs_exits` |
| Why exits rest | mostly `rate_limited`. `checkpoint` or `page_wall` on one exit means replace it with a spare. `page_wall` on most exits means Instagram is walling page loads from them: stop all reading (section 3) and lower the pace | `igs_exit_rests_total` by reason |
| Schedule lag | well under 3 hours | `igs:ig_leader:schedule_lag_seconds` |
| Parked tasks, dead letters | near 0 | `igs_dlq_total`, `igsctl health` |
| Data stored | grows every hour | `igs_persist_rows_total` by entity |
| Memory | no pod above 90% of its limit, no OOM kills | `container_memory_working_set_bytes`, alert `IgsIgMemoryNearLimit` |
| PostgreSQL connections | igs at 40 or fewer server connections (about 26 seen while 50 accounts backfilled); the server not full | the platform's pg-pool view |
| Registry | every query at 1, none `failing`; a run that checked every query within the hour, once a canary has highlights | `igs_registry_query_healthy`, registry-sync's `run finished` log line |

## 5. Connections and pods it adds

- **Pods:** 5 more: crawler-profile 1, crawler-post 2, crawler-retry 1, registry-sync 1. They request 1.7 vCPU and 1.9 GiB in all; the cluster now grows to 8 nodes (platform PR #37).
- **Server connections:** igs holds about 3 while idle.
  - Ten pods open PostgreSQL: api, scheduler, persister, both media pods, crawler-profile, both crawler-post pods, crawler-retry and registry-sync. Each keeps a client pool of at most 4 (`max_conns: 4`), so igs never holds more than 40 server connections: PgBouncer in transaction mode holds no more server connections than clients.
  - Collecting, most pods touch the database every few seconds, and PgBouncer runs several processes, each with its own pools. On 7 Nov, with 50 accounts backfilling, igs held 26 (the first estimate here, 8 to 12, was too low).
- **Room:** with `max_connections` at 200 (D-037) this fits easily. If the server ever nears full, lower crawler-post to 1. The exits, not the pods, set the pace, so collection barely slows.

## 6. Tests during the run

- **A pod restart**, about hour 12: delete one crawler-post pod. Its tasks go back to Kafka and finish on the other pod; nothing is lost or stored twice.
- **An hour with no crawling**, about hour 24: the platform holds the crawlers at 0 for one hour, then restores them. The schedule falls behind and catches up. The report gives the lag at its worst and how long the catch-up took.

## 7. The report (counts only)

The build session writes `docs/master/igs-pilot-report.md` from what the master sends. Times are in Iraq time.

| Measure | How |
|---|---|
| Posts and comments per hour | `sum by (entity) (increase(igs_persist_rows_total[1h]))` |
| Blocked requests and resting exits over time | `igs:ig_block_ratio:rate5m`, `igs_exits_resting`, `sum by (reason) (increase(igs_exit_rests_total[1h]))` |
| Page loads per exit per day | `sum by (exit) (increase(igs_exit_page_loads_total[1d]))` |
| Proxy data per account per day | `sum(increase(igs_proxy_bytes_total[1d]))` divided by the profiles on the schedule (`igsctl health`) |
| Peak memory and CPU per pod | `max_over_time(container_memory_working_set_bytes{namespace="igs"}[48h])`, `rate(container_cpu_usage_seconds_total{namespace="igs"}[5m])` |
| PostgreSQL growth per day | schema `ig`'s size at the start and every 24 hours: `SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'ig' AND c.relkind = 'r'` |
| Server connections | the platform's pg-pool view, at idle and while collecting |
| Dead letters and stuck jobs | `igs_dlq_total`; `igsctl health`'s jobs line |
| Discovered accounts | the count registered by discovery, apart from the 348 |
| Completeness | 10 accounts checked by hand in a logged-out browser against `GET /v1/profiles/<username>/posts` |
| Cost per 1,000 accounts | proxies from the data per account; compute from the pods' requests |

**Pass:**
- fewer than 5% of requests blocked;
- no OOM kills;
- no account waiting more than 3 hours past its due time;
- dead letters near 0;
- the 10 manual checks matching.

Then the go-live ramp follows ([igs-go-live.md](igs-go-live.md) §9), with the measured numbers in place of the estimates.
