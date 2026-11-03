# igs go-live runbook

This takes igs from a release to production traffic: release, infrastructure, secrets, install, a canary on real profiles, and the ramp. Day-2 operations are in [`igs-operations.md`](igs-operations.md).

Legal signed off on 2026-11-03 (C4). igs reads only what Instagram shows logged out.

## 1. Size the exits first

Instagram limits each IP, so the number of ISP exits sets how many profiles igs can keep fresh. Each exit sends at most `ig.exit_rps` requests: 0.25 a second, 900 an hour.

| Work | Requests | One exit's time |
|---|---|---|
| A new profile's first read (a year back; Phase 0's average) | 37.6 | about 2.5 min, once |
| A scheduled run every 3 hours (new posts, the daily profile read, comment refreshes) | 5 to 10 | 20 to 40 s |

So:
- one exit keeps about **270 to 540 profiles** fresh on the 3-hour schedule;
- 10 exits keep 2,700 to 5,400 profiles;
- 100 exits keep 27,000 to 54,000;
- every 1,000 new profiles cost about 42 exit-hours of first reads, spread over the first days.

Buy exits for where you want to be in a month. Phase 0 found 3 of 10 exits flagged for page loads, so expect some to be unusable. igs rests flagged exits by itself, but they add no capacity. Replace them.

## 2. Infrastructure

| Component | Needs |
|---|---|
| Kubernetes | 1.30 (what `make chart-check` validates against), with [KEDA](https://keda.sh) (crawlers and the persister scale on Kafka lag); the Prometheus Operator and Grafana sidecar for alerts and the dashboard |
| Postgres 18 | a database `igs` (schema `ig` is created by the install hook); daily partitions and partition-drop retention run from `igs-scheduler` |
| Kafka | 3 brokers; topics are created by the install hook (`configs/ig/topics.yaml`, sized for 300k profiles; `hooks.topicPartitionsCap` lowers them) |
| Schema registry | optional (`config.kafka.schema_registry.url`, `hooks.schemas: true`) |
| Valkey (or Redis) 7+ | job accounting, leases, checkpoints, rate limits. Plan about 5 GB at 300k profiles ([sizing](igs-operations.md#capacity-and-sizing)) |
| S3 | two buckets: media and raw captures. Media files stay off until the analysis service exists (D5) |
| ISP proxies | the exits from step 1 |

## 3. Release

A version tag publishes the images and the chart (`.github/workflows/release.yml`):

```bash
git tag v1.0.0
```

```bash
git push origin v1.0.0
```

The workflow's summary lists what it published:
- `ghcr.io/abdullah95k/igs:1.0.0`;
- `ghcr.io/abdullah95k/igs:1.0.0-browser`, with Chromium, for the browser fallback;
- the chart `oci://ghcr.io/abdullah95k/charts/igs`, version `1.0.0`.

GHCR packages start private. Either make both packages public in their package settings, or give the cluster a pull secret (`imagePullSecrets`).

## 4. Secrets

Create them in the release namespace (`igs` below).

1. **`igs-env`**, the environment of every pod:
   - `IGS_POSTGRES__DSN`: `postgres://USER:PASS@HOST:5432/igs?sslmode=require`;
   - `IGS_REDIS__PASSWORD`;
   - `IGS_S3__ACCESS_KEY` and `IGS_S3__SECRET_KEY`;
   - `IGS_IG__PRIVACY__AUTHOR_HMAC_KEY`: 32 or more random bytes, never reused elsewhere. Changing it later changes every commenter's key.
2. **`igs-proxies`**, key `proxies.yaml`: the exits, in the format of [`phase0/proxies.example.yaml`](../../phase0/proxies.example.yaml), tier name `isp`. This can be the Phase 0 file.
3. **`igs-keda-kafka`**: only if the brokers need SASL or TLS (see `keda.authSecret` in the chart's values).

Generate the HMAC key:

```bash
openssl rand -base64 48
```

Create the proxies Secret from the Phase 0 file:

```bash
kubectl -n igs create secret generic igs-proxies --from-file=proxies.yaml=phase0/proxies.yaml
```

## 5. Values

Copy [`deploy/helm/igs/examples/production.yaml`](../../deploy/helm/igs/examples/production.yaml) and replace every `<...>`. The settings that matter most:

- **`config.ig.registry_sync.canary_pages`:** three or more public profiles that post often. igs-registry-sync follows Instagram's doc_id rotations on them.
- **`config.ig.privacy.pii_mode`:**
  - `full` stores commenters' usernames and names (behind the `pii:read` scope);
  - `pseudonymized` stores keyed hashes instead.
- **`config.ig.budget.bytes_per_day`:** set it if the proxy plan bills per GB ([Daily budget](igs-operations.md#daily-budget)).
- **`components.crawler-*.autoscale.max`:** exits bound the crawl, not pods. The example's maxima suit up to about 200 exits.

## 6. Install

```bash
helm upgrade --install igs oci://ghcr.io/abdullah95k/charts/igs --version 1.0.0 -n igs -f production.yaml
```

The pre-install hooks:
1. migrate schema `ig`, seed the query registry and create the partitions;
2. create the topics;
3. with `hooks.schemas`, register the schemas.

A failing hook stops the install; its Job's log says why.

Check that every pod is ready:

```bash
kubectl -n igs get pods
```

## 7. API key and first profiles

Create a key for the client that registers profiles and reads data:

```bash
kubectl -n igs exec deploy/igs-api -- igsctl apikey create --name securebrand --scopes profiles:write,jobs:write,data:read
```

Keep the printed `api_key`; it is shown once. Add `pii:read` only for clients that may see commenters' identities.

Register a profile. Its first read (backfill) starts at once:

```bash
curl -H "Authorization: Bearer $KEY" -d '{"profile":"<username>"}' https://<igs.example.com>/v1/profiles
```

## 8. Canary: 50 to 100 profiles for a day

Register 50 to 100 profiles, then watch the **igs / Instagram overview** dashboard and these signals:

| Signal | Healthy |
|---|---|
| Backfill jobs (`GET /v1/jobs`) | `completed`; some `partial` is coverage (a wall at a grid depth), not failure |
| Block rate (`igs:ig_block_ratio:rate5m`) | under 5% |
| Exits resting (`igs_exits_resting` / `igs_exits`) | a few at a time, not most |
| `igs_exit_rests_total` by reason | mostly `rate_limited`, rare; `checkpoint` or `page_wall` points to flagged exits to replace |
| Parked tasks, DLQ | 0 |
| Schedule lag (`igs:ig_leader:schedule_lag_seconds`) | well under 3 h |
| Data (`GET /v1/profiles/<username>/posts`) | posts, counts and comments as on Instagram |

Then:
1. Compare a few profiles by hand with Instagram in a logged-out browser.
2. Tune:
   - Rate-limit rests every few minutes per exit mean `ig.exit_rps` is above what Instagram tolerates: lower it to 0.2.
   - No rests at all for a day: try 0.3.

## 9. Ramp

Grow in steps while the canary's signals stay healthy:

1. Each day, add at most about **twice the profiles already registered**, and no more than the exits can backfill that day: 1,000 profiles need about 42 exit-hours.
2. Keep the schedule lag under 3 hours. When it grows while few exits rest, add exits before adding profiles.
3. Add exits by updating `igs-proxies`, then restart the crawlers:

```bash
kubectl -n igs rollout restart deploy/igs-crawler-profile deploy/igs-crawler-post deploy/igs-registry-sync
```

Profile discovery adds Iraqi profiles without a cap, from the profiles you register ([how](igs-operations.md#profile-discovery)). It is on in the chart's values. Each discovered profile needs exits like any other, so watch the same signals. To slow it, lower `config.ig.discovery.vet_in_flight` or raise `threshold`; to stop it, set `enabled: false`. Check its first registrations by hand:

```bash
curl -H "Authorization: Bearer $ADMIN_KEY" "https://<igs.example.com>/v1/discovery/candidates?status=registered"
```

## 10. Pause, roll back

To stop reading Instagram at once while keeping everything else up, scale the crawlers to zero. KEDA's minimum would scale them back up, so first pause the ScaledObjects:

```bash
kubectl -n igs annotate scaledobject --all autoscaling.keda.sh/paused-replicas=0 --overwrite
```

Resume:

```bash
kubectl -n igs annotate scaledobject --all autoscaling.keda.sh/paused-replicas-
```

Roll back a release:

```bash
helm -n igs rollback igs
```

igs's migrations only add tables, columns and partitions, so the previous release runs on the newer schema. A release that ever needs a destructive migration says so in its notes.

## 11. After go-live

- [`igs-operations.md`](igs-operations.md): alerts, signals, rotation and failover, budget, sizing.
- [`igs-registry-sync.md`](igs-registry-sync.md): doc_id rotations.
- Releases: tag, then the same `helm upgrade` with the new `--version`.
