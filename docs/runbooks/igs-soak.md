# igs soak (24 hours against fakeig)

**Not a gate since D-045** (7 Nov 2026): the owner decided to test on the real site, not on fake ones, so the pilot on Instagram is the test and this soak runs no more in production. The files stay, for running it locally or in CI against the code. The one production run (pinned to `60efca8`) ended void: fakeig deadlocked a few hours in (fixed in #107).

REQ-008 fix 12. igs runs for 24 hours on the production cluster, in its own namespace `igs-soak`, against **fakeig** (a synthetic logged-out Instagram) and **throwaway stores**. Nothing reaches Instagram, the platform's stores or the internet. It shows whether igs holds up for a day: no memory creep, no out-of-memory kills, flat goroutines, jobs that end well, and a store that ends equal to the truth.

## What runs

Two Argo CD applications, at one igs commit (the platform builds igs's image at that commit, default target of `deploy/docker/Dockerfile`):

| Application | Source | Holds |
|---|---|---|
| `igs-soak` stores and driver | `deploy/soak` (kustomize; image `igs` set to the build) | Postgres, Redpanda, Valkey, an in-memory S3 (`devs3`), fakeig with 500 profiles, and the `igs-soak` Job with its service account and role |
| igs | `deploy/helm/igs`, values `deploy/soak/values-soak.yaml`, **release name `igs`** | Every igs component, one replica each (two post crawlers), no KEDA |

- **fakeig** (`fakeig -admin`) serves 500 generated profiles of 40 posts each. Every 37th GraphQL request answers 429. Its `/__fakeig/` paths give the driver the profiles, world changes and the ground truth.
- **The `igs-soak` Job** (`cmd/igs-soak`):
  1. waits for igs's schema, its API and fakeig (up to 30 minutes);
  2. registers the 500 profiles through igs's API;
  3. for 23 hours:
     - keeps every profile on a 30-minute schedule;
     - changes fakeig's world every 2 seconds (new posts, comments on recent posts, more likes);
     - kills a crawler pod every 2 hours;
     - restarts Valkey once, at 12 hours (its data is lost);
  4. stops the schedules and waits for running jobs (up to 30 minutes);
  5. reads every profile in full through the API (up to 2 hours);
  6. compares igs's store with fakeig's truth: every post with its counts, every top-level comment with its text and reply count.

  It runs once (`backoffLimit: 0`) and stops after 27 hours at the latest.
- **Resources:** every container has requests and limits. Running pods ask for 3.05 CPU and 5.1 Gi, and are limited to 5.5 CPU and 10.6 Gi. Each hook job (migrate, topics) is limited to 0.5 CPU and 256 Mi. Two volumes: Postgres 10 Gi and Redpanda 10 Gi.
- **The namespace's network policy** must allow traffic inside the namespace, DNS, and, for the Job's chaos, **the Kubernetes API**. Without the API, the Job can't kill pods or restart Valkey: each attempt is recorded as "chaos failed", and the soak fails.
- **Discovery is off**, so the store holds only fakeig's profiles. No secret is involved: Postgres trusts connections inside the namespace, and the S3 keys and the authors' key are igs's development defaults (`service.env: soak`).

## Watch it

- **The Job's log** (Loki):
  - a line every minute with the jobs by status, the number running, the oldest, and the world changes so far;
  - a line for each chaos event.
- **igs's pods** expose port 9090 (named `metrics`); the chart's PodMonitor has the platform's Prometheus scrape them every 30 s.

## The verdict

**The report** is the Job's last log lines. It ends `igs soak: PASS` or `igs soak: FAIL`, and the Job exits 0 on a pass, 1 on a fail, and 2 if it couldn't run. It gives:
- the world changes and chaos events;
- the jobs by status, and the longest any job ran;
- the posts and comments stored;
- the differences from fakeig's truth (the first 20);
- any job that ended failed, partial or cancelled, and any chaos that failed;
- whether the harness held: how many of the driver's world changes fakeig left unanswered, and whether fakeig restarted.

**When the harness fails, the soak fails, and its report says so** ("the harness failed, not necessarily igs; run the soak again"):
- **fakeig left more than 1% of the world changes unanswered.** Its world then mostly stood still, so the soak didn't test what it should.
- **fakeig restarted.** fakeig keeps its world in memory and generates it again from the seed at each start, so its truth is no longer the world igs read. The driver compares when fakeig says it started (`/__fakeig/instance`) at the start and at the end.

Either way, the comparison says nothing about igs. Fix the cause and run the soak again. igs's own pods can still be judged from Prometheus, below.

**From Prometheus,** over the 24 hours, for igs's pods (`app.kubernetes.io/name="igs"`; the stores grow with their data, as they should):

| Criterion | Query | Pass |
|---|---|---|
| No memory creep | `max by (pod) (container_memory_working_set_bytes{namespace="igs-soak", container!="", pod=~"igs-.*"})` | After the first hour, each pod's working set levels off. A crawler pod killed by the chaos starts again from low; judge its replacement the same way |
| No out-of-memory kill | `increase(kube_pod_container_status_restarts_total{namespace="igs-soak"}[24h])` and `kube_pod_container_status_last_terminated_reason{namespace="igs-soak", reason="OOMKilled"}` | 0 restarts, no OOMKilled (deleted pods are replaced, not restarted) |
| Goroutines flat | `go_goroutines{namespace="igs-soak"}` | Level after the first hour, per pod |

The soak passes when the report says PASS and the three criteria hold.

## Rerun or clean up

- **To rerun:** delete the Job `igs-soak`, Postgres's volume and Redpanda's volume, so the stores start empty; then sync both applications. fakeig's world is generated again from its seed at each start.
- **To remove the soak:** delete both applications. Then delete the two volumes (`data-postgres-0`, `data-redpanda-0`): a StatefulSet's volumes outlive it.

## Run it locally (a minute instead of a day)

`TestSoakDriver` (`internal/ig/e2e`) runs the same driver for 40 seconds against the in-process stack, with a crawler restart and Valkey losing its data:
1. Run `make infra-up` to start the local Postgres, Valkey and Kafka.
2. Then run:

```bash
go test ./internal/ig/e2e/ -run TestSoakDriver -count=1 -v
```

`make igs-soak` is the longer local soak (`TestSoak`, 20 minutes by default).
