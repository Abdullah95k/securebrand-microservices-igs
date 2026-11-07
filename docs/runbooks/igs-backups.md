# igs backups and restore

What igs keeps, how each part is backed up, how to restore it, and the drill that checks a restore works. Times are Iraq time (UTC+3) unless they say UTC.

## What to back up

| Store | What it holds | Backed up? |
|---|---|---|
| Postgres, schema `ig` | The system of record: profiles, posts (including highlight items), comments, edit history, metrics, schedule and tiers, the query registry, discovery candidates, erasure records | **Yes.** Continuous backups with point-in-time restore, plus a daily dump of schema `ig` |
| ClickHouse, the content database | The platform's shared content store, filled by the platform's writer from `content.*` | **The platform's to back up.** igs writes none of it (see below) |
| Object storage: the raw bucket and the media bucket | Every raw response (kept forever), and media until the analysis acknowledges it | **Yes.** Versioning on, replicated to a second bucket |
| Secrets | Proxy credentials, API keys, S3 and database passwords, `ig.privacy.author_hmac_key` | **Yes.** In the secret store, with an offline copy kept by the owner |
| Kafka | Records in transit, kept 7 days (`ig.*`, `content.*`) or 3 days (tasks) | **No.** It is a pipe. Its retention is what bridges a restore (see below) |
| Valkey | Exit rests, parked identities, rate limits, job progress | **No.** It rebuilds itself within minutes. Losing it costs fresh identities and may repeat a few running jobs |

**The author key matters most.** `ig.privacy.author_hmac_key` turns each commenter into a stable key. If it is lost, the same person gets a new key from then on, and their old comments no longer link to their new ones. Keep it in two places.

**When the Postgres server is shared** with fbs, yts and tks, each service backs up its own schema. A schema dump restores igs without touching the others.

## Postgres

### Continuous backups (point in time)

- **Managed Postgres** (recommended): turn on its daily backups and point-in-time recovery. On DigitalOcean this is built in, with 7 days of recovery. There is nothing to run.
- **A server you run yourself:** use pgBackRest or WAL-G. Archive the write-ahead log to object storage. Take a full backup weekly and a differential daily, and keep 4 weeks.

### The daily dump of schema `ig`

A dump is portable: it restores into any Postgres of the same or a newer version, including a database shared with other services. It also survives mistakes in the backup tooling. Run it daily at 03:00 (00:00 UTC) as a cron job or a Kubernetes CronJob, and upload the file to the backups bucket. Keep 30 daily dumps and 12 monthly ones.

```bash
pg_dump --format=custom --schema=ig --no-owner --no-privileges --dbname="$IGS_PG_DSN" --file="ig-$(date -u +%F).dump"
```

`pg_dump` must be the same major version as the server, or newer. Use the server's own image if the machine's is older:

```bash
docker run --rm -i --network host postgres:18-alpine pg_dump --format=custom --schema=ig --no-owner --no-privileges --dbname="$IGS_PG_DSN" > "ig-$(date -u +%F).dump"
```

### Restore

1. **Stop everything that writes to schema `ig`:** the crawler, persister, scheduler, API, discovery, media and eraser. The chart's `paused` switch holds every component at 0 replicas, pauses KEDA (which would otherwise scale them straight back up) and skips the install hooks, so no migration runs against a half-restored database:

   ```bash
   helm upgrade igs deploy/helm/igs -n igs --reuse-values --set paused=true
   ```

2. **Restore into an empty database,** or into the shared one after dropping its schema `ig`:
   - **Point in time (managed):** restore or fork the cluster to the chosen time, then point `IGS_PG_DSN` at it.
   - **From a dump:**

     ```bash
     pg_restore --exit-on-error --no-owner --no-privileges --dbname="$IGS_PG_DSN" ig-2026-11-04.dump
     ```

3. **Check the migrations.** Nothing should be pending. If you restored an older dump under a newer release, run `igsctl migrate up`: migrations only move forward.

   ```bash
   igsctl migrate status
   ```

4. **Replay what came after the backup.** The persister resumes from its committed offsets, so records it had already written after the backup was taken are missing. While Kafka still holds them (7 days for `ig.*`), move the persister's group back to the backup's time (Unix seconds, UTC) while it is stopped. Replaying is safe: the persister ignores anything older than what it holds.

   ```bash
   rpk group seek igs-ig-persister --to 1793750400
   ```

   The number is the dump's time in Unix seconds: `date -u -d "2026-11-04 00:00" +%s`. If `kafka.topic_prefix` is set, the group name starts with it.

5. **Start everything again.** The upgrade also runs the migration hook, which applies anything pending. The schedule re-reads anything else at its next visit.

   ```bash
   helm upgrade igs deploy/helm/igs -n igs --reuse-values --set paused=false
   ```

## ClickHouse

Since REQ-019, igs publishes `content.*` in the contract's form and the platform's writer fills the platform's ClickHouse. Its backups are the platform's.

What igs can do after a ClickHouse loss is publish again what it still has:
- **Within the `ig.*` topics' 7 days:** stop `igs-normalizer`, move its group `igs-ig-normalizer-c1` back to the loss's time, and start it. The same observation gets the same `event_id`, so whatever the writer kept is not doubled.

  ```bash
  rpk group seek igs-ig-normalizer-c1 --to 1794186000
  ```

- **Older than that:** the platform's backup. igs's store has every observation, but igs has no tool yet that re-publishes from it.

## Object storage

- **Raw and media buckets:**
  - turn on versioning;
  - replicate to a second bucket in another region;
  - lifecycle: keep current objects forever, and delete replaced (noncurrent) versions after 30 days.
- **Backups bucket** (the dumps): a different account or key from the one igs writes with, so a leaked igs key cannot delete the backups.

## The drill

**Every change:** CI runs `TestBackupRestoreDrill` (`internal/ig/e2e/backup_test.go`). It crawls fake profiles, dumps schema `ig` and restores it into an empty database. It then checks four things:

- every table and partition has the same rows;
- every sequence is where it was;
- no migration is pending;
- the partition job works on the restored database.

**Monthly on the server:** restore last night's dump into a scratch database and compare the counts with production.

```bash
createdb igs_drill
```

```bash
pg_restore --exit-on-error --no-owner --no-privileges --dbname=igs_drill ig-2026-11-04.dump
```

```bash
psql igs_drill -c "SELECT (SELECT count(*) FROM ig.profiles) AS profiles, (SELECT count(*) FROM ig.posts) AS posts, (SELECT count(*) FROM ig.comments) AS comments"
```

```bash
dropdb igs_drill
```

**Write the timings here after the first drill on the server:** how long the dump, the restore and the replay took, and at what size. They are the recovery time to plan for.
