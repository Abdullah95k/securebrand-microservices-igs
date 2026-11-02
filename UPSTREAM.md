# Upstream: fbs

This repository (igs) owns a copy of the code of fbs, the Facebook page scraper. The decision and its reasons are in [ADR 0004](docs/adr/0004-ig-code-organisation.md). igs work never changes fbs's repository.

- **Upstream repository:** [`Abdullah95k/securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices)
- **Copied from:** `f1377a9` (main, 2026-10-31), with its full history, so `git blame` reaches back into fbs.
- **Seed changes:** `.github/workflows/release.yml` removed (on a `v*` tag it would have published `ghcr.io/<owner>/fbs` and fbs's Helm chart); this file and a note at the top of the README added.

To port an fbs change: `git remote add upstream https://github.com/Abdullah95k/securebrand-microservices`, `git fetch upstream`, then `git cherry-pick <sha>` while the paths still match, or port it by hand. Record it below either way. Fetching is read-only; never push to fbs's repository.

The Go module here is `github.com/abdullah95k/securebrand-microservices-igs` (fbs: `github.com/abdullah95k/securebrand-microservices`), so a cherry-picked fbs change conflicts on any import line it touches. Resolve those by keeping this repository's paths, then run `go build ./...`. If it adds or changes a `.proto` file, run `buf generate` afterwards.

## Ported from fbs

| Date | fbs commit | What | igs PR |
|---|---|---|---|

## igs fixes fbs may want

Fixes made in igs's copy of shared code that fbs still lacks. fbs's own sessions decide whether to port them.

| Date | igs PR | Defect | fbs location |
|---|---|---|---|
| 2026-11-02 | M2a | `fbctl dlq replay` did nothing for 7 days: the dead letter marks the task done, and the replay re-sent the same task id. Now each replayed task gets a new id (`task.Replay`, `internal/crawl/dlq`) with its retry counts reset, keeps its lineage and checkpoint, and rejoins its job while the job still runs | `internal/crawl/stage/worker.go`, `cmd/fbctl/commands.go` (`dlqReplay`) |
| 2026-11-02 | M2a | A task of unknown kind crashed the crawler (`deadLetter` → `topics.Task` panicked, unrecovered). Now `topics.TaskTopic` reports unknown kinds, and a stage that panics dead-letters its task (reason `panic`) | `internal/topics/topics.go`, `internal/crawl/stage/worker.go` |
| 2026-11-02 | M2a | The DLQ's origin header was unprefixed for decoded tasks and prefixed for undecodable records; both are unprefixed now (as on the retry topics) | `internal/crawl/stage/worker.go` (`deadLetterRaw`) |
| 2026-11-02 | M2a | `GET`, `cancel` and `results` of `/v1/jobs/{id}` did not check the caller owns the job; another client's job now answers 404, an admin sees all | `internal/api/jobs.go`, `internal/dispatch/dispatch.go` |
| 2026-11-02 | M2a | `max_comments_per_post` held per 20-page segment: `Task.items_seen` now carries what earlier segments saw | `internal/crawl/stage/post.go`, `api/proto/securebrand/fbs/task/v1/tasks.proto` |
| 2026-11-02 | M2a | `TestBlockedIdentitiesRetryAndTruncation` flaked: with every 4th request of an identity limited, five stalls in a row dead-lettered a thread task about once in a thousand; the test now allows 40 | `internal/e2e/e2e_test.go` |
| 2026-11-02 | M2b | One bad record stalled the persister for good (the batch was retried every 2 s forever), and an "undecodable record skipped" was written half-decoded. Now undecodable records, and records Postgres rejects on their own data (classes 22 and 23, or a value pgx cannot encode, such as an empty UUID), are found by bisecting the batch and quarantined to `fbs.persist.dlq` (metric `persist_quarantined_total`, alert `IgsPersisterQuarantine`, `fbctl dlq replay --persist`); outages still retry the whole batch | `internal/persist/consumer.go` |
| 2026-11-02 | M2b | Suppression was enforced only by the crawlers, and the page upsert set `status = 'active'`, undoing an erasure. The persister now drops a suppressed page's entities and the media of redacted comments, redacts suppressed authors' comments, and never updates a suppressed page row | `internal/persist/writer.go` |
| 2026-11-02 | M2c | Detection rules were hard-coded although the code called them config: `fb.detect` now extends (or replaces) them; HTTP 400, 401 and 403 classified as OK and now go by their body (block → rate limit, checkpoint, login) with a soft-error fallback | `internal/fb/detect/detect.go`, `internal/config/config.go` |
| 2026-11-02 | M2c | The rate-limit cooldown was effectively a constant 60 s: a second block retired the identity first. It now grows (30 s, 60 s, 2 min, 4 min) and retires at the fifth limit in a row | `internal/fb/session/identity.go` |
| 2026-11-02 | M2c | Every identity of a pod used one fingerprint: `fb.fingerprint_profiles` (default `chrome_152`, `chrome_146`) is picked per identity, and `chrome_152` joins `fingerprints.yaml`; the unused `fb.bootstrap_attempts` is gone | `internal/fb/session/pool.go`, `configs/fingerprints.yaml` |

