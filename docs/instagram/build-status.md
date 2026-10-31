# igs build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** **checkpoint C2, the owner runs the probe** (steps in "What the owner needs to do" below). M4 is done. The next build session does **M2** (the brief §5 fixes, the unknown-kind crash, the flaky e2e test), then M3, by the owner's order; when the probe reports come back, that session first records the answers to U1, U3, U5, U8 and U9 here and in the HAR findings. If U1 shows the grid walled at 24 posts, it proposes backfill alternatives and waits for the owner (brief §6, C2). C1 (the owner's extra HARs) can happen at any time.

## Decisions taken in the M0 session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| How should M1's moves fit with ongoing fbs work? | igs gets **its own repository**, so fbs work is never impacted. The owner created it as `Abdullah95k/securebrand-microservices-igs` (the session could not: HTTP 403); the session seeded it | D7 changed (brief §2); ADR 0004 rewritten; M1 and M2 now happen in igs's copy |
| Who merges the pull requests? | **The build session merges them** once CI is green (owner, 2026-10-31), and **never touches the fbs repository** | Sessions mark a PR ready and merge it on green CI; nothing is pushed, commented or changed in `securebrand-microservices` |
| How often to refresh profile details (counts, bio, verified, highlights list)? | **Daily** | ADR 0005 §4 "Freshness": daily follower history; highlight items only when the list changed |
| Milestone order after M1? (owner's session prompt, 2026-10-31) | **M4 before M2 and M3**, so the probe reaches the owner sooner | M4's client builds on the M1 seams; M2's fixes and M3's contracts follow it |

The owner's decisions doc (rev 743) still shows the old D7 answer ("share code ... make sure that does not produce any risks"); the answer above supersedes it.

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Verify and decide (ADRs 0004, 0005, reuse matrix) | **done** (merged 2026-10-31) | [#1](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/1) (moved from fbs's #25, closed there) | Every package classified at `df76ce1`; all brief §5 defects confirmed; new findings below |
| M1 | Seed and neutralise (igs repository) | **done** (2026-10-31) | M1a: [#2](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/2) (merged); M1b-1: [#3](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/3) (merged); M1b-2: [#4](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/4); M1c: [#5](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/5); M1d-1: [#6](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/6); M1d-2: [#7](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/7) (merged); M1d-3: [#8](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/8) (merged); M1d-4: [#9](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/9) (merged); M1d-5a: [#10](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/10) (merged); M1d-5b: [#11](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/11) (merged) | Seed done (`c996ec5`). M1a renamed the module. M1b-1: every Valkey key starts with `igs:` (with a test that fails on any unprefixed key), advisory locks `4_300_100`, `4_300_001`, `igs:registry-sync`, `igs:lane:*`, `igs:client:*`, consumer groups `igs-*`, header `igs-type`, API keys `igs_…`, buckets `igs-media`/`igs-raw`, TTL rule `igs-media-ttl`, schema-check subjects `igs-check-…`; the buf exception removed. M1b-2 (with M1b-3): metric namespace `igs_` and the four `fb_` series renamed `ig_`; alert names `Igs…`, rule groups `igs.*`, recording rules `igs:*`, dashboard uid `igs-overview`; a test fails on any non-`igs_` series; env prefix `IGS_` everywhere (config, tests, CI, Makefile, Compose, chart, scripts). M1c: `transport`, `detect`, `proxy`, `ratelimit`, `registry`, `docids` moved to `internal/scrape/*` with `scripts/move-pkg.sh` (pure moves), and `internal/scrape/boundary_test.go` fails on any import of Facebook or Instagram code. M1d-1: redirect paths and page markers are `detect.Rules` data (Facebook's defaults unchanged, tested both ways). M1d-2: a registry query may name an `endpoint` (a plain path on the identity's host) and ordered `headers` (a name the client already sends is overridden in place); seed, `fb_queries` (migration `00005`), admin API, registry-sync's candidate copy and the GraphQL client carry them; empty means fbs's `/api/graphql/` and header list, byte for byte. The registry's table names (`queries`, `client_params` in igs's schema) wait for M3's `migrations/ig`. M1d-3: `internal/scrape/htmlx` (`JSONBlobs`, an ordered `Pattern` table with `Extract` and `ContainsAny`; Facebook's tokens are now a pattern table keyed by form field) and `internal/scrape/walk` (the walker with `CollectBy(parts, KindFunc)` for typename-less Instagram objects, `Node.Kind`, `Report`, `Int64`, `Unix`, `ParseCount`, `URLExpiry`). M1d-4: an identity's tokens are a map keyed by form field (`session.Tokens`; Facebook's `Valid`, `Jazoest`, `ConsentWall` are functions over it), and the GraphQL client asks a `RequestBuilder` for path, ordered headers and body (`FacebookBuilder` holds fbs's form and headers; `TestFacebookRequestBytes` pins them byte for byte). Moving the GraphQL core needs `session` neutral first, so it joins M1d-5. M1d-5a: `session.Hooks{Extract, IsWall, AfterLoad}` run by the loader and the browser bootstrapper (a wall path is never followed, so it is classified; a missing `Extract` is `ErrNoHooks`), `PoolConfig.Profiles` (a random fingerprint per identity; empty = `Profile`), and `internal/fb/client` holding Facebook's request `Builder` and `Hooks` (tokens, walls, datr, consent); the GraphQL package now holds only neutral code. M1d-5b: `session`, the GraphQL core and `browser` moved to `internal/scrape` (pure moves); the browser tests that need `fakefb` moved to `internal/fb/client`. Facebook keeps `internal/fb/{client,htmlx,parse,fbtest}`. Facebook's builder and hooks live in `internal/fb/client`, not in `internal/fb/graphql` as ADR 0004's table put it |
| M2 | Fixes in igs's copy (§5 items 1–6, unknown-kind crash) | not started | | After M4 (owner's order). Item 6: the per-identity profile list exists since M1d-5a (`PoolConfig.Profiles`); adding Chrome 152 needs no dependency bump |
| C1 | Owner: HARs of a grid click (post-detail query), reels tab, tagged tab | waiting for owner | | Re-share the Drive folder by link when recorded |
| M3 | Contracts (protos, topics, migrations, config, metrics) | not started | | After M4 and M2 (owner's order). Follows ADR 0005; own database `igs`, schema `ig`. Also gives the registry its table names (`queries`, `client_params`, with M1d-2's `endpoint` and `headers` columns) |
| M4 | Instagram client, first `fakeig` slice, `igs-spike probe` | **done** (2026-10-31) | M4a: [#12](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/12) (merged); M4b and M4c: [#13](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/13) | M4a: `internal/ig/{htmlx,detect,client}` (tokens with candidate patterns, the `/accounts/login` and `/challenge` walls, the request builder, `configs/ig/queries.seed.yaml` with the five operations). M4b: `internal/fakeig` (generated world; pages that mint tokens and cookies; the five operations, checked like a real client; faults for walls, rate limits, rotated doc_ids and U1, U3, U5, U9) and `internal/ig/igtest`, with client tests over both transports. M4c: `internal/ig/probe` and `igs-spike probe` (report without PII; `make igs-spike-bin`). The probe uses fingerprint `chrome_146` by default; `chrome_152` (the recordings' Chrome) comes with M2 item 6 |
| C2 | Owner: run the probe (direct + each proxy tier) | **waiting for owner** | | Answers U1, U3, U5, U8, U9 |
| M5 | Parsers, Instagram scrubber, golden fixtures | not started | | Also checks U10 and the pk range against the fixtures |
| M6 | Crawl stages, full `fakeig`, e2e with ground truth | not started | | |
| M7 | Persister, API, scheduler, media, GDPR | not started | | Profile refresh daily (decision above) |
| M8 | Phase 0 kit (`igs-spike phase0`) | not started | | The three fbs kit gaps are confirmed (reuse matrix §6) |
| C3 | Owner: Phase 0 run per proxy tier | blocked by M8 | | Rotating ISP vs residential |
| M9 | Registry sync and browser fallback | not started | | |
| M10 | Deploy and operations (Helm, CI, alerts, load and soak, runbooks) | not started | | Also removes the copied Facebook code once igs's suites cover the pipeline (ADR 0004 §2) |
| C4 | Owner: legal sign-off, staging, canary, ramp | waiting for legal | | Legal review in progress (2026-10-31) |

## Open questions

| # | Question | Status |
|---|---|---|
| U1 | Grid paging past 24 posts | open: probe (C2) |
| U2 | Cheap post-detail query | open: HAR (C1) |
| U3 | Client-state form fields needed? | open: probe (C2) |
| U4 | Per-IP tolerance and wire bytes per tier | open: Phase 0 (C3) |
| U5 | Cursors bound to identity? | open: probe (C2) |
| U6 | doc_ids in static bundles? | open: M9 |
| U7 | Tagged posts logged out? | open: HAR (C1) |
| U8 | Cookies set and expected | open: probe (C2) |
| U9 | Can a profile whose username changed be read by its pk alone (the grid `_connection` query with `id` and no cursor)? The profile and first-grid queries take a username | new (M0); open: probe (C2) |
| U10 | Does a media pk encode its creation time? If so, the grid walk can stop at the age window without reading post details | new (M0), a hypothesis; open: check against the HAR timestamps in M5 |

## Findings

From M0 (details and evidence in the [reuse matrix](reuse-matrix.md)):
- **fbs assumes it is alone on its infrastructure:** `fbs_` metrics at package init; unprefixed Valkey keys (`jobs:finalize`, `rl:global`, `lease:post:<n>` would collide); fixed advisory-lock numbers (a second scheduler would fight fbs's for leadership); a janitor whose orphan sweep and TTL rule cover the whole media bucket. igs must use different values for every one of them on shared infrastructure (ADR 0004 §4).
- **An igs pod exporting `fbs_*` would hide fbs alerts:** the rules select by name only, and `fbs_scheduler_leader` is always exported.
- **fbs's partition maintenance ignores the schema**, so igs never creates tables in a database fbs maintains (own database `igs`, ADR 0005).
- **New defect, for M2:** a task of unknown kind crashes the crawler (`deadLetter` → `topics.Task` panics; no recover). fbs has it too: worth porting there (listed for `UPSTREAM.md`).
- **All six brief §5 defects are confirmed at HEAD**, with extra detail: replayed DLQ tasks keep their stall count; the persister appends a half-decoded record before checking the error; the page upsert's `status = 'active'` undoes erasure; the identity cooldown is always 60 s.
- **The pinned tls-client (v1.17.1) already ships `chrome_150` and `chrome_152`**; fbs's `fingerprints.yaml` only has 146, 144 and 133.
- **`SplitParts` already reads Instagram's NDJSON**; the HTML loader would follow a redirect to `/accounts/login` or `/challenge` as if it were the page (fixed by igs's wall paths in M4).
- **Go cannot import another module's `internal/` packages**, which is why a separate repository means a copy (ADR 0004).
- **A flaky test in the copied suite:** `TestBlockedIdentitiesRetryAndTruncation` (`internal/e2e`) failed once in PR #1's CI (a thread task used up its 5 stalled retries under fakefb's rate-limit fault; job `partial`, 329/349 comments) and passed on re-run, with identical code. It is part of the regression net for M1 and M2; make it deterministic before relying on it for M2's retry fixes.
- The GitHub integration in build sessions cannot create repositories (HTTP 403); the owner creates them, and a session attaches one with `add_repo`.

From the M1d-2 to M1d-5a session:
- **registry-sync must copy a query's route.** It builds a candidate doc_id's row by copying the active row; without copying `endpoint` and `headers`, a rotation would have sent Instagram's promoted highlights query to `/api/graphql`. Fixed in M1d-2 and covered by an integration test.
- **Facebook's token patterns: the last match wins,** not the first (several `lsd` sources). The neutral `Extract` keeps that rule, and a test pins it.
- **The GraphQL core could not move before `session`:** the client depends on `session.Identity`. Hence the split into M1d-5a (seams) and M1d-5b (moves).
- **Local runs without Docker:** a session can start Postgres 16 and Redis from the container's packages. All tests pass on them except three persister writer tests, which use Postgres 18's `RETURNING old./new.` and fail on `main` too. There is no Kafka locally, so the e2e suites run only in CI.

## Seeding (done 2026-10-31)

`main` of this repository is fbs's history up to `f1377a9` (77 commits, no tags) plus the seed commit `c996ec5`, which removes `.github/workflows/release.yml` (on a `v*` tag it would publish `ghcr.io/<owner>/fbs` and fbs's chart) and adds [`UPSTREAM.md`](../../UPSTREAM.md) and a README note. The M0 pull request cherry-picks fbs PR #22's five docs commits and fbs PR #25's two M0 commits on top. To port a later fbs change, see `UPSTREAM.md`.

## What the owner needs to do

**Checkpoint C2: run the probe.** It answers whether the grid pages past 24 posts (U1), whether queries need the browser's client-state fields (U3), whether cursors are tied to one identity (U5), which cookies Instagram sets (U8), and whether a profile can be read by its id alone (U9). It sends about 50 requests per profile, one every 3 seconds, so each profile takes 2 to 3 minutes. The report it writes holds no usernames, ids or text.

1. Install Go once: `brew install go` (macOS).
2. Get the code: `git clone https://github.com/Abdullah95k/securebrand-microservices-igs` and `cd securebrand-microservices-igs`.
3. Make a file `canary.txt` outside the repository (for example in your home folder), with 5 to 10 public profiles, one per line (a username, `@username` or the profile URL). Mix a big brand, a small account, at least two with more than 50 posts, and one with highlights. Don't commit it.
4. Run it without a proxy: `go run ./cmd/igs-spike probe --profiles ~/canary.txt --out probe-direct.json`.
5. For each proxy tier you have (rotating ISP first, then residential):
   1. `cp phase0/proxies.example.yaml phase0/proxies.yaml`, put your credentials in it, `chmod 600 phase0/proxies.yaml` (it is git-ignored).
   2. `go run ./cmd/igs-spike probe --profiles ~/canary.txt --config phase0/proxies.yaml --tier isp --out probe-isp.json` (then `--tier residential --out probe-residential.json`).
6. Send the `probe-*.json` files back: paste them into the next build session, or put them in the Drive folder and share the link. Ctrl-C stops a run early and still writes what it read.

**Any time:** record C1 (brief §6):
1. Open a post by clicking it in a profile grid.
2. Open a profile's reels tab and its tagged tab.
3. Save each as "HAR with content" and put them in the Drive folder.
4. Share the folder by link, and paste the link into a build session.

**Next build session:** start it the same way, with the same prompt. It does M2 and records the probe's answers once you send them.

## Log

- **2026-10-31:** brief written. Evidence so far: `har-findings-2026-10-30.md` (9 recordings). Owner decisions are in the Claude doc "Instagram scraper (igs): owner questions" and summarised in brief §2.
- **2026-10-31 (M0 session, in `securebrand-microservices`):** reuse matrix verified at `df76ce1` with `scripts/import-graph.sh` and a read of every package; ADRs 0004 and 0005 proposed; `.local/` git-ignored for HARs. fbs unit suite green at HEAD (33 packages); no Go code changed. The owner then chose a separate repository and daily profile refreshes; brief D7, M1, M2, §5, §7 and the session prompt updated, ADR 0004 rewritten. Creating the repository from the session failed (HTTP 403); the owner created `securebrand-microservices-igs`, and the session attached it, seeded `main` (`c996ec5`) and moved M0 there.
- **2026-10-31 (M1 session):** the owner asked the session to merge its PRs and never touch the fbs repository. PR #1 (M0) merged on green CI after one re-run of the flaky e2e test above. Then, each merged on green CI: #2 (M1a, module rename), #3 (M1b-1, runtime names), #4 (M1b-2, metrics and env), #5 (M1c, first moves to `internal/scrape`), and M1d-1 (detect wall rules). Each was tested locally with `go test -race ./...` (34–35 packages) and by CI's test, integration (e2e included) and deploy jobs.
- **2026-10-31 (M1d session 2):** the decisions doc was unchanged (rev 743). Each merged on green CI (test, integration with e2e, deploy):
  - [#7](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/7) (M1d-2): registry endpoint and headers, migration `00005`;
  - [#8](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/8) (M1d-3): `scrape/htmlx` and `scrape/walk`;
  - [#9](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/9) (M1d-4): token map and `RequestBuilder`;
  - [#10](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/10) (M1d-5a): loader hooks, fingerprint list, `internal/fb/client`.

  Each was tested locally with `go test -race ./...` against a local Postgres 16 and Redis; registry-sync's integration tests and the new migration (up, down, up) also ran locally. The flaky e2e test did not recur. The session stopped before M1d-5b so that it could end cleanly.
- **2026-10-31 (same session, continued at the owner's request):** [#11](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/11) (M1d-5b) moved `session`, the GraphQL core and `browser` to `internal/scrape`, merged on green CI. M1 is done.
- **2026-10-31 (same session, continued):** [#12](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/12) (M4a, Instagram client) and [#13](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/13) (M4b `fakeig` and M4c `igs-spike probe`), merged on green CI. M4 is done; the session stopped at checkpoint C2.
