# igs build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** M1d-2 to M1d-5, in the order the M1 row lists them, one pull request each, merged by the session on green CI (owner's rule). M1 is then done and M2 (the brief §5 fixes in igs's copy, plus the unknown-kind crash and the flaky e2e test) follows. C1 (the owner's extra HARs) can happen at any time.

## Decisions taken in the M0 session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| How should M1's moves fit with ongoing fbs work? | igs gets **its own repository**, so fbs work is never impacted. The owner created it as `Abdullah95k/securebrand-microservices-igs` (the session could not: HTTP 403); the session seeded it | D7 changed (brief §2); ADR 0004 rewritten; M1 and M2 now happen in igs's copy |
| Who merges the pull requests? | **The build session merges them** once CI is green (owner, 2026-10-31), and **never touches the fbs repository** | Sessions mark a PR ready and merge it on green CI; nothing is pushed, commented or changed in `securebrand-microservices` |
| How often to refresh profile details (counts, bio, verified, highlights list)? | **Daily** | ADR 0005 §4 "Freshness": daily follower history; highlight items only when the list changed |

The owner's decisions doc (rev 743) still shows the old D7 answer ("share code ... make sure that does not produce any risks"); the answer above supersedes it.

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Verify and decide (ADRs 0004, 0005, reuse matrix) | **done** (merged 2026-10-31) | [#1](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/1) (moved from fbs's #25, closed there) | Every package classified at `df76ce1`; all brief §5 defects confirmed; new findings below |
| M1 | Seed and neutralise (igs repository) | in progress | M1a: [#2](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/2) (merged); M1b-1: [#3](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/3) (merged); M1b-2: [#4](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/4); M1c: [#5](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/5); M1d-1: [#6](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/6); M1d-2: [#7](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/7) (merged); M1d-3: [#8](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/8) | Seed done (`c996ec5`). M1a renamed the module. M1b-1: every Valkey key starts with `igs:` (with a test that fails on any unprefixed key), advisory locks `4_300_100`, `4_300_001`, `igs:registry-sync`, `igs:lane:*`, `igs:client:*`, consumer groups `igs-*`, header `igs-type`, API keys `igs_…`, buckets `igs-media`/`igs-raw`, TTL rule `igs-media-ttl`, schema-check subjects `igs-check-…`; the buf exception removed. M1b-2 (with M1b-3): metric namespace `igs_` and the four `fb_` series renamed `ig_`; alert names `Igs…`, rule groups `igs.*`, recording rules `igs:*`, dashboard uid `igs-overview`; a test fails on any non-`igs_` series; env prefix `IGS_` everywhere (config, tests, CI, Makefile, Compose, chart, scripts). M1c: `transport`, `detect`, `proxy`, `ratelimit`, `registry`, `docids` moved to `internal/scrape/*` with `scripts/move-pkg.sh` (pure moves), and `internal/scrape/boundary_test.go` fails on any import of Facebook or Instagram code. M1d-1: redirect paths and page markers are `detect.Rules` data (Facebook's defaults unchanged, tested both ways). M1d-2: a registry query may name an `endpoint` (a plain path on the identity's host) and ordered `headers` (a name the client already sends is overridden in place); seed, `fb_queries` (migration `00005`), admin API, registry-sync's candidate copy and the GraphQL client carry them; empty means fbs's `/api/graphql/` and header list, byte for byte. The registry's table names (`queries`, `client_params` in igs's schema) wait for M3's `migrations/ig`. M1d-3: `internal/scrape/htmlx` (`JSONBlobs`, an ordered `Pattern` table with `Extract` and `ContainsAny`; Facebook's tokens are now a pattern table keyed by form field) and `internal/scrape/walk` (the walker with `CollectBy(parts, KindFunc)` for typename-less Instagram objects, `Node.Kind`, `Report`, `Int64`, `Unix`, `ParseCount`, `URLExpiry`). Next, in order: M1d-4 a token seam and a `RequestBuilder` in the GraphQL client, then move it to `internal/scrape/graphql`; M1d-5 loader hooks (`Extract`, `IsWall`, `AfterLoad`) and a fingerprint list per pool, then move `session` and `browser` |
| M2 | Fixes in igs's copy (§5 items 1–6, unknown-kind crash) | not started | | Item 6's Chrome 152 profile needs no dependency bump |
| C1 | Owner: HARs of a grid click (post-detail query), reels tab, tagged tab | waiting for owner | | Re-share the Drive folder by link when recorded |
| M3 | Contracts (protos, topics, migrations, config, metrics) | not started | | Follows ADR 0005; own database `igs`, schema `ig` |
| M4 | Instagram client, first `fakeig` slice, `igs-spike probe` | not started | | The probe also answers U9 |
| C2 | Owner: run the probe (direct + each proxy tier) | blocked by M4 | | Answers U1, U3, U5, U8, U9 |
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

## Seeding (done 2026-10-31)

`main` of this repository is fbs's history up to `f1377a9` (77 commits, no tags) plus the seed commit `c996ec5`, which removes `.github/workflows/release.yml` (on a `v*` tag it would publish `ghcr.io/<owner>/fbs` and fbs's chart) and adds [`UPSTREAM.md`](../../UPSTREAM.md) and a README note. The M0 pull request cherry-picks fbs PR #22's five docs commits and fbs PR #25's two M0 commits on top. To port a later fbs change, see `UPSTREAM.md`.

## What the owner needs to do

1. **Review M0 in this repository:** read [ADR 0004](../adr/0004-ig-code-organisation.md) (separate repository, layout, isolation on shared infrastructure) and [ADR 0005](../adr/0005-ig-data-model-and-contracts.md) (topics, tables, privacy). Comment on the M0 pull request if you disagree with anything.
2. **Merge it.** Merging accepts the ADRs and puts the brief and this file on `main`, where every later session starts.
3. **fbs repository:** nothing is required. [PR #22](https://github.com/Abdullah95k/securebrand-microservices/pull/22) there only adds documentation of fbs: merge it if you want that reference next to fbs's code, or close it. fbs PR #25 is closed; its content is here.
4. **Next session:** start it with **only `securebrand-microservices-igs`** selected, using the prompt at the end of the brief. It does M1.
5. **Any time:** record C1 (brief §6): (a) open a post by clicking it in a profile grid; (b) open a profile's reels tab and tagged tab. Save each as "HAR with content", put them in the Drive folder, share it by link and paste the link into a build session.

## Log

- **2026-10-31:** brief written. Evidence so far: `har-findings-2026-10-30.md` (9 recordings). Owner decisions are in the Claude doc "Instagram scraper (igs): owner questions" and summarised in brief §2.
- **2026-10-31 (M0 session, in `securebrand-microservices`):** reuse matrix verified at `df76ce1` with `scripts/import-graph.sh` and a read of every package; ADRs 0004 and 0005 proposed; `.local/` git-ignored for HARs. fbs unit suite green at HEAD (33 packages); no Go code changed. The owner then chose a separate repository and daily profile refreshes; brief D7, M1, M2, §5, §7 and the session prompt updated, ADR 0004 rewritten. Creating the repository from the session failed (HTTP 403); the owner created `securebrand-microservices-igs`, and the session attached it, seeded `main` (`c996ec5`) and moved M0 there.
- **2026-10-31 (M1 session):** the owner asked the session to merge its PRs and never touch the fbs repository. PR #1 (M0) merged on green CI after one re-run of the flaky e2e test above. Then, each merged on green CI: #2 (M1a, module rename), #3 (M1b-1, runtime names), #4 (M1b-2, metrics and env), #5 (M1c, first moves to `internal/scrape`), and M1d-1 (detect wall rules). Each was tested locally with `go test -race ./...` (34–35 packages) and by CI's test, integration (e2e included) and deploy jobs.
