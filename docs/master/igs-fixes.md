# igs fixes (REQ-008)

The owner's approved fixes for igs (REQ-008, 6 November 2026), in the order to build them. Each row has its PR, status and evidence. Times are Iraq time.

**Status:**
- **planned**: not started;
- **in review**: draft PR open;
- **done**: merged, with the evidence named;
- **waiting**: blocked on something named in the row.

## Before the pilot

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 1 | Don't register shared schemas (E3): `content.*` left out of `igsctl schemas apply` unless `--include-content`; `hooks.schemas: false` in `examples/production.yaml` | [#66](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/66) | done | `TestPlatformTopicsLeftOut` (`cmd/igsctl`); the production example renders no schema hook |
| 2 | Don't change topics igs doesn't own: `igsctl kafka apply` is a dry run by default; `--apply` creates missing topics only; adding partitions or changing retention needs `--grow-partitions` or `--set-retention`; `content.*` left alone unless `--include-content`; `hooks.topics: false` in production | [#66](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/66) | done | `TestApplyTopicsLeavesExistingTopics` (`internal/platform/kafka`): an existing topic keeps its partitions and retention under a dry run and under `--apply`; `TestPrintTopicDiffs` |
| 3 | Exits named by tier and number, never by address (D4, F2), in logs, metric labels and Valkey keys; user pk, not usernames, in logs; the proxy provider's host removed from the docs | [#67](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/67) | done | `TestExits`, `TestBuckets` (`internal/scrape/proxy`): exits are `isp#<n>`, buckets `isp/b<k>` and `res/gw-us`, and no name holds an address; `TestAccountName` (`internal/platform/log`); `TestProbeRestsWalledExits` reports walls by `isp/b<k>`; the provider's name is in no tracked file |
| 4 | The shared rate limiter fails closed without Valkey (B6) | [#68](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/68) | done | `TestNoRequestWithoutValkey` (`internal/scrape/graphql`): with Valkey down, neither the exit pacing nor the query limit lets a request through, and 0 requests reach the platform (the same test let 2 through before the fix); `TestWaitFailsClosedWithoutValkey` (`internal/scrape/ratelimit`): waits until the context ends, and goes ahead once Valkey is back; alert `IgsIgValkeyUnavailable` |
| 5 | Reels: the 7-day window, plus a weekly full pass. A scheduled run reads the reels tab back `ig.schedule.reels_window` (168h); once a profile's last full pass is `reels_full_every` (168h) old, the run reads it back to `limits.max_age_days`. A profile's first read and full runs always read it all | [#69](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/69) | done | `TestReelsWindow` (`internal/ig/e2e`): a full run reads all reels and records `last_full_at`; a scheduled run within the week reads only the reels of the last 7 days (an older reel's changed likes stay unread) and leaves `last_full_at`; once `last_full_at` is a week old, a scheduled run reads them all again and the older reel's likes are updated. Without the change the test fails |
| 6 | Discovery below regular collection: vets only while the schedule is on time and no tier is mostly resting; no caps in production. The vetter starts no vet while an unpaused profile is overdue by its own interval, a scheduled run is still going a full interval after it started, or more than half of a static tier's exits rest (`ig.discovery.hold_when_behind`, `hold_resting_share` 0.5); `igs_discovery_held{reason}`. `max_new_per_day: 0` in the production example | [#70](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/70) | done | `TestDiscoveryWaitsForRegularCollection` (`internal/ig/e2e`): no vet starts while a profile is 4 h overdue on a 3 h interval, nor while its run has gone 4 h, nor while 3 of 4 exits rest; a paused profile does not count; with 2 of 4 resting, the vet starts. Without the check the test fails. `TestRestingTier`: ended rests, gateway sessions and other names don't count. `TestIGDefaults`: no cap by default |
| 7 | Identity pool: `igs_identities{state}`, `IgsIgIdentityPoolLow`, page loads per exit per day, and a per-exit page-load limit (off until the pilot measures it). Gauge `igs_identities{tier,state}` (idle, leased, waiting) and counter `igs_identity_shortfall_total{tier,reason}` (exits_cooling, no_other_exit, at_cap); `IgsIgIdentityPoolLow` on 15 minutes of shortfalls; `igs_exit_page_loads_total{tier,exit}`; `ig.exit_page_loads_per_day` (0, off): a static exit at the limit rests until the UTC day ends | [#71](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/71) | done | `TestIdentityGaugesAndShortfall` (`internal/scrape/session`): the gauge follows leased, idle and waiting identities; a task waiting at the cap and one finding every exit resting are counted. `TestExitPageLoadLimit` (`internal/ig/e2e`): with a limit of 4 a day and 3 exits, each exit's fleet-wide count stops at about 4, all three rest until the day ends (reason `page_load_limit`), and the Prometheus counter matches the fleet's count. Monitoring consistency test: the alert's metrics exist and its runbook section does |
| 8 | Park identities every few minutes under a lease (B5), as fbs `27d2062`. A running crawler checkpoints every identity it holds (idle or in use) every 2 minutes under a 5-minute lease; once a dead crawler's lease ends, another adopts its checkpoint within a minute and parks its identities exit by exit; a clean stop parks and drops the checkpoint. Recorded in `UPSTREAM.md` | [#72](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/72) | in review | `TestCheckpointOutlivesACrash` (`internal/scrape/session`, against a Valkey stand-in): a crawler that dies after a checkpoint leaves its idle and busy identities; nobody takes them while its lease holds; after it, another crawler adopts both, once, and resumes them with their tokens; a crawler that parked leaves nothing to adopt; no proxy credential is stored. `TestEveryKeyIsPrefixed` covers the new keys |
| 9 | The linked Facebook page's ID (E9) | - | planned | |
| 10 | Keep everything: `media_rows_days` and `jobs_days` 0 | - | planned | |
| 11 | Port fbs fixes: `9b1863c` (the leader lock), and `ca4769e`, `907e08a`, `cfc4295` (consumer panic recovery, revoked partitions); recorded in `UPSTREAM.md` | - | planned | |
| 11a | The chart reads the platform's secrets (`platform-credentials`, `proxies`). Added with the owner's approval, 6 November | - | planned | |
| 12 | The 24-hour soak against the fake Instagram, in its own namespace on the production cluster, with its own throwaway stores | - | waiting: the owner confirms the cluster is ready | |

## The pilot

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 13 | The 48-hour pilot on the production cluster, with the owner's 348 Phase 0 accounts; report in `docs/master/igs-pilot-report.md` | - | waiting: the cluster, and fixes 1 to 12 | |

## After the pilot, before the full production run

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 14 | Comment deletions and 30-day re-reads (E7) | - | planned | |
| 15 | Walled posts and per-post coverage (E10) | - | planned | |
| 16 | A metrics series for every re-read post | - | planned | |
| 17 | Raw responses: parser version on every line; no `raw_ref` without its object; failures and a 1% sample under prefixes of their own (E8) | - | planned | |
| 18 | Profile links: `external_url` if served; `country_signals` (E9) | - | planned | |
| 19 | Alerts "no data for N minutes" and "memory near limit"; `igsctl health` and its runbook step (F3, F5) | - | planned | |

## Before the production canary

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 20 | Topic sizes for 10,000 accounts (proposed to the master on 6 November: 186 partitions instead of 915) | - | planned | |
| 21 | Chart defaults: NetworkPolicy with egress, alerts on, hook Job resources, the browser in its own container (B3) | - | planned | |
| 22 | Security: a credential per component, gitleaks, Dependabot, `govulncheck`, an image scan (I1, I3, G4) | - | planned | |
| 23 | Smaller items: a migration check (A6), Iraq time in owner-facing output (B7), a 3-day canary in the runbook (H3) | - | planned | |
