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
| 4 | The shared rate limiter fails closed without Valkey (B6) | [#68](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/68) | in review | `TestNoRequestWithoutValkey` (`internal/scrape/graphql`): with Valkey down, neither the exit pacing nor the query limit lets a request through, and 0 requests reach the platform (the same test let 2 through before the fix); `TestWaitFailsClosedWithoutValkey` (`internal/scrape/ratelimit`): waits until the context ends, and goes ahead once Valkey is back; alert `IgsIgValkeyUnavailable` |
| 5 | Reels: the 7-day window, plus a weekly full pass | - | planned | |
| 6 | Discovery below regular collection: vets only while the schedule is on time and no tier is mostly resting; no caps in production | - | planned | |
| 7 | Identity pool: `igs_identities{state}`, `IgsIgIdentityPoolLow`, page loads per exit per day, and a per-exit page-load limit (off until the pilot measures it) | - | planned | |
| 8 | Park identities every few minutes under a lease (B5), as fbs `27d2062` | - | planned | |
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
