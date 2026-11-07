# igdisc status

Instagram keyword discovery, built in the igs repository beside igs's own work. The master session reads this page. Times are Iraq time (UTC+3).

**Now (7 November 2026):** building step 1, Track 1 (keyword evidence from what igs already reads). The owner confirmed D-027 and said to start step 1; the master's answers to O1 to O4 stand. Still waiting for the owner's recording ([`har-recording.md`](har-recording.md)), which decides Track 2. Nothing has run against Instagram, the production cluster or the shared server.

| Step | What | Where | Status |
|---|---|---|---|
| 1a | igdisc's own code: the keyword index, the recorder, the evidence table (its own migrations), keyword context, erasure; unit and database tests | `internal/ig/igdisc`, `migrations/igdisc`, #65 | built, tested locally |
| 1b | The hooks into igs's files: the collector hands posts and comments to the recorder, the score's `keyword_context` signal, migrations, erasure, config, an e2e test | a second PR stacked on #65 | in progress |

## Decisions (the owner's)

| Date | Decision |
|---|---|
| 6 Nov | Plan only until the owner approves the plan |
| 6 Nov | Measure logged-out search first with the owner's own recording (option a); a production probe only after it, and only with approval |
| 7 Nov | D-027 is the owner's: the master decides the plan's questions within the owner's decisions. Start step 1 |
| 6 Nov (master, D-027) | O1 approved (Track 1 first; Track 2 only if the recording shows logged-out access; stop at any wall); O2 private accounts to the second list only if they pass the Iraq check; O3 10% of each exit; O4 a cap of 50 a day in the canary week only |
| 6 Nov | Hooks in igs's own files go to the igs session as separate small PRs |
| 6 Nov (platform) | Test directly on the production cluster at full scale (D-026); no caps in production (D-008); logged out, no CAPTCHA or challenge solving; contract v1.0.0 (D-016) |

## Open

| # | Question | For | Status |
|---|---|---|---|
| O1 | Approve the plan, Track 1 first, Track 2 only if the recording allows it | owner | **decided** (master, D-027; owner confirmed 7 Nov) |
| O2 | Private accounts: second list only when they pass the Iraq check, or every one | owner | **decided** (master, D-027; owner confirmed 7 Nov) |
| O3 | Discovery's share of each exit (10% proposed) | owner | **decided** (master, D-027; owner confirmed 7 Nov) |
| O4 | Canary cap of 50 a day during the labelled sample, then none | owner | **decided** (master, D-027; owner confirmed 7 Nov) |
| M1 | Stable keyword IDs across services | master | **done in contract v1.1.0:** `contracts.KeywordID` and `FoldKeyword`, now identical to igs's `Normalize` (all 393 list spellings checked); computed from the list's stored spelling |
| M2 | Contract additions | master | **done in v1.1.0:** `SOURCE_LOCATION_PAGE = 18`, `Evidence.content_time = 9`, and `conformance.Options.Canary` for the canary week |
| M3 | The Google discovery tool | master | **answered:** outside the platform's scope; not planned on |
| M4 | When `discovery.candidate.v1` exists on production | master | **answered:** in the platform's first production apply; the master will say when |
| P1 | `SESSION-PACK.md` (documents 03 to 09) didn't reach this session; §5 of the plan follows `igs-gaps.md` instead | owner | open |
| R1 | Owner's recording of 9 search surfaces, logged out | owner | waiting |

Answered by the contract v1.0.0:
- the source publishes a candidate when found, and the collector publishes the outcome;
- the platform registers every shared subject, so igdisc registers nothing.

## Log

- **6 Nov:** session started from the owner's REQ-002. Read the contract v1.0.0 (`candidate.proto`, conformance, topics), fbs's keyword list and discovery (identical `iraq.yaml`: 332 spellings on, 52 off), and igs's discovery, lanes and gaps. Desk research on logged-out search found that hashtag pages very likely need a login since October 2024 (a third-party source). Wrote the plan, the owner's recording steps and this page. Docs-only draft PR [#65](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/65).
- **6 Nov, 18:10:** #65's `integration` job was red from a date in the fake Instagram, not from this PR: its newest post was fixed at 30 October, and from 15:00 the 7-day refresh window held no fake posts (`TestScheduledRuns`). The igs session fixed it on `main` (`6369e3d`, #66); `main` merged into this branch so CI runs again.
- **6 Nov, 18:15:** the master answered M1 to M4 (taken into the plan, §3.6 and §7) and O1 to O4, citing D-027. This session hasn't seen D-027, so O1 to O4 were recorded as waiting for the owner's confirmation.
- **6 Nov, 18:25:** contract v1.1.0 published (`5e13d2f`). Checked: the two new fields, `KeywordID` and `FoldKeyword` (identical to igs's fold on all 393 list spellings), and the canary option. The check ran in a throwaway copy; igs's `go.mod` is unchanged.
- **7 Nov:** the owner confirmed D-027 and said to start step 1. igdisc's own code (Track 1) is on #65's branch: the keyword index on igs's matcher (one matcher group per spelling, so a match names its keyword and contract ID), the recorder (a 6-hour window; tags, co-authors, mentions, commenters with `pii_mode: full`; accounts on the schedule or rejected skipped), `ig.igdisc_evidence` with its own goose table, keyword context (distinct places), erasure. The contract v1.1.0 module is now in igs's `go.mod`. Tests pass locally on Postgres 16 (CI uses 18).
