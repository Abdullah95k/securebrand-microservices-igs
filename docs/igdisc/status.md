# igdisc status

Instagram keyword discovery, built in the igs repository beside igs's own work. The master session reads this page. Times are Iraq time (UTC+3).

**Now (6 November 2026):** plan written ([`igdisc-plan.md`](../master/igdisc-plan.md)), waiting for the owner's recording ([`har-recording.md`](har-recording.md)) and the owner's approval. **Nothing is built.** Nothing has run against Instagram, the production cluster or the shared server.

## Decisions (the owner's)

| Date | Decision |
|---|---|
| 6 Nov | Plan only until the owner approves the plan |
| 6 Nov | Measure logged-out search first with the owner's own recording (option a); a production probe only after it, and only with approval |
| 6 Nov | Hooks in igs's own files go to the igs session as separate small PRs |
| 6 Nov (platform) | Test directly on the production cluster at full scale (D-026); no caps in production (D-008); logged out, no CAPTCHA or challenge solving; contract v1.0.0 (D-016) |

## Open

| # | Question | For | Status |
|---|---|---|---|
| O1 | Approve the plan, Track 1 first, Track 2 only if the recording allows it | owner | **master answered yes (6 Nov, 18:15), citing D-027; waiting for the owner to confirm** |
| O2 | Private accounts: second list only when they pass the Iraq check, or every one | owner | master: only when they pass; owner to confirm |
| O3 | Discovery's share of each exit (10% proposed) | owner | master: 10%, and agree with the igs session who builds it; owner to confirm |
| O4 | Canary cap of 50 a day during the labelled sample, then none | owner | master: yes; owner to confirm |
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
- **6 Nov, 18:15:** the master answered M1 to M4 (taken into the plan, §3.6 and §7) and O1 to O4, citing D-027. This session hasn't seen D-027, so O1 to O4 are recorded as waiting for the owner's confirmation, and nothing is built until then.
- **6 Nov, 18:25:** contract v1.1.0 published (`5e13d2f`). Checked: the two new fields, `KeywordID` and `FoldKeyword` (identical to igs's fold on all 393 list spellings), and the canary option. The check ran in a throwaway copy; igs's `go.mod` is unchanged.
