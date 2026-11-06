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
| O1 | Approve the plan, Track 1 first, Track 2 only if the recording allows it | owner | open |
| O2 | Private accounts: second list only when they pass the Iraq check, or every one | owner | open |
| O3 | Discovery's share of each exit (10% proposed) | owner | open |
| O4 | Canary cap of 50 a day during the labelled sample, then none | owner | open |
| M1 | Stable keyword IDs across services (`<group>:<language>:<folded spelling>` proposed) | master | open |
| M2 | Contract additions: `SOURCE_LOCATION_PAGE`, `Evidence.content_time` | master | open |
| M3 | The Google discovery tool: who runs it, and how it searches after Google's Custom Search API closes (1 Jan 2027) | master | open |
| M4 | When the platform creates `discovery.candidate.v1` on production Kafka | master | open |
| P1 | `SESSION-PACK.md` (documents 03 to 09) didn't reach this session; §5 of the plan follows `igs-gaps.md` instead | owner | open |
| R1 | Owner's recording of 9 search surfaces, logged out | owner | waiting |

Answered by the contract v1.0.0:
- the source publishes a candidate when found, and the collector publishes the outcome;
- the platform registers every shared subject, so igdisc registers nothing.

## Log

- **6 Nov:** session started from the owner's REQ-002. Read the contract v1.0.0 (`candidate.proto`, conformance, topics), fbs's keyword list and discovery (identical `iraq.yaml`: 332 spellings on, 52 off), and igs's discovery, lanes and gaps. Desk research on logged-out search found that hashtag pages very likely need a login since October 2024 (a third-party source). Wrote the plan, the owner's recording steps and this page. Docs-only draft PR [#65](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/65).
