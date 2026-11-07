# igs adopts contract v1.2.0 (REQ-019)

**Who and when:** the igs session, from 7 November 2026, Iraq time.

**The go:** the owner said "start the contract work" on 7 Nov, at about 23:10 Iraq time. The master relayed it, and it approves REQ-019 to build now. The pilot comes first.

**The contract:** `github.com/abdullah95k/securebrand-contracts` at **v1.2.0** (6 Nov 2026).
- v1.2.0 is v1.0.0 plus additions: v1.1.0 added discovery fields and keyword IDs, and v1.2.0 added conformance for every published type.
- It is what the platform's writer decodes, and what tks already publishes.
- `sb-contract` carries `contracts.Version`: `1.2.0`.

**Why now:** the writer started reading `content.*` on 7 Nov at 19:10Z and rejected every igs record.
- igs published its own draft `securebrand.content.v1`, written before the contract existed. It has the same package and topic names, but different field numbers, and no `sb-type` or `sb-contract` headers.
- The normalizer has been at 0 since then, so igs publishes no `content.*`.
- Nothing is lost:
  - igs's store has everything;
  - the `ig.*` topics keep 7 days;
  - igs's own ClickHouse writer was off.

This file tracks each item of REQ-019 with its PR and evidence, as REQ-019's "done when" asks.

## The plan

### Slice 1: the core content (Account, AccountSnapshot, Post, Comment, Metrics)

The data the owner wants in ClickHouse first. About **4 to 5 days**.

| # | Item | Notes |
|---|---|---|
| 1.1 | Depend on the contract module at v1.2.0. Delete igs's own `securebrand.content.v1` (proto and generated code) | Two packages of that name can't be linked into one program (README, "Adopting it") |
| 1.2 | Retire igs's own ClickHouse writer and schema (`igs-content-writer`, `internal/content/clickhouse`, `igsctl clickhouse migrate`, `migrations/clickhouse`) | It decodes igs's draft. It is off in production, and the platform's writer does its job. Removing it is an igs-internal change: nothing else uses it |
| 1.3 | The envelope | `platform` `ig`; `kind`; `id` `ig:<kind>:<native id>`; `version` = `observed_at` µs; `content_hash` = SHA-256 of the content fields without counts; `event_id` = SHA-256 of `id\|observed_at µs\|content_hash`; `partial` for deletions |
| 1.4 | Provenance | Collector `igs` and version; `parser_version` `<query>:<n>`; `source_operation` (the query's friendly name) and `source_variant` (the doc_id); job IDs; `SCRAPED_LOGGED_OUT`; the crawl mode; `raw_ref`; `exit_country` from the exit's tier |
| 1.5 | Account and AccountSnapshot | Account: handle, name, bio as text with spans, links (bio links and the linked Facebook page: `LINK_SOURCE_LINKED_ACCOUNT`, `target_platform` `fb`, URL `https://www.facebook.com/<id>`), verified, private as status, scope. Counts go to AccountSnapshot. Type and `is_business` stay unset (REQ-019 item 3) |
| 1.6 | Post | Type and subtype by the README's table (photo, carousel, video or igtv, reel as `SHORT_VIDEO`). Caption as text with spans. `published` from `taken_at`, `EXACT`. `account` as seen. Location, usertags and co-authors as `AccountRef`. `ai_generated` only with a label. Status: walled posts `RESTRICTED`, reason `login_wall`, counts unset (E10). `ItemCoverage` with `replies_available = false` |
| 1.7 | Comment | Parent and root (one level logged out). Text with spans. Author as `AccountRef` (scope `NOT_MONITORED`). Time from the pk, `MINUTE`. Deletion from a complete re-read as a partial `DELETED` event. `flags.edited` unset (versions show edits) |
| 1.8 | Metrics | Post counts (likes, comments, views on reels, `hidden` for hidden likes) and comment counts (likes, replies), keyed by their post. Post-page counts for photos and carousels. The volume rule (a count changed, or once a day per refreshed item), with its memory in Valkey |
| 1.9 | On the wire | Plain Protobuf, one type per topic. Headers `sb-type` (full name) and `sb-contract` (`contracts.Version`). Keys as in `registry/topics.yaml`: account ID; post ID for posts, comments and metrics |
| 1.10 | Conformance | Every message the normalizer emits passes `conformance.<Kind>(m, Options{Production: true})`: in its unit tests, on the contract's Instagram samples, and in the end-to-end test against fakeig |
| 1.11 | Re-emit | A new normalizer consumer group reads the `ig.*` topics from their start (7 days kept) and publishes everything since the pilot began, once. Event IDs repeat for the same observation, so a second pass adds nothing. igs's store covers anything older, but there is none yet |
| 1.12 | Platform | The master pins it, sets the normalizer back to its size, and checks `topic-peek` and `writer-stats` |

### Slice 2: media (Media, the hand-off). About 2 days.

| # | Item |
|---|---|
| 2.1 | `content.media.v1` (Media) for post media |
| 2.2 | The hand-off on `content.media.ready.v1`, `processed`, `deleted`: `delete_after` at least 24 hours after storing (72 by default), consumer `sb-analysis`, `attempt`, at most 3 announcements. This replaces `ig.media.*` for analysis; the `ig.media.*` topics stay |

### Slice 3: highlights and audio. About 1.5 days.

| # | Item |
|---|---|
| 3.1 | Playlist per highlight (title, cover, URL) |
| 3.2 | Highlight items as `POST_TYPE_STORY`, subtype `highlight`: only after one real read-only check that logged-out Instagram serves them. The owner runs it when I give the command |
| 3.3 | Audio (a reel's music), from the post page |

### Slice 4: discovery, coverage, requests. About 4 days.

| # | Item |
|---|---|
| 4.1 | `discovery.candidate.v1`: Instagram candidates with outcomes; cross-platform candidates from bio links; keyword IDs with `contracts.KeywordID`; mentions resolved only within the same post |
| 4.2 | `collection.coverage.v1` (JobCoverage) per job |
| 4.3 | A `registry.account_request.v1` consumer: add, update, pause, resume, collect now |
| 4.4 | Extras `ig.latest_story_at` and `ig.threads_badge` |

**Total: about 11.5 to 12.5 days.** That's within my estimate against v0.1 (11 to 14 days). v1.1.0 and v1.2.0 added only fields and checks: `Post.account`, `Comment.url` (no logged-out source on Instagram, so unset), the "not monitored" scope, keyword IDs, and `content_time` on discovery evidence.

**Done when (REQ-019):**
- every kind passes conformance end to end in CI;
- the fakeig run produces each kind at least once, including a deletion (a partial event) and an estimate;
- this file lists each item with its PR and evidence.

## Progress

| Item | PR | Evidence |
|---|---|---|
| Plan and re-estimate | this file | v1.2.0 read on 7 Nov; the contract's Go module resolves at v1.2.0 |
| 1.1 to 1.11 (slice 1) | the slice 1 PR | See below |

**Slice 1, as built** (`internal/ig/normalize`, `internal/ig/app/normalizer.go`):
- **1.1:** `go.mod` requires `securebrand-contracts v1.2.0`. igs's own `securebrand.content.v1` (proto, generated code, embed) is deleted.
- **1.2:** `igs-content-writer`, its consumer and row code, its chart component and `IgsContentWriterLag` are gone, and so is CI's ClickHouse service. The master confirmed (7 Nov, 20:40Z) that igs's own tables were never created in production and nothing reads them, so `migrations/clickhouse`, `igsctl clickhouse migrate`, the `clickhouse:` config section and the hook are gone too. Old values that still set `clickhouse:` are ignored.
- **1.3 and 1.4:** the envelope and provenance as planned. **Not yet set:** `exit_country` and the account's `scope`. igs's `ig.*` events carry neither the exit nor the registration. Both are additions to igs's own events, planned with slice 4 (the registry consumer). Conformance allows them unset.
- **1.5:** Account and AccountSnapshot as planned, plus extras `ig.pronouns`, `ig.requested_ref` and `ig.highlight_count`, and the country signals as `relevance.matched_rules`. A missing account is a partial `UNAVAILABLE` event (reason `no_user`).
- **1.6:** Post as planned. A walled post is a partial `RESTRICTED` event, reason `login_wall`; any other unavailable post is `UNAVAILABLE`. Hidden likes are `Count{hidden: true}`.
- **1.7:** Comment as planned. Instagram serves each comment's `created_at`, so its time is `EXACT`, not derived from the pk. A redacted (erased) author's comment publishes nothing. An author pseudonymised for privacy is `hidden`.
- **1.8:** Metrics for posts and comments, keyed by the post. The volume rule's memory is in Valkey (`igs:content:metrics:<id>`, `SET … GET`, 48 h). If Valkey fails, the counts are published: a repeat costs a row, a dropped change loses a count.
- **1.9:** headers `sb-type` (`proto.MessageName`) and `sb-contract` (`contracts.Version`). Keys from `registry/topics.yaml`.
- **1.10:** every message passes `conformance.<Kind>(m, Options{Production: true})`:
  - in `normalize_test.go`: Account, AccountSnapshot, every post type, walled and unavailable posts, comments, deletions, metrics, event IDs, spans, the volume rule;
  - end to end: `TestContentEventsMatchStore` crawls fakeig. Every record on the five topics carries both headers and passes conformance. Posts, comments and accounts match Postgres, and snapshots and metrics are there.
- **1.11:** the normalizer's group is now `igs-ig-normalizer-c1`, which starts at the earliest offset. When it starts, it publishes the 7 days the `ig.*` topics hold, once.
- **1.12:** the platform's step, below.

**For the platform (1.12):**
1. `content.account_snapshot.v1` must exist on the shared Kafka before the normalizer starts. igs creates no topics there.
2. Pin the slice 1 commit and set the normalizer back to its chart size.
3. Check `topic-peek` and `writer-stats`.
