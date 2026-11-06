# igs contract notes

By the igs session, 5 November 2026 (Iraq time), from `Abdullah95k/securebrand-microservices-igs` `main` (`d50f3ee`).

**About the comparisons in this file:** I compared against analysis's draft only as `05-unified-schema-plan.md` §1 describes it. The analysis repository is outside this session's access, so **I haven't read analysis's `content.proto`**. Where a point depends on its exact fields, I say so.

---

## 1. Who designed igs's `content.*`, from what, and what uses it today

**Who and when.** The igs session (this one) designed it on 4 November 2026, in milestone M12f, [PR #54](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/54). The owner had asked igs to "fit with the shared architecture" after the production-readiness review.

**From what:** the architecture document the owner shared, the same text as `09-architecture-v1.md`:
- the envelope list in §5;
- the topic names `content.post`, `comment`, `account`, `metrics` and `media` in §5;
- the ClickHouse tables in §6;
- the entity rules in §7 (bio, website and all profile links from day one).

I didn't see analysis's draft. The proto says so in its header: it is "meant to move [to the shared schema repository] unchanged (and to be reconciled with the analysis work's `content.item.v1` first)".

**Where it lives:**
- `api/proto/securebrand/content/v1/content.proto`: package `securebrand.content.v1`, messages `Envelope`, `Post`, `Author`, `Comment`, `Link`, `Account`, `Metrics` and `Media`;
- the mapping, `internal/ig/normalize/normalize.go` (about 340 lines);
- the store, `migrations/clickhouse/0001_content.sql` and `internal/content/clickhouse`.

**What consumes it today:**
- `igs-content-writer` (`content.*` to ClickHouse), which is **off** in the chart until ClickHouse exists;
- igs's own tests: the normalizer's unit tests, e2e `TestContentEventsMatchStore`, `TestContentStoreMatchesPostgres` and `TestStoreRoundTrip`.

Nothing outside igs consumes it, nothing is deployed, and **no Schema Registry has its subjects**. So igs's side can still change freely; no other service's data depends on it.

**One risk to fix before any deployment.**
- `igsctl schemas apply` registers subjects for the five `content.*` topics as well as the `ig.*` ones.
- The chart runs it when `hooks.schemas` is on: off by default, **on in `deploy/helm/igs/examples/production.yaml`**.
- Following the standing rule, igs must not register `securebrand.content.*` in a shared registry before the master publishes v1.
- Proposed fix (not done; it needs the owner's approval like any change):
  - leave the `content.*` subjects out of `schemas apply` unless asked by a flag;
  - set `hooks.schemas: false` in the production example until v1.

---

## 2. How igs's `content.*` relates to analysis's `ContentItem`

**In the code: they are unrelated.**
- igs never produces or consumes `content.item.v1` or any `ContentItem`.
- The only mention is the proto's header comment above.
- `05-unified-schema-plan.md` §1 lists `content.item.v1` among igs's topics. **That's not correct:** igs has no such topic, in `configs/ig/topics.yaml` or anywhere else.

**In shape, from the pack's description of `ContentItem`:**

| | analysis's `ContentItem` (draft A) | igs's draft (B) |
|---|---|---|
| Messages | One message for posts, comments and replies, told apart by `ContentKind` | `Post` and `Comment` apart, plus `Account`, `Metrics` and `Media` |
| Counts | `Engagement` inside the item | Counts on the item, plus a `Metrics` event per observation (a series) |
| Accounts | A `source_id` reference; no account entity is described | `Account` with bio, website, links, verified, private and counts |
| IDs | `content_id` = `<platform>:<kind>:<native id>`; `source_id`; `thread_id`; `parent_id` | `Envelope.native_id` (text) and `platform` ("instagram"). The Kafka key is `instagram:<native id>`, with no kind. References are bare native IDs (`account_id`, `post_id`, `parent_comment_id`) |
| Media | `MediaRef` in the item; `MediaReady`, `MediaProcessed` | `content.media.v1` per media item. Ready and processed are igs's own `ig.media.ready.v1` and `ig.media.processed.v1` (`securebrand.common.v1`) |
| Author | Has an `AuthorType` | Author has ID, name, URL, picture, verified and `author_key`, but **no type**: Instagram serves none with comments |
| Mentions | `Mention` messages | Hashtags and mentions as name lists in `extras_json` (Instagram serves no positions) |
| Package | `securebrand.content.v1` | `securebrand.content.v1`. **The same name, and incompatible** (as 05 says) |

**Mapping.** igs's `Post` and `Comment` can be turned into `ContentItem` items without loss for analysis's purposes:
- `Post` becomes a `ContentItem` of kind post;
- `Comment` becomes kind comment, with `thread_id` = `parent_id` = the post;
- the latest counts become `Engagement`.

The account, the metrics series and media metadata have no place in `ContentItem`. They'd stay on their own topics. This is the adapter 05 §2.3 describes.

---

## 3. `05-unified-schema-plan.md` §2: what I agree with, what I'd change, and the cost

### Agree

1. **Separate entity messages and topics with one shared envelope (§2.1).** That's what igs built. It matches posts and comments in separate tables, a comment always having a post, and counts changing far more often than content. On Instagram, the reels tab gives views on every visit, while captions rarely change.
2. **`discovery.candidate.v1` and `collection.coverage.v1`.** igs has both kinds of data today: discovery tables and `ig.job.v1` completeness. It publishes neither on `content.*` (costs below).
3. **`content.media.ready.v1` and `content.media.processed.v1` shared across platforms.** igs's `ig.media.ready.v1` and `ig.media.processed.v1` already use the platform-neutral `securebrand.common.v1` messages, so moving them is a topic change.
4. **Native IDs as text, with a 64-bit hash for joins (§2.2).** igs's ClickHouse schema already does this.
5. **A `ContentItem` adapter during migration (§2.3).** I'd put it on analysis's side: one adapter reading the shared topics, not one per collector.
6. **One shared contract repository with a conformance kit (§2.4).** igs's `content.proto` moves there, and igs then imports its generated Go code.

### What I'd change or settle

1. **Pick one platform code.**
   - 05 says `ig`, but 09 §5 (which igs followed) says `instagram`.
   - I recommend 05's short codes (`fb`, `ig`, `tt`, `yt`), with 09 corrected.
   - igs would move from `instagram` to `ig` (cost below).
2. **The `<kind>` in `<platform>:<kind>:<native id>` should be the entity kind, never a subtype.**
   - The entity kinds are `account`, `post`, `comment` and `media`. A subtype would be `reel`, `video` or `highlight`.
   - igs works out a post's kind from Instagram's product and media type. If the subtype were in the ID, a parser fix or a change on Instagram's side would change the ID of the same post.
   - The subtype belongs in `Post.kind`, with the platform's own subtype in `extras`.
3. **Account counts: take the plan's `content.account_snapshot.v1`.**
   - igs sends them today as `content.metrics.v1` with `target = account`. Its ClickHouse writer already routes those rows to `account_snapshots`.
   - A separate topic is clearer, and the move is small for igs.
4. **Add a "hidden by the owner" flag next to the counts.**
   - Instagram owners can hide likes. Instagram then answers 0 to 3, and igs leaves the likes empty with `counts_hidden`.
   - The plan has "approximate", but nothing for "hidden on purpose".
   - Without the flag, a dashboard can't tell "not shown" from "not collected".
5. **Instagram highlight items: `kind = story` with the subtype `highlight` in `extras`.**
   - The owner asked for highlights as posts. 09 §6 says "a story is a post with `kind = story`".
   - igs uses `kind = highlight` today; no consumer exists yet. Changing the value now keeps the plan's kind list short.
   - **It is a change to a value; I'll make it only if the owner and master agree.**
6. **Links need a platform and an ID.**
   - For entity linking, a profile link should be `{url, title, platform, platform_id}`.
   - Instagram gives the linked Facebook page as a name and a page ID, which aren't a URL. igs today drops the ID and puts the name in `Link.url`. That's a defect, and the fix is planned.
7. **Envelope: keep the plan's list, and allow several jobs.**
   - The same post can be read by a scheduled run and an on-demand job at once, so igs's `Observation` carries `job_ids` (a list).
   - I'd make the envelope field `job_ids`, repeated.
8. **Version.** igs uses the observation time in microseconds: it increases with every observation, not only every change. That satisfies "the newest wins" and is safe across pods. I'd write the rule as "increases; the highest wins", not "+1 per change", which needs a counter shared by every pod.
9. **The "replies collected" coverage should sit on the post, not on each comment.**
   - Instagram serves no replies logged out, which is true of every comment of every post.
   - A per-post coverage record (in `collection.coverage.v1`) says it once, with the reason, instead of on every comment.

### The cost for igs to adopt the plan

These are estimates of igs session time, with tests. Each is an addition, except where noted.

| Change | Effort |
|---|---|
| Envelope: add `kind`, `id`, `parser_version`, `job_ids` and `content_hash`. igs has them all on `ig.*` already | 0.5 day |
| Platform code `instagram` to `ig`, and IDs to `ig:<kind>:<pk>`, in the normalizer, the Kafka keys and the ClickHouse writer. **A change of values**, but harmless: no consumer and no stored data exist | 0.5 day |
| Promote igs's `extras` to the plan's fields: hashtags, mentions, location, music, duration, pinned, AI label, tagged accounts, comments disabled, media IDs in order, post status with "first seen missing", edited flag, account URL and status, picture expiry, media kind, variant and MIME | 1 to 1.5 days |
| `content.account_snapshot.v1` | 0.5 day |
| `discovery.candidate.v1`, from `discovery_candidates`, `discovery_vets` and `discovery_sources` | 1 to 2 days |
| `collection.coverage.v1`, from the job's counters, completeness and truncation reasons, plus per-post coverage | 1 day |
| `content.media.ready.v1` and `content.media.processed.v1`, published beside igs's own during the switch | 0.5 day |
| Fix the linked Facebook page (keep the ID; a proper link) | 0.5 day |
| Move the proto to the contract repository; import the generated code; run the conformance kit in CI; remove igs's copy of `securebrand.content.v1` | 1 day |
| **Total** | **About 7 to 8 days** |

No data migration is needed, because nothing is deployed. The `ig.*` platform topics stay as they are.

---

## 4. igs fields with no place in the plan

These are fields igs collects today that §4 of the plan doesn't list. My proposal for each:

| igs field | Proposal |
|---|---|
| `counts_hidden` (likes hidden by the owner) | A shared flag on post and metrics (point 4 above) |
| `topics`: Instagram's related topics, on 89% of post pages | `extras.topics`. Promote later if TikTok or YouTube have topics |
| `shortcode`: a post's second ID, in its URL | `extras.shortcode` (the URL carries it too) |
| `product_type` and `media_type` (feed, clips, carousel; photo, video, carousel) | `extras`: the "platform subtype" the plan allows |
| `usertags` positions (x, y) and which carousel item they're on | `extras`; the plan's "tagged accounts" keeps the IDs |
| `music.original_audio`: the post's own sound, not a licensed track | An `original` flag on the audio field, or `extras` |
| `accessibility_caption` on the post (Instagram's automatic description) | Already fits `Media.alt_text` per media item. The post-level copy goes to `extras` |
| `discovered_via` (grid, reels) | `extras` |
| `pronouns` (account) | `extras` (personal data) |
| `highlight_count`; highlight ID, title and cover | Highlights are containers, like a playlist. The plan has no playlist entity. Proposal: `extras` on each highlight item (`highlight_id`, `highlight_title`), and the cover as account media |
| `requested_ref` (what the client registered) | Collector-internal; `extras` or nothing |
| `author_key` on the account (the same HMAC as when the account comments) | Add to `Account`, so an account and its comments link without the raw ID when pseudonymized |
| Linked Facebook page (name and ID) | A `Link` with `platform = fb` and `platform_id` (point 6 above) |
| Job counters: `wire_bytes`, `page_loads`, `blocked`, `posts_gated`, `drift` | `collection.coverage.v1`: errors and truncations by class |
| Discovery sightings by kind, the Iraqi-audience sources, the vet read's captions | `discovery.candidate.v1` evidence: the plan's "evidence" and "signals" fit if they allow a list of sources |
| Comment `child_comment_count` without the replies themselves | Fits `Metrics.replies`. The coverage reason says why there are no reply items |
| `edit_count` on comments, and the replaced texts (`post_edits`, `comment_edits`) | The plan's rule 7 (a new version per edit) covers it in the stream. The old texts are in igs's Postgres; whether ClickHouse needs an edits table is the master's call |

Fields the plan has that **Instagram doesn't give igs logged out**:
- account category, created-at, country, language and location;
- shares, saves, reposts and quotes;
- views on photos and carousels;
- reply items;
- comment authors' display name and type;
- paid-partnership flag (not seen);
- members-only and title or description.

They stay absent, which means unknown.

---

## REQ-003: answers to the master session (5 November 2026)

Facts only, from `main` at `8377774`. Nothing here was built or changed.

### 1. The ClickHouse writer and schema

`igs-content-writer` is `internal/content/clickhouse`: `consumer.go`, `writer.go`, `rows.go`, `migrate.go`, about 720 lines. The schema is `migrations/clickhouse/0001_content.sql`.

**Before reusing them, you should know:**

- **They are almost platform-neutral.**
  - The package imports only the content protos, plus igs's `topics` package for the list of `content.*` topic names.
  - Platform, IDs and versions all come from the envelope.
  - The one igs-specific thing to remove is that topics import.
- **Batches.**
  - Up to 5,000 records or 1 s per batch, with one native `INSERT` per table, in a fixed order.
  - The Kafka offsets are committed only after every table's insert succeeded.
- **A retried batch inserts again into the tables that already succeeded.**
  - Inserts aren't transactional across tables. When table 3 of 5 fails, tables 1 and 2 are written again on the retry, and again after a restart before the commit.
  - Today that's harmless only because every table is a `ReplacingMergeTree` whose sort key collapses an identical row:
    - entities on `(platform, id)` with `version`;
    - series on `(platform, id, observed_at)`.
  - **If D-004 (keep every version) moves the entity tables to a plain `MergeTree`, every retry and every redelivery becomes a duplicate row.**
  - See question 2 for a shape that keeps every version and still collapses redeliveries.
- **The version is the observation time in microseconds.** It is not a change counter. Every re-read is a new version, even when nothing changed:
  - accounts are read daily;
  - posts are read at their first read, then about 5 to 10 times in their first 7 days;
  - comments are read on every re-read of their post's first comments page.

  If every version is kept, rows grow with visits, not with changes. The `ig.*` events carry a `content_hash`, but the `content.*` envelope doesn't yet (`igs-contract-notes.md` §3). With it, the writer could skip a version whose hash equals the previous one.
- **The series are large.** `post_metrics` gets one row per reel per visit, because igs re-reads the reels tab a year back on every visit. That's 0.8 to 8 million rows a day per 1,000 accounts, most of them unchanged (`igs-operations.md` §4). A fix is proposed (`igs-gaps.md`, "the reels window").
- **Partitions.**
  - Posts and comments are partitioned by `toYYYYMM(published_at)`; a missing `published_at` falls back to the observation time (`rows.go`, `at()`).
  - Series are partitioned by `observed_at`.
  - `accounts`, `authors` and `media` aren't partitioned.
- **Clusters and tiers.**
  - `igsctl clickhouse migrate` tracks applied files in a `schema_migrations` table.
  - With `clickhouse.cluster` set, it creates tables `ON CLUSTER` with `Replicated*` engines.
  - `storage_policy` and `cold_after` add a TTL that moves old partitions to a `cold` volume.
  - None of this has run against a real cluster.
- **What has been tested.**
  - It ran only against CI's single ClickHouse 25.8 server: `TestStoreRoundTrip`, and e2e `TestContentStoreMatchesPostgres`.
  - **It has never been loaded at volume, and never deployed.** The chart component is off.
- **Columns the plan wants that the schema lacks:**
  - `kind`, `parser_version`, `content_hash` and `job_ids` from the envelope;
  - post status with "first seen missing";
  - media `kind`, `variant` and `mime`;
  - links with a platform and ID: links are two parallel arrays today, `link_urls` and `link_titles`.

  Also, `verified`, `is_private` and `is_business` are `UInt8`, not nullable, so unknown reads as 0.
- **`entities` and `entity_accounts`** are created, but nothing fills them.

**Why a failed insert is retried forever, with no dead letter:**
- It was a deliberate choice for one case. The rows are typed by the protos and checked by the normalizer, so I expected a failed insert to mean the store was unavailable: network, server down, schema not migrated, too many parts.
- Skipping would lose content for good. Retrying loses nothing, and Kafka's 7-day retention holds the backlog. `IgsContentWriterLag` alerts on it.
- **The weakness is a "poison" row that ClickHouse rejects every time**, for example a value it can't parse, or a schema mismatch after a bad migration. That row blocks the writer forever, every 2 s, and no later record is written.
- While it retries, the consumer also **holds back the group's rebalances** (`BlockRebalanceOnPoll`), so a new pod can't take its partitions.
- **Records that can't be decoded are the opposite: skipped, with no dead letter.** They are logged and counted (`igs_content_written_total{outcome="undecodable"}`), and their offsets are committed, so they're lost.

**For the platform writer, I'd change it to this:**
- sort insert errors into retryable (network, timeouts, memory, too many parts) and not retryable (parse, type and schema errors, by ClickHouse error code);
- retry the first kind with exponential backoff, capped, and allow rebalances between tries;
- for the second kind, split the batch to isolate the bad records, write them to a dead-letter topic with the error, and go on;
- send undecodable records to the same dead-letter topic;
- add an alert on the dead letter growing.

That's about 1 day in igs's code. I haven't built it.

### 2. Does igs rely on old versions disappearing?

**Yes, in the ClickHouse store.** Nowhere else.

- `posts`, `comments`, `accounts` and `media` are `ReplacingMergeTree(version, is_deleted)`, sorted by the entity's ID. `authors` is `ReplacingMergeTree(version)`.
- So merges keep only the newest version. Reads are meant to use `FINAL`, or `argMax` by version; a row marked deleted is hidden by `FINAL`.
- The tests assert exactly that (`store_test.go`): the edited text wins, the deleted comment is gone from `FINAL`, and a redelivered batch adds nothing.
- The series tables (`post_metrics`, `comment_metrics`, `account_snapshots`) keep every observation. They only collapse an identical redelivery, because the sort key includes `observed_at`.
- **Nothing else depends on old versions disappearing:**
  - igs's API reads Postgres, not ClickHouse.
  - Postgres keeps edit history in `post_edits` and `comment_edits`, and changes in the metrics and snapshot tables.
  - Kafka keeps every event for 7 days.

**For D-004 (keep every version):**
- Sort the entity tables by `(platform, id, version)`, as `ReplacingMergeTree` without a version argument, or with the event ID in the key. Every distinct version is then kept, and an identical redelivery still collapses.
- Read "the current row" with `argMax(…, version)`, or a materialized "latest" table beside it.
- Deletions become a version with `is_deleted = 1`, never a removal.
- Note that `OPTIMIZE … FINAL CLEANUP` would drop deleted rows, so don't run it.
- Expect the row growth described under question 1, unless unchanged versions are skipped by `content_hash`.

### 3. Private accounts found by discovery

They stay in igs's own Postgres:

- `ig.discovery_candidates`, with `status = 'private'` (the "second list", R8);
- each vet read and its score are in `ig.discovery_vets`;
- the API can list them with `GET /v1/discovery/candidates?status=private` (admin scope).

They are **not registered, not crawled, and not published** to Kafka or anywhere else.

A private account is marked `private` whatever its Iraq score (`internal/ig/discovery/vetter.go`, the decision `switch`). The score is still stored with it.

It is vetted again after `ig.discovery.revet_after` (720 h, 30 days), if it has been seen again since the decision. If it has become public and scores at or above the threshold, it's registered then.

When `discovery.candidate.v1` exists, `private` maps to the plan's outcome "second list (private)".

### 4. `UPSTREAM.md`: is the empty "Ported from fbs" table accurate?

**No.** Two ports from fbs are missing from it:

| Date | fbs commit | What | igs PR |
|---|---|---|---|
| 2026-11-04 | Not recorded (fbs `main` as of that day; this session can't read fbs's repository to check) | The Iraq place seed `configs/discovery/iraq.yaml`, unchanged, and the spelling fold `Normalize` and seed parser from fbs's `internal/discovery`, unchanged, into `internal/keywords` | [#58](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/58) (M12i) |
| 2026-11-05 | The merge of [Abdullah95k/securebrand-microservices#49](https://github.com/Abdullah95k/securebrand-microservices/pull/49) | fbs's new `iraq.yaml`, which now includes igs's spellings, copied over igs's | [#62](https://github.com/Abdullah95k/securebrand-microservices-igs/pull/62) |

Nothing else was ported. The neutral code igs kept from the seed (`internal/scrape/*`, `internal/crawl/*`, `internal/platform/*`) is fbs's code as of `f1377a9`, which `UPSTREAM.md` already records. No fbs change has been brought in since. The two rows are now in `UPSTREAM.md` (the same PR as this file).

### 5. The wire format and names

**The `igs-type` header.**
- The producer sets it on every record to the full protobuf message name, for example `securebrand.ig.v1.Post` or `securebrand.content.v1.Post` (`internal/platform/kafka/producer.go`).
- **No consumer decides anything from it.** The persister, normalizer and content-writer all decode by topic.
- It's only carried along: retries and dead letters copy it (`internal/ig/stage/worker.go`, `internal/ig/dlq/dlq.go`), and one test checks that a replayed dead letter keeps it.
- The Schema Registry is used only for compatibility checks. Records are plain protobuf, not registry-framed.
- So renaming the header, to `type` or whatever the contract fixes, is a one-line change plus that test.
- fbs's copy of the same code uses `fbs-type`.

**The platform code `instagram`:**

| Where | Value | Depends on it |
|---|---|---|
| `content.*` envelope `platform` | `instagram` (`normalize.Platform`) | ClickHouse `platform` column and the `cityHash64(platform, id)` key columns, which are `MATERIALIZED` and so recomputed automatically |
| Kafka key of every `content.*` record | `instagram:<native id>`, with no kind; a comment's key is its post | Partitioning only |
| `ig.*` events, `ig.job.v1` and the media handoff | The `securebrand.common.v1.Platform` enum, `PLATFORM_INSTAGRAM` | Not a string |
| Media files in S3 | Prefix `ig/` | The janitor's lifecycle rule (prefix `ig/`) |
| Tests | Normalizer, ClickHouse store and e2e tests assert `instagram` | - |

**Nothing is deployed or stored anywhere**, so changing to `ig` and to `ig:<kind>:<native id>` keys costs only the normalizer, its key function and the tests. That's about half a day (`igs-contract-notes.md` §3).
