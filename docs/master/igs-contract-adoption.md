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
| 1.1 to 1.11 (slice 1) | #112 (merge 0c7e102) | See below |
| 2.1 and 2.2 (slice 2) | #113 (merge dc79342) | See "Slice 2, as built" |
| 3.1 and 3.3 (slice 3) | #115 (merge f2c40b8) | See "Slice 3, as built"; 3.2 waits for the owner's check |
| 4.2 (slice 4) | #117 | See "Slice 4, as built" |
| 4.4 (slice 4) | the extras PR | See "Slice 4, as built" |

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

**Slice 2, as built** (`internal/ig/normalize/media.go`, `internal/ig/media`):
- **2.1, `content.media.v1`:** every `ig.media.v1` record becomes a Media keyed by its ID, `ig:media:<uuid>` (igs's UUIDv5 of pk and variant, stable across reads).
  - **Type and role:** photo and video `CONTENT`; reel `VIDEO`; thumbnail `THUMBNAIL`; cover `COVER`; profile picture `PROFILE_PICTURE`; comment photo, GIF, video and sticker `ATTACHMENT`.
  - **Owner:** the post (a carousel child's parent, with platform kind `carousel_child`), the comment, or the account.
  - **Also carried:** size, duration, MIME type, alt text, and the signed CDN URL with its expiry.
  - The URL and expiry are left out of the content hash, because Instagram re-signs them on every read.
  - Highlight media waits for slice 3.
- **2.2, the hand-off (`securebrand.media.v1`):**
  - **Announcing:** igs-media announces each stored file on `content.media.ready.v1`. The announcement carries:
    - the owner and its account;
    - the object, with its hex SHA-256;
    - `stored_at` and `delete_after`;
    - `required_consumers` (`sb-analysis`);
    - provenance;
    - `attempt`.
  - **Acknowledgements:** the janitor reads acks on `content.media.processed.v1`.
    - It skips other collectors' media, which share the topic.
    - It deletes a file once every required consumer has processed it, failed it for good, or skipped it.
    - After a retryable failure it announces the file again, at most 3 times.
  - **Deletions:** recorded on `content.media.deleted.v1`, as `PROCESSED` or `EXPIRED`.
  - **Old topics:** igs no longer uses `ig.media.ready`, `processed` or `deleted`. No other service read them, and igs deletes no topic.
- **The deadline:** the contract gives every file one, at least 24 h after storing and 72 h by default (owner R5).
  - `media.ttl` now defaults to 72 h. 0 means 72 h, and anything under 24 h counts as 24 h, so old values that still say `0s` keep working.
  - `media.required_consumers` defaults to `sb-analysis`, the registry's name.
  - The unused presign settings are gone.
  - Media downloads are off in production (`include_media`), so none of this acts there until analysis exists.
- **Evidence:**
  - Unit tests: every kind's type, role and owner, with conformance; the hash under a re-signed URL; the lifetime rule.
  - End to end: `TestMediaPipeline` crawls fakeig with media on. Every stored file is announced, the announcement passes production conformance with a 72 h deadline, and another platform's ack is ignored. Every deletion is recorded and conformant.
  - End to end: `TestContentEventsMatchStore` now also checks `content.media.v1`.

**Fix after slice 2:** a post's `audio.audio_id` was Instagram's bare audio ID. The contract wants a contract ID, `ig:audio:<native id>` (`post.proto`, AudioRef), so it now is. Conformance doesn't check this field. The fix landed before slice 1 was pinned in production, so the store never held the bare form.

**Slice 3, as built** (`internal/ig/normalize/playlist.go`). The master confirmed on 7 Nov, 21:41Z, that Playlist and Audio may be published now: in `registry/topics.yaml` they aren't reserved, and the README's wording is out of date.
- **3.1, `content.playlist.v1`:**
  - One Playlist of type highlight per highlight, keyed `ig:playlist:<id>`, with its account, title (with spans), item count, URL and status.
  - The items are story posts. They're left out until 3.2 publishes them, so the playlist never points at posts that don't exist.
  - The cover is left out until it has a media record. Extras accept only registered keys.
- **3.3, `content.audio.v1`:**
  - One Audio per post sound that has an ID, keyed `ig:audio:<id>`, with title, artist, original or not, URL and status.
  - The post's `audio.audio_id` is that same ID.
  - The contract has no Audio check yet, so the tests check its envelope with `conformance.Envelope` plus a lossless round trip, as tks does.
- **3.2, highlight items as story posts:** waits for the owner's one real read-only check.
- **Evidence:**
  - `TestHighlightToPlaylist`, `TestPostAudio`, and the conformance helper covering both types.
  - End to end: `TestContentEventsMatchStore` checks every record on `content.playlist.v1` and `content.audio.v1`, and expects playlists. fakeig gives reels no sound, so Audio is covered by the unit tests.

**Slice 4, as built so far:**
- **4.2, `collection.coverage.v1`** (`internal/ig/scheduler/jobcoverage.go`): when a job ends, the finalizer publishes its JobCoverage beside `ig.job.v1`, keyed by the job ID. It carries:
  - kind (scheduled, backfill, on demand) and status;
  - the account as `ig:account:<pk>` and the reference the job was asked for;
  - the counters: the contract's shared names as they are, and igs's own as `ig.<name>`;
  - completeness, with the ratios only when posts were read;
  - the missing data as reason classes. Walled posts are `wall`, login pages `login_required`, private profiles `private`, missing profiles and posts `removed`, cut-short timelines and caps `limit`, and blocks `rate_limited`.
  - The contract has no JobCoverage check, so the tests check its fields.
- **4.4, account extras:**
  - The profile parser now reads `latest_reel_media` (the newest story's time) and `show_text_post_app_badge` (Instagram's Threads hint) into igs's Profile, as `latest_story_at` and `threads_badge` (additive fields).
  - The normalizer publishes them as the registered extras `ig.latest_story_at` (RFC 3339) and `ig.threads_badge`, only when the answer has them.
  - The badge's label holds a handle, so it isn't kept.
  - The golden profiles show both.
- **Headers fix:** igs-media's hand-off records (`content.media.ready.v1` and `deleted`) had no `sb-type` or `sb-contract` headers. They have them now, from the same helper as the normalizer's (`normalize.Headers`). Media downloads are off in production, so none went out without them.
- **Evidence:** `TestJobCoverage`. End to end: `TestPostCoverage` checks a backfill's coverage record (key, headers, kind, status, account, counters, completeness), and `TestMediaPipeline` checks the hand-off headers.

