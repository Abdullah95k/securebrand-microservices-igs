# igs review of the contract draft v0.1 (REQ-012)

By the igs session, 6 November 2026 (Iraq time).

**What I read:**
- `Abdullah95k/securebrand-contracts`, branch `claude/securebrand-platform-integration-4iiwlr` (commit `3a75c8b`, "Contract draft v0.1 for review"): `README.md`, every proto in `proto/securebrand/`, `docs/field-matrix.md`, `samples/` (the two Instagram samples), `registry/*.yaml` and `conformance/conformance.go`.
- igs's own code at `main` `8377774`.

**Ground rules for this review:**
- Read only. No code was changed for it.
- Examples are synthetic.
- Where I don't know, I say so.

**Short answer.** The draft fits Instagram well. Every field igs collects has a place. The changes I ask for are below; most are small:
- "partial" events and the ClickHouse writer (§7, question 7);
- content hash and event ID semantics (§5);
- an `ig` account type;
- three conformance points (`delete_after`, the precision of times derived from IDs, `owner_kind = playlist`).

The adoption effort for igs rises from my earlier 7 to 8 days to **about 11 to 14 days** (§6), because the draft adds messages and duties my earlier estimate didn't have:
- Audio and Playlist;
- the `AccountRequest` consumer;
- `Candidate` with outcomes;
- the metrics volume rule;
- the raw-response policy in D-004.

---

## 1. Mapping, message by message

**Terms:**
- `P` is the post's pk, `U` a user pk, `C` a comment pk and `M` igs's media UUID.
- IDs are `ig:<entity kind>:<native id>`.
- "Agree" means igs can fill the field as the contract describes it.

### Envelope and provenance (every message)

| igs today (`Observation` on `ig.*`; `Envelope` on its draft `content.*`) | Contract | Verdict |
|---|---|---|
| `platform` "instagram" | `platform` `ig` | **Agree** (igs changes) |
| (none) | `kind` | **Agree** (igs adds) |
| Kafka key `instagram:<native>` | `id` `ig:<kind>:<native>` | **Agree** (igs changes) |
| `native_id` | `native_id` | Agree |
| `observed_at` | `observed_at` | Agree |
| `version` = observation µs | `version` | Agree: the same rule |
| `content_hash`: xxh3-128 over the whole message without its observation or CDN signatures, **counts included** | `content_hash`: hex SHA-256 of content fields, **not counts** | **Agree, igs changes.** The present hash changes when a like count changes, which defeats "a new version only when content changes" |
| `event_id` = hash of entity and content hash: **the same for two observations with the same content** | `event_id`: unique per observation; the same only for a redelivery | **Agree, igs changes.** igs's present ID would make two identical observations a day apart look like one redelivery. It becomes the draft's recommendation, `"<id>\|<observed_at µs>\|<content_hash>"` |
| (none) | `partial` | **Agree**, for deletions and status-only events (see §5 and question 7) |
| `collector` "igs", `collector_version` | `provenance.collector`, `collector_version` | Agree |
| `parser_version` (per query, an integer: profile 1, grid 1, post 1, comments 1, reels 1, highlights 1) | `provenance.parser_version` (string) | **Agree.** igs will send `"<query>:<n>"`, for example `"post_page:1"`. The samples' `"21"` isn't igs's form |
| `query_name`, `doc_id` | `source_operation`, `source_variant` | Agree |
| `job_ids` | `job_ids` | Agree |
| (implied) | `acquisition` = `SCRAPED_LOGGED_OUT` | Agree |
| `mode` (full, incremental, refresh, recrawl) | `mode` | Agree. A raw re-parse tool would add `REPARSE` (`igs-gaps.md`, E8) |
| `raw_ref` | `raw_ref` | Agree |
| (none) | `exit_country` | **Can fill once configured.** igs doesn't record its exits' countries; it needs a `country` per proxy tier in `proxies.yaml` |
| (none) | `terms_expire_at` | Not applicable |

### Account (`content.account.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `user_pk` | `envelope.id` `ig:account:U`, `native_id` | Agree |
| (profile, person or business unknown) | `type` | **Question.** No `AccountType` fits: igs can't tell a person from a business logged out. I propose leaving it `UNSPECIFIED`, documented as "Instagram: not known", rather than guessing `PERSON` |
| `username` | `handle` | Agree |
| `full_name` | `display_name` | Agree (empty when the account sets none) |
| derived `https://www.instagram.com/<username>/` | `url` | Agree |
| `biography` | `bio.text` (no spans; `platform_language` empty) | Agree |
| `bio_links[]` (url, title) | `links[]`: `source = BIO`, `url`, `title`, `domain`; `target_platform` and `target_id` when the URL is a known platform's | **Agree.** igs will add the URL-to-platform parsing (facebook.com, tiktok.com, youtube.com, instagram.com) |
| `linked_fb_info.linked_fb_page` (name; the ID is dropped today) | `links[]`: `source = LINKED_ACCOUNT`, `target_platform = fb`, `target_id` = the page ID, `title` = the name | **Agree** once the ID is kept (question 4) |
| (none served) | `contact`, `categories`, `stated_country`, `platform_language`, `location`, `created_at`, `handle_changed_at`, `name_changed_at`, `cover`, `keywords`, `alt_ids` | Unset (§5) |
| `is_verified` | `verified` | Agree |
| `is_private` | `is_private` | Agree |
| `is_business` (never served; `false` today) | `is_business` | **Agree: unset**, never `false` |
| `profile_pic` | `picture` (a `MediaRef` to the profile picture's `Media`, role `PROFILE_PICTURE`) | Agree |
| `status` (active, private, unavailable, suppressed) | `status.availability`: `VISIBLE`, `PRIVATE`, `UNAVAILABLE` (reason `no_user`) | **Agree.** "No user" covers renamed, deleted and misspelled alike, so `UNAVAILABLE`, not `DELETED`. Suppressed (erasure) is dormant, so no mapping is needed now |
| registered, or the private second list | `scope`: `FULL`, or `PROFILE_ONLY` | Agree |
| `follower_count`, `following_count`, `media_count` | `counts` (`AccountCounts`) and `content.account_snapshot.v1` | Agree (question 1) |
| discovery score, when the account came from discovery | `relevance.score`, `matched_rules`, `matched_keyword_ids` | **Agree.** Keyword IDs once the shared list has stable IDs (open point 1) |
| `pronouns`, `highlight_count`, `requested_ref` | `extras` `ig.pronouns`, `ig.highlight_count`, `ig.requested_ref` | Agree (already in `registry/extras.yaml`) |
| `author_key` | (gone, D-003) | Agree |

### Account snapshot (`content.account_snapshot.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `follower_count`, `following_count`, `media_count` | `counts.followers`, `following`, `posts`, exact | Agree. For a private account, followers and following are served; the post count may be absent, and is then left unset |
| `is_verified`, `is_private` | `verified`, `is_private` | Agree |
| `username`, `full_name` | `handle`, `display_name` | Agree |
| `status` | `status` | Agree |

### Post (`content.post.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `media_pk` | `envelope.id` `ig:post:P` | Agree |
| `owner_pk` | `account_id` `ig:account:U` | Agree |
| `product_type` and `media_type` | `type` and `subtype` | **Agree**, with the full Instagram table in §2 |
| `permalink` | `url` | Agree |
| `taken_at` (exact seconds) | `published_at` (`EXACT`) | Agree. **But** where igs derives the time from the pk (highlight items, posts known only by pk), the time is within about a minute, so the precision should be `MINUTE`, not `EXACT` |
| `caption` | `text.text` (no spans, no language; `truncated` false) | Agree |
| `hashtags` (from the caption) | `hashtags` | Agree |
| `mentions` (usernames from the caption) | `mentions[]` (`AccountRef` with `handle`) | Agree (question 5) |
| `usertags` (pk, username, x, y, child) | `tagged[]` (`AccountRef` `id`, `handle`), plus `extras` `ig.usertag_positions` | Agree |
| `coauthors` (pk, username) | `coauthors[]` | Agree |
| (none) | `links` | Unset: Instagram captions hold no clickable links |
| `location` (pk, name, lat, lng) | `location`: `platform_location_id`, `name`, `latitude`, `longitude` | Agree |
| `music` (title, artist, audio_id, original) | `audio` (`AudioRef`: `ig:audio:<id>`, title, artist, original), and a `content.audio.v1` record | **Agree.** igs can publish `Audio` with title, artist, author, original and its ID; nothing else is served |
| `video_duration_ms`, `has_audio`, `width`, `height` | `video.duration_ms`, `has_audio`, `width`, `height` | Agree. Live status isn't applicable |
| `media_ids` and `children` | `media[]` (`MediaRef`: `ig:media:M`, type, role `CONTENT` or `THUMBNAIL`, position) | Agree |
| `ai_label` | `labels.ai_label_text`, and `labels.ai_generated = true` when present | **Agree, with one rule:** when there's no label, `ai_generated` stays **unset**, not `false`. A missing label doesn't prove a post isn't AI-made |
| (not served) | `labels.paid_partnership`, `ad`, `age_restricted`, `members_only`, `family_safe` | Unset |
| `is_pinned` (inferred) | `flags.pinned` | Agree |
| `commenting_disabled` | `flags.comments_disabled` | Agree |
| `caption_edited` (Instagram's flag; no time) | `flags.edited` (`edited_at` unset) | Agree |
| `topics` | `platform_topics` | Agree |
| highlight item | `playlists[]` (`ig:playlist:<highlight id>`, position) | Agree (question 2) |
| `like_count`, `comment_count`, `counts_hidden` | `counts.likes` (`hidden = true` and no value when hidden), `counts.comments` | Agree (question 3). The post page serves no views, so a post event has no views; reel views come with the reels tab, on `content.metrics.v1` |
| `status` (visible, unavailable) | `status`: `VISIBLE`; gone on a re-read becomes `DELETED` (reason `not_found`, as a partial event); walled becomes `RESTRICTED` (reason `login_wall`) | **Agree.** `DELETED` is defined as "gone, as far as a logged-out reader can tell", which is exactly igs's case. Walled posts publish nothing today; that's fixed with E10 |
| (per-post coverage not written today) | `coverage` (`ItemCoverage`): `comments_state`, `comments_collected`, `comments_reported`, `completeness`, `replies_available = false`, `reason` | **Agree.** igs must write it (E10 in `igs-gaps.md`) |
| `shortcode`, `product_type`, `media_type`, `discovered_via` | `extras` `ig.shortcode`, `ig.product_type`, `ig.media_type`, `ig.discovered_via` | Agree |
| (none) | `title`, `description`, `uploader_tags`, `created_in_country`, `reshare`, `category`, `poll`, `captions` | Unset |

### Comment (`content.comment.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `comment_pk` | `envelope.id` `ig:comment:C` | Agree |
| `media_pk`, `owner_pk` | `post_id` `ig:post:P`, `account_id` | Agree. Kafka key = the post's ID |
| (no replies logged out) | `parent_comment_id`, `root_comment_id` empty, `depth` 0 | Agree |
| author `user_pk`, `username`, `is_verified`, `profile_pic_url` | `author.account`: `id` `ig:account:U`, `handle`, `url`, `verified`, `picture_url`; `type` unset; `display_name` empty | Agree |
| `text` | `text.text` | Agree |
| `created_at` | `published_at` (`EXACT`) | Agree |
| `is_op` | `flags.by_post_owner` | Agree |
| (text change between reads) | `flags.edited` | **Question.** igs's normalizer has no memory of earlier text, so it can't set `edited` without new state. The versions themselves show the edit. I propose leaving it unset for Instagram unless the master needs the flag |
| `comment_like_count`, `child_comment_count` | `counts.likes`, `counts.replies` | Agree |
| (deletion not detected today) | `status` `DELETED`, as a partial event | Agree, once built (E7) |
| `giphy_media_info` (dropped today) | `attachments[]` (`MediaRef`, type `GIF`, role `ATTACHMENT`) | Agree, once built |
| (not parsed) | `flags.pinned`, `liked_by_creator` | Unknown whether Instagram serves them logged out; unset |

### Metrics (`content.metrics.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| reels tab: `play_count`, `like_count`, `comment_count`, `counts_hidden` | `target_kind = post`, `envelope.id` `ig:post:P`, `parent_id` = the account, `counts.views`, `likes` (hidden as above), `comments` | Agree |
| post page re-reads: likes and comments | Same | **Agree, igs adds this.** Today only reels have a metrics series |
| comment likes and replies | `target_kind = comment`, `parent_id` = the post; **key = the post** | **Agree, igs changes the key.** igs's draft keys comment metrics by the comment today |
| (none) | `shares`, `saves`, `reposts`, `quotes`, `reactions` | Unset: not served |

### Media (`content.media.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `media_id` (UUIDv5) | `envelope.id` `ig:media:M` | Agree |
| owner (post; carousel child to its post; comment; profile; highlight) | `owner_id`, `owner_kind` | **Question.** A highlight's cover belongs to the highlight, which is now a `Playlist`. The draft's owner kinds are post, comment, account and audio. I propose adding `playlist` (and conformance accepting it) |
| `kind` (photo, video, reel, thumbnail, cover, profile_pic, highlight) | `type` (`IMAGE` or `VIDEO`), `role` (`CONTENT`, `THUMBNAIL`, `COVER`, `PROFILE_PICTURE`), `platform_kind` (igs's kind, `carousel_child` for children) | Agree; the sample matches |
| `variant`, `position`, `source_media_id`, `source_url`, `source_expires_at`, `width`, `height`, `duration_ms`, `mime`, `alt_text` | The fields of the same names (`platform_media_id`) | Agree |
| `size_bytes` (only once downloaded) | `size_bytes` | Agree |

### Playlist (`content.playlist.v1`): Instagram highlights

| igs field | Contract | Verdict |
|---|---|---|
| `highlight_id` | `envelope.id` `ig:playlist:<id>` | Agree |
| `user_pk` | `account_id` | Agree |
| (always) | `type = HIGHLIGHT` | Agree |
| `title` | `title.text` | Agree |
| `cover` | `cover` (`MediaRef`, role `COVER`) | Agree |
| `item_count`, `items[]` (item pk, in order) | `item_count`, `items[]` (`post_id` `ig:post:<item pk>`, `position`) | Agree, but see question 2 |
| derived `/stories/highlights/<id>/` | `url` | Agree |

### Audio (`content.audio.v1`)

From the post page: `audio_id`, title, artist, and original or licensed; for an original sound, its author when shown. **Nothing else is served**: no duration, album, cover, copyright flags, or use count. **Agree.**

### Media hand-off (`content.media.ready.v1` and the others)

| igs today (`ig.media.ready.v1`, `securebrand.common.v1`) | Contract (`securebrand.media.v1`) | Verdict |
|---|---|---|
| bucket, key, size, SHA-256, content type | `object` | Agree |
| (none) | `perceptual_hash` | Can add when media is downloaded; not computed today |
| `media.ttl: 0`: no deadline, kept until every required consumer acknowledged it | `delete_after` **required** | **Agree, igs changes.** The owner picks a deadline (for example 7 or 30 days). Today igs has none |
| `media.required_consumers` | `required_consumers` | Agree |
| acknowledgements; janitor deletions | `MediaProcessed`, `MediaDeleted` | Agree |

### Job coverage (`collection.coverage.v1`) and discovery (`discovery.candidate.v1`)

| igs field | Contract | Verdict |
|---|---|---|
| `ig.job.v1` `JobEvent`: kind, status, subject, times, counters, completeness, errors | `JobCoverage`: `kind`, `status`, `subject_id`, `subject_ref`, times, `counters` (shared keys plus `ig.*`), `completeness`, `reasons` | Agree. igs maps its counters to the shared keys (requests, bytes, posts, comments, blocked, page_loads), and the rest to `ig.<name>`. The content window (`window_from` and `window_to`) is known per job |
| `discovery_candidates`, `discovery_vets`, `discovery_sources` | `Candidate` | **Agree.** Sources: `TAG`, `COAUTHOR`, `MENTION`, `COMMENTER`. Evidence: one entry per way it was seen, with `sightings`, and `distinct_sources` (the "Iraqi audience"). `relevance.score`; `is_private`. Outcomes: registered → `ADDED`, below → `NOT_RELEVANT`, private → `SECOND_LIST` (scope `PROFILE_ONLY`), gone → `UNAVAILABLE`, failed → stays `PENDING`, rejected → `NOT_RELEVANT` (reason `rejected`), capped → `CAPPED` (tests only, D-008) |
| (none) | `Candidate` for **other platforms**, from Instagram bio links to Facebook, TikTok and YouTube accounts | **Agree; igs would add it.** It's how Instagram can feed fbs (Q11), and it needs the bio-link parsing above |

### Account requests (`registry.account_request.v1`)

igs doesn't read this topic today. Accounts come through its own API and `igsctl`.

**Agree.** igs will consume it, mapping add, update, pause, resume and collect-now, and the tier, onto its registrations and schedule. It's new work (§6).

---

## 2. The mapping rules (`README.md`) that apply to Instagram

| Rule | Verdict |
|---|---|
| "Facebook and Instagram reels … → `POST_TYPE_SHORT_VIDEO`" | **Right.** Instagram's product type `clips` |
| "Instagram highlight items → `POST_TYPE_STORY`, subtype `highlight`, plus a `Playlist` of type highlight" | **Right** (question 2) |
| "Instagram replies: none logged out: `ItemCoverage.replies_available = false`" | **Right** |
| (missing) | **Add the rest of Instagram's table:**<br>• photo (`feed`, media type 1) → `POST_TYPE_POST`, subtype `photo`;<br>• carousel (`carousel_container`, media type 8) → `POST_TYPE_POST`, subtype `carousel`;<br>• a non-reel video (`feed` with media type 2, or `igtv`) → `POST_TYPE_VIDEO`, subtype `video` or `igtv`;<br>• a current 24-hour story, if ever collected → `POST_TYPE_STORY`, subtype `story` |
| Rule 4: fields a platform doesn't have stay unset | **Right.** It matters most for Instagram's `is_business`, `ai_generated` and views on photos |
| Rule 5: a deletion is a partial event with `DELETED` | **Right.** For Instagram, it applies to posts gone on a re-read and, once built, comments gone on a complete re-read |

---

## 3. Corrections to the Instagram column of `docs/field-matrix.md`

| Row | The matrix says | Should say |
|---|---|---|
| Account `stated_country`, `created_at` | — | **N (not tried).** Instagram's "About this account" shows a country and a join date, but igs never tried it logged out, and it's probably login-only. "—" says the platform doesn't have it, which isn't so |
| Account `type` | profile (business flag **N**) | Right. Add: person or business can't be told, so `type` stays unset |
| Account snapshot `posts` | **A** | **O.** A private account's answer may lack the post count (seen in a captured answer); public ones have it |
| Account snapshot "How often" | daily | Daily, plus the first read on registration |
| Post `links` | — (captions have none) | Right in effect: URL text in a caption isn't a link on Instagram, and igs doesn't extract it |
| Post `published_at` | exact **A** | Exact **A**; times derived from the pk are minute-precision (highlight items) |
| Post `flags.edited` | (no row) | **Add the row:** ig `caption_is_edited` **S** (8%), no time |
| Post `status` | visible, unavailable **A** | Visible, unavailable **A**; walled posts (2.6%) are **X** (nothing published today) |
| Counts `views` | reels **O** (99.4%); photos and carousels **N** | Right. Add: non-reel videos (feed or IGTV) are unknown; the post page serves no views for any post |
| Counts: post series | (no row) | **Add:** ig post counts as a series, reels **A** (reels tab), photos and carousels **X** (not published today) |
| Comment `flags.pinned`, `liked_by_creator` | — | **Unknown (not parsed).** Instagram has pinned comments; whether they show logged out is unknown |
| Comment `flags.edited` | detected by text change | Detected in igs's Postgres by text change. On the event, unset (§1, Comment) |
| Media kinds | photo, video, reel, carousel child, thumbnail, highlight cover, profile picture | Add highlight-item media |
| Provenance `raw_ref` | **A** (all kept) | **A** today. D-004 changes the policy (failures and 1% forever, the rest 30 days), so igs must change too (§5) |
| Provenance `exit_country` | — | **X.** igs could fill it with a country per proxy tier |
| Discovery sources | tags, co-authors, mentions, commenters (on in the chart) | Right. Add: profile links to other platforms → **X** (§1) |

Everything else in the Instagram column matches what igs collects.

---

## 4. Fields with no place

Nearly everything has a place now (`Count.hidden`, `platform_topics`, `Playlist`, `Audio`, `ItemCoverage`, the `ig.*` extras keys). What's left:

| igs field | Proposal |
|---|---|
| `latest_reel_media` (the time of the account's newest story; dropped today) | **New extras key `ig.latest_story_at`** on account: a cheap "is it active" signal |
| `is_memorialized` (dropped today) | No new field: `status.reason = "memorialized"` with `VISIBLE` |
| `text_post_app_badge_label` (hints that a Threads account exists) | **Extras key `ig.threads_badge`** on account. Low value; skip it if extras should stay short |
| `redacted` (erasure) | Nothing while erasure is dormant. If used, the erasure contract covers it |
| `highlights_hash`, `content_hash` bookkeeping | Internal; no field needed |
| `discovered_via` value `tagged` | Never used (the tagged tab needs a login). Keep the key, with values `grid` and `reels` |

**Extras to add to `registry/extras.yaml`:** `ig.latest_story_at`, and optionally `ig.threads_badge`.

---

## 5. What igs can't fill, and what would change how it collects

**Can't fill (Instagram doesn't serve it logged out, or igs never tried):**

| Message | Fields |
|---|---|
| Account | `type` (person or business), `is_business`, `categories`, `contact`, `stated_country`, `platform_language`, `location`, `created_at`, name or handle change times, `cover`, `keywords`, `alt_ids` |
| Post | `title`, `description`, text spans (no positions served), `platform_language`, `uploader_tags`, `links`, `created_in_country`, `reshare`, `category`, `poll`, `captions`, paid-partnership and other labels, live status; views on photos and carousels; shares, saves and reposts |
| Comment | Replies (so `parent`, `root`, `depth` > 0), the author's display name and type, text spans, `platform_language`, `pinned`, `liked_by_creator` |
| Media | `mime` and `size_bytes` until downloaded; `perceptual_hash` until computed |

**Would change how igs collects or publishes:**
1. **The metrics volume rule** needs memory: the last published counts per target, and the day of its last event. igs's normalizer is stateless today. It needs a small Valkey record per target (question 6).
2. **`content_hash` without counts, and `event_id` per observation** (§1, Envelope).
3. **D-004's raw-response policy** replaces igs's "keep every raw answer forever":
   - failures and a 1% sample kept forever;
   - the rest for 30 days.

   The raw sink writes the sample to its own prefix, with a lifecycle rule of 30 days on `raw/`. A re-parse tool then covers only 30 days, plus the sample.
4. **`MediaReady.delete_after` is required**, so `media.ttl` can't stay 0. The owner needs to pick a number.
5. **D-008, no discovery caps in production.** This overrides my earlier proposal of a 50-a-day cap (`igs-gaps.md`, Plans 1). The guard that vets can't take regular collection's exits still matters; that one isn't a cap.
6. **Status for walled posts and per-post coverage** need the E10 fix. Comment deletions need the E7 fix.
7. **New duties:**
   - consume `registry.account_request.v1`;
   - publish `Candidate` events for other platforms from Instagram bio links;
   - publish `Audio` and `Playlist`.
8. **Headers:**
   - `sb-type` and `sb-contract` on `content.*`. igs keeps `igs-type` on its own `ig.*` topics; nothing reads it.
   - **Collectors don't register shared subjects**, which matches the standing rule, so igs's `schemas apply` must leave out `content.*` (E3).

---

## 6. The effort to adopt, updated

My earlier estimate was 7 to 8 days (`igs-contract-notes.md` §3), for an envelope and field mapping onto igs's own draft. Against v0.1:

| Work | Effort |
|---|---|
| Normalizer onto the contract's messages: Account, AccountSnapshot, Post, Comment, Metrics, Media, Playlist and Audio, with `Count`, `Text`, `Time`, `AccountRef` and `Link`; conformance in CI; delete igs's `securebrand.content.v1` | 3 to 4 days |
| `content_hash` without counts; `event_id` per observation; platform `ig` and IDs; `sb-*` headers | 0.5 day |
| The metrics volume rule (state per target in Valkey); post-page metrics for photos and carousels | 1 day |
| Per-post `ItemCoverage`, and walled posts as `RESTRICTED` (E10) | 1 day |
| Bio-link parsing to platform and ID; the linked Facebook page ID (REQ-008 fix 9) | 1 day |
| `discovery.candidate.v1`: Instagram candidates with outcomes, and cross-platform candidates from bio links | 1.5 to 2 days |
| `collection.coverage.v1` from igs's jobs | 0.5 day |
| The media hand-off on `content.media.*` with `delete_after` | 0.5 to 1 day |
| A `registry.account_request.v1` consumer | 1 to 1.5 days |
| D-004 raw policy (sample prefix, lifecycle) | 0.5 day |
| **Total** | **About 11 to 14 days** |

Comment deletions (E7) are another 2 days if they're wanted before adoption. Adapting the ClickHouse writer to the contract is the platform's work and isn't included here (question 7).

---

## 7. The questions for igs

### Q1. Account counts move to `content.account_snapshot.v1`. Does anything break?

No:
- The only consumer of igs's `content.metrics` with `target = account` is igs's own ClickHouse writer, which routes it to `account_snapshots`. It reads the new topic instead.
- Nothing is deployed, so nothing in Kafka or storage needs migrating.

Two notes:
- `Account` gets a new version "when anything but counts changes". That needs the new count-free `content_hash` (§5), or every daily read becomes a new account version.
- igs reads a profile daily, so snapshots are daily. That's one a day per account even when nothing changed, which matches the volume rule's daily floor.

### Q2. Highlights as `POST_TYPE_STORY` items plus a `Playlist`: does it fit, given the items are unverified?

**The shape fits.** A highlight is an ordered list of an account's stories, which is exactly `Playlist`, and each item is a story.

What's verified and what isn't:
- The highlights list (IDs, titles, covers) comes with the profile query and is real.
- **The items come from a query whose real answer igs has never seen.** The parser is tested on a hand-made fixture only.

My proposal:
- Publish each `Playlist` with its title, cover and URL now.
- Publish `items` and the `POST_TYPE_STORY` posts **only after one real check**: a single read of one public profile with highlights, run by the owner with the existing probe.
- If logged-out Instagram doesn't serve the items, the playlists stay without items, and `ItemCoverage` or `Status` records why (reason `login_required`).

Also, an item's time comes from its pk, so its `published_at` precision should be `MINUTE` (§1, Post).

### Q3. Hidden likes as `Count.hidden = true` with no value; views on photos and carousels unset. Right?

**Right on both.**
- When an owner hides likes, Instagram answers a number from 0 to 3 that means nothing. igs already drops it and keeps the flag (3% of post pages, 1.9% of reels). Views and comments stay real when likes are hidden.
- Photos and carousels have no views logged out, so they stay unset, never 0.
- One more point: **the post page serves no views for any post**. Reel views come only from the reels tab, on `content.metrics.v1`. A `content.post.v1` event for a reel has no views, and that's correct.

### Q4. The linked Facebook page as a `Link` (`LINKED_ACCOUNT`, `target_platform = fb`, `target_id` = page ID). Can igs fill it once the ID is kept?

**Yes.**
- The profile query serves the page's ID and name together (`linked_fb_info.linked_fb_page`). igs drops the ID only because its own draft had nowhere to put it.
- With fix 9: `target_id` = the ID, `title` = the name. `url` is empty (Instagram gives no URL), or the master may prefer `https://www.facebook.com/<id>`; please say which.
- How often accounts have one isn't measured. Where present, it's the best Instagram-to-Facebook link the entity layer will get.
- It should also become a `Candidate` for fbs (`SOURCE_PROFILE_LINK`), when the page isn't on fbs's list.

### Q5. Mentions: does igs resolve any username to a pk?

**Not today.** The normalizer is stateless, and a caption mention is only a name. Two cheap resolutions are possible:
1. **Within the same post.** A mentioned username that is also a tagged account or co-author of that post has a known pk. igs can fill `id` with no state at all. This is cheap and certain.
2. **From igs's own tables.** Registered accounts and vetted discovery candidates have a username and pk. Names change, so a match is only right "as of" the read. I'd leave this to the entity layer, which can resolve handles with a time.

I propose that igs does (1), and leaves the rest as handle-only `AccountRef`s.

### Q6. The metrics volume rule with the reels tab: what does it do to igs's event counts?

**Today:**
- every visit re-reads the reels tab a year back;
- every reel there is a metrics event per visit, about 8 a day on the 3-hour tier;
- for accounts with 100 to 1,000 reels a year, that's 0.8 to 8 million events a day per 1,000 accounts.

**Under the rule:**
- An event is published when a reel's counts changed since the last published one, and at least once a day per reel still being refreshed.
- Recent reels change on almost every visit. Older ones change less, but views keep growing slowly.
- I haven't measured the share of unchanged reads.
- So the count falls to between 1× and 8× the number of reels per day: likely about 2 to 4 million a day per 1,000 accounts for reel-heavy accounts. That's an estimate.

**The bigger saving is the reels window** (`igs-gaps.md`, "the reels window"):
- Reels older than 7 days are re-read weekly instead of every visit.
- They're then "still refreshed" only weekly, so they get at most one event a week, and only if changed.
- With both, I estimate under about 0.3 million metrics events a day per 1,000 accounts.

The rule needs igs to remember, per target, the last published counts and the day of its last event. That's a small Valkey record (§5). Implemented in the crawler, it also saves Kafka traffic. The owner still has to decide on the reels window.

### Q7. igs's ClickHouse writer: anything in the contract that would be hard to store with it?

The writer's loop (batch, insert, commit) carries over unchanged. Its schema and rows don't. The hard parts:

1. **Partial events.**
   - A deletion or status-only event carries the keys and the status, with every other field unset, meaning "unchanged".
   - igs's tables keep the newest version per entity (`ReplacingMergeTree(version, is_deleted)`). A partial row there would **replace the full row with blanks**.
   - So it needs either a separate status table joined at read time, or a read of the previous version to merge before insert. **That's the main design point.**
2. **D-004, every version kept.**
   - Sort entity tables by `(platform, id, version)`, so every version stays and an identical redelivery still collapses. igs's writer re-inserts tables that already succeeded when a batch is retried (REQ-003 §1).
   - Read "current" with `argMax`, or keep a "latest" table beside it.
3. **Nested messages.**
   - `Count` (value, exactness, displayed, hidden) per count means several columns each. Store them as `Nullable(Int64)` plus small `LowCardinality` columns, or as one `Tuple` per count.
   - `Engagement.reactions` and `other` are maps.
   - `Text.spans`, `AccountRef` lists (mentions, tagged, coauthors), `Link` lists, playlist items and `MediaRef` lists become `Array(Tuple(...))`, or child tables. Child tables are better for joins on account IDs, since the entity layer will look up "every post that tags account X".
   - igs's present schema stores links as two parallel arrays, with no platform or ID.
4. **`google.protobuf.Struct` extras.** Today it's a JSON string. ClickHouse 25.x's `JSON` type would let analysts query `ig.*` keys.
5. **`Time` with precision.** Store `published_at` plus `published_precision`. Partitioning by month still works for YouTube's year-precision times; they're estimates, and a later reading replaces them by version.
6. **Volume.** With the volume rule and the reels window, metrics rows drop by about 10 to 30 times (Q6). Without them, `post_metrics` is the largest table by far.
7. **The poison-row problem** (REQ-003 §1). A row ClickHouse rejects blocks igs's writer forever. The platform writer should sort errors into retryable and not retryable, with a dead-letter topic.

My estimate for adapting igs's writer and schema to v0.1, as the platform's writer, is 3 to 5 days. The partial-event design is the uncertain part.

---

## Open points I'd add to the README

1. **Instagram account `type`:** unset (person or business unknown), or a new value.
2. **`owner_kind = playlist`** for a highlight's cover.
3. **Time precision for times derived from IDs:** `MINUTE` for Instagram times worked out from a pk.
4. **The linked Facebook page's `url`:** empty, or built from the ID.
5. **`MediaReady.delete_after`:** the owner's deadline for media files.
