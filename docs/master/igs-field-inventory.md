# igs field inventory

Written by the igs session from the code at `Abdullah95k/securebrand-microservices-igs` `main` (`d50f3ee`), 5 November 2026 (Iraq time). It describes what the code does today; planned changes are in the last section. **Every example value is synthetic.**

Sources of truth: protos `api/proto/securebrand/ig/v1/*.proto` and `content/v1/content.proto`; topics `configs/ig/topics.yaml`; tables `migrations/ig/*.sql`; parsers `internal/ig/parse`; normalizer `internal/ig/normalize`; settings `configs/default.yaml` (`ig:`). Percentages are measured on real Instagram (1 Nov 2026, 3,521 post pages and 325,681 reels; `docs/instagram/build-status.md`).

## 1. Overview

- **Service:** igs (Instagram), `Abdullah95k/securebrand-microservices-igs`, commit `d50f3ee`.
- **Access method:** logged out, no Instagram accounts. One page load (profile page HTML) per identity gives the tokens; then Instagram's own web GraphQL over plain HTTP with a Chrome TLS fingerprint, and post pages (HTML with embedded JSON). A browser fallback exists and is **off** (capped at 2% and 2 per process when on).
- **Entities and code names:** profile = account; post = post, video, carousel or reel (reel = product type `clips`); post metrics (reels tab); comment (top-level only); comment author; media; highlight (its items are posts of kind `highlight`); job; discovery candidate.
- **Not available logged out:** replies (only their count); views except on reels; shares and saves; the tagged tab (login page); category, business and contact fields; comment authors' display names; about 2.6% of post pages (login wall). Stories: not attempted.
- **How often:**
  - the grid on every visit: hot every 10 min, warm every hour, long tail every 3 h (auto: warm at 2 or more posts a day over 14 days);
  - the reels tab on every visit, back a year;
  - the profile and highlights list once a day;
  - each new post's page and comments once;
  - posts of the last 7 days re-read with doubling gaps (about 5 times on the 3-hour tier);
  - backfill on registration: 365 days.

Columns: **Present** is always, often (over 50%), sometimes, rare, logged_in_only or unknown, with a measured percentage where there is one. **History** is latest, versions, snapshots or key. **PD** = personal data. Published names: `ig.*` is igs's platform contract (`securebrand.ig.v1`), `content.*` is igs's draft of the shared contract (`securebrand.content.v1`, see `igs-contract-notes.md`); `extras` means `extras_json`.

## 2. Entities

### profile (unified: account)

**Fetched:** On the account's first run, then once a day (`ig.schedule.profile_refresh: 24h`). Discovery's vet reads also read it, but publish nothing.

Account = Instagram profile. The profile query returns about 5 KB.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `user_pk` | The account's numeric Instagram ID | int64 | 1000000001 | always | profile query (user by username) (`pk`; `id` is the same number as text) | `profiles.user_pk` | `ig.profile.v1 Profile.user_pk`; `content.account.v1 Envelope.native_id` (text) | key | yes | Stable over renames. igs never keys on the username. Content key: `instagram:<pk>`. |
| `username` | The handle in the profile URL | text | fake_user_1 | always | profile query (user by username) | `profiles.username`; `profile_snapshots.username` | `Profile.username`; `Account.handle` | snapshots | yes | Can change. A rename shows as the same `user_pk` with a new name: a new snapshot row and a new event version. Registration by name (`u:<name>`) resolves to the pk on the first read. |
| `full_name` | Display name | text | Example Name | often | profile query (user by username) | `profiles.full_name`; `profile_snapshots.full_name` | `Profile.full_name`; `Account.name` | snapshots | yes | Empty when the account sets none. UTF-8 as received (Arabic, Kurdish, emoji). |
| `biography` | The bio | text | example bio text | often | profile query (user by username) | `profiles.biography` | `Profile.biography`; `Account.bio` | latest | yes | Postgres keeps the latest only (no bio history table). Each change is a new event version, and the raw response is kept. Instagram's limit is 150 characters. Can hold phone numbers and e-mails. |
| `bio_links[].url` | Every link on the profile (website and link-in-bio) | list of text | https://example.com/fake | sometimes | profile query (user by username) (`bio_links[].url`; `lynx_url` when `url` is empty) | `profiles.bio_links` (JSON) | `Profile.bio_links[].url`; `Account.links[].url`; `Account.website` = the first link | latest | no | All links Instagram shows (up to 5). `lynx_url` is Instagram's redirect wrapper around the same target. No history table: the latest set, plus each change as a new event version. |
| `bio_links[].title` | The link's label | text | Example shop | sometimes | profile query (user by username) | `profiles.bio_links` (JSON) | `Profile.bio_links[].title`; `Account.links[].title` | latest | no |  |
| `bio_links[] other keys` | `link_id`, `link_type`, `is_pinned`, `creation_source`, `image_url`, `media_type`, `media_accent_color_hex` | mixed | - | sometimes | profile query (user by username) | not stored (raw response only) | not published | - | no | Dropped: display data, not needed for linking. |
| `is_verified` | Blue tick | bool | false | always | profile query (user by username) | `profiles.verified`; `profile_snapshots.verified` | `Profile.is_verified`; `Account.verified` | snapshots | no |  |
| `is_private` | Private account | bool | false | always | profile query (user by username) | `profiles.private`; `profile_snapshots.private`; `profiles.status = private` | `Profile.is_private`; `Profile.status`; `Account.is_private` | snapshots | no | A private account gives its profile fields only; igs reads no posts from it. |
| `follower_count` | Followers | int64 (optional) | 12345 | always | profile query (user by username) | `profiles.follower_count`; `profile_snapshots.follower_count` | `Profile.follower_count`; `Account.followers`; `content.metrics.v1 Metrics.followers` (target account) | snapshots | no | Exact integer, not "12.3K". Absent means unknown, never 0. |
| `following_count` | Accounts followed | int64 (optional) | 321 | always | profile query (user by username) | `profiles.following_count`; `profile_snapshots.following_count` | `Profile.following_count`; `Account.following`; `Metrics.following` | snapshots | no | Exact. |
| `media_count` | Number of posts | int64 (optional) | 456 | always | profile query (user by username) (`media_count` or `all_media_count`) | `profiles.media_count`; `profile_snapshots.media_count` | `Profile.media_count`; `Account.post_count`; `Metrics.post_count` | snapshots | no | Exact. |
| `profile_pic` | Profile picture URL (HD first) | text (URL) | https://cdn.example.invalid/fake_pic.jpg?oe=6A1B2C3D | always | profile query (user by username) (`profile_pic_url_hd`, else `profile_pic_url`) | `profiles.profile_pic_url` | `Profile.profile_pic.url`; `Account.picture_url`; `ig.media.v1` kind `profile_pic` | latest | yes | Signed CDN URL: expires (the `oe` parameter, hex Unix time; a few days). Width and height are not served. Not downloaded today. |
| `linked_facebook_page` | Name of the Facebook page linked to the account | text | Example Page | rare | profile query (user by username) (`linked_fb_info.linked_fb_page.name`) | `profiles.linked_facebook_page` | `Profile.linked_facebook_page`; in `content.account.v1` as the `title` of a `Link` with `platform` `fb` (the name was in `url` until REQ-008 fix 9) | latest | no | Share not measured. Its ID is kept since REQ-008 fix 9 (the next row). |
| `linked_fb_info.linked_fb_page.id` | The linked Facebook page's ID | text | 100000000000001 | rare | profile query (user by username) | `profiles.linked_facebook_page_id` | `Profile.linked_facebook_page_id`; in `content.account.v1` as a `Link` with `platform` `fb`, `platform_id` the ID and `url` `https://www.facebook.com/<id>` | latest | no | Kept since REQ-008 fix 9 (dropped before). The strongest Instagram-to-Facebook link for the entity layer. A linked personal account's ID is not kept: it is not a page. |
| `linked_fb_info.linked_fb_user` | A linked Facebook user | object | - | unknown | profile query (user by username) | not stored | not published | - | yes | Empty in every captured answer. Dropped. |
| `category` | Account category | text | Example category | unknown | profile query (user by username) (`category_name` or `category`, if served) | `profiles.category` | `Profile.category`; `Account.category` | latest | no | Absent from every logged-out answer captured; may be logged-in only. Always empty in practice. |
| `is_business` | Business or creator account | bool | false | unknown | profile query (user by username) (`is_business_account`, if served) | `profiles.is_business` | `Profile.is_business`; `Account.is_business` | latest | no | Absent from every logged-out answer captured, so always `false`; `false` here means unknown (a proto3 bool can't say so). Fix planned: make it optional. |
| `business contact fields` | Public e-mail, phone, address, city, `external_url` | text | - | unknown | profile query (user by username) | not stored | not published | - | yes | Not in the logged-out answer igs reads. The website appears as a bio link instead of `external_url`. Whether another logged-out page serves them is unknown. |
| `pronouns` | Pronouns, joined with "/" | text | they/them | rare | profile query (user by username) | not stored | `Profile.pronouns`; `Account.extras_json.pronouns` | latest | yes | Published but not stored in Postgres (no column). |
| `highlight_count` | Number of highlights | int32 | 3 | always | highlights list inside the profile query (counted) | `profiles.highlight_count` | `Profile.highlight_count`; `Account.extras_json.highlight_count` | latest | no |  |
| `highlights_hash` | Hash of the highlights list | bytes | - | always | derived by igs | `profiles.highlights_hash` | `Profile.highlights_hash` | latest | no | Internal: unchanged highlights aren't re-read. |
| `has_any_clips` | The account has reels | bool | true | always | profile query (user by username) | not stored | not published | - | no | Parsed, unused. |
| `is_memorialized` | Memorial account | bool | false | always | profile query (user by username) | not stored (`profiles.memorialized` exists but is never written) | not published | - | no | Dropped. Could feed `status`. |
| `other profile keys` | `is_unpublished`, `is_coppa_enforced`, `account_badges`, `show_text_post_app_badge`, `text_post_app_badge_label` | mixed | - | always | profile query (user by username) | not stored | not published | - | no | Dropped. `text_post_app_badge_label` hints that a Threads account exists (a possible cross-platform signal). |
| `latest_reel_media` | Time of the newest story | int64 (Unix s) | 1767254400 | sometimes | profile query (user by username) | not stored | not published | - | no | Dropped. Stories aren't collected (see Story or highlight). |
| `status` | active, private, unavailable, suppressed | enum | active | always | derived by igs (no user: unavailable; erasure: suppressed) | `profiles.status` | `Profile.status`; `Account.is_deleted` (unavailable or suppressed); `Account.is_private` | latest | no | "Unavailable" covers renamed, deleted and wrong names: Instagram answers the same "no user" for all three. |
| `requested_ref` | What the client registered | text | u:fake_user_1 | always | API registration | `profile_schedule.profile_ref`; `profile_registrations` | `Profile.requested_ref`; `Account.extras_json.requested_ref` | key | yes | `u:<name>` or `id:<pk>`. |
| `author_key` | HMAC of the user pk | bytes (32) | - | always | derived by igs (`privacy.author_hmac_key`) | `profiles.author_key` | `Profile.author_key` (not on `content.account.v1`) | key | no | A pseudonym, the same as the account's `author_key` when it comments. |
| `url` | Profile URL | text | https://www.instagram.com/fake_user_1/ | always | derived by igs (from username) | not stored | `Account.extras_json.url` (`content.account.v1` has no url field) | latest | no |  |
| `kind` | Account kind | text | profile | always | derived by igs | not stored | `Account.kind` (always `profile`) | latest | no |  |
| `bookkeeping` | `content_hash`, `first_seen_at`, `last_scraped_at`, `last_profile_read_at` | bytes, timestamps | - | always | derived by igs | `profiles.*` | `Observation.content_hash` and `observed_at` | latest | no |  |

### profile_snapshot (unified: account_snapshot)

**Fetched:** Every profile read (daily). Stored only when a value changed since the last row; published on every read.

There is no `content.account_snapshot.v1` topic: account counts go to `content.metrics.v1` with `target = account`.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `user_pk` | The account | int64 | 1000000001 | always | profile query (user by username) | `profile_snapshots.user_pk` | `content.metrics.v1 Envelope.native_id` (target account) | key | no |  |
| `captured_at` | When igs read it | timestamp (UTC) | 2026-11-05T09:00:00Z | always | derived by igs | `profile_snapshots.captured_at` | `Envelope.observed_at` | key | no | Monthly partitions. |
| `follower_count, following_count, media_count` | Counts | int64 (optional) | 12345 | always | profile query (user by username) | `profile_snapshots.*` | `Metrics.followers`, `following`, `post_count` | snapshots | no | Exact. |
| `username, full_name, verified, private` | Identity flags at that time | text, bool | fake_user_1 | always | profile query (user by username) | `profile_snapshots.*` | on `content.account.v1` (a new version) | snapshots | yes | Bio and links are **not** in the snapshot table. |

### post (unified: post)

**Fetched:** The grid on every visit (hot 10 min, warm 1 h, long tail 3 h) back to the newest stored post; each new post's page once; posts of the last 7 days re-read with doubling gaps (`interval * 2^n`, at most 30 a run), which is about 5 re-reads per post on the 3-hour tier. Backfill on registration: 365 days (`limits.max_age_days`).

Post = feed post, video, carousel or reel (reel = product type `clips`). The post page is about 118 KB.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `media_pk` | The post's numeric ID | int64 | 3000000000000000001 | always | grid query (profile timeline) (`pk`/`id`) | `posts.media_pk`; `post_index` | `ig.post.v1 Post.media_pk`; `content.post.v1 Envelope.native_id` (text) | key | no | Stable. The posting time can be derived from it. One object, two IDs: `media_pk` and `shortcode` (convertible). |
| `shortcode` | The post's code in its URL | text | FAKEcode01 | always | grid query (profile timeline) (`code`) | `posts.shortcode`; `post_index.shortcode` | `Post.shortcode`; `content.post.v1 extras.shortcode` | key | no |  |
| `owner_pk` | The account that posted it | int64 | 1000000001 | always | grid query (profile timeline) | `posts.owner_pk`; `post_owners` (role owner) | `Post.owner_pk`; `content.post.v1 Post.account_id` | key | yes |  |
| `taken_at` | Posting time | timestamp (UTC) | 2026-11-01T18:30:00Z | always | grid query (profile timeline) (`taken_at`, Unix seconds) | `posts.taken_at` (monthly partitions) | `Post.taken_at`; `Envelope.published_at` | key | no | Exact to the second. |
| `media_type` | photo (1), video (2), carousel (8) | enum | 1 | always | grid query (profile timeline); post page (HTML with embedded JSON) | `posts.media_type` | `Post.media_type` | latest | no | In one run of 69,094 posts: 57% photos, 40% videos, 2.5% carousels. |
| `product_type` | feed, clips (reel), carousel_container, igtv | text | clips | always | grid query (profile timeline); post page (HTML with embedded JSON) | `posts.product_type` | `Post.product_type`; `extras.product_type` | latest | no |  |
| `kind` | post, video, reel or highlight | enum | reel | always | derived by igs (from product and media type) | `posts.kind` | `content.post.v1 Post.kind` | latest | no | Highlight items are posts of kind `highlight` (owner's choice). |
| `caption` | The post's text | text | example caption #fakeTag @fake_user_2 | often | post page (HTML with embedded JSON) (`caption.text`) | `posts.caption`; `post_edits.caption` (each replaced caption, with when the new one was seen) | `Post.caption`; `content.post.v1 Post.text` | versions | yes | Full text, never truncated by igs; line breaks and emoji kept. Instagram's limit is 2,200 characters. No language field is served. |
| `caption_edited` | Instagram's edited flag | bool | false | sometimes (8%) | post page (HTML with embedded JSON) (`caption_is_edited`) | `posts.caption_edited` | `Post.caption_edited`; `extras.caption_edited` | latest | no | True on 8% of 3,521 post pages (measured 1 Nov). No edit time is served. |
| `hashtags` | Hashtags in the caption | list of text | ["fakeTag"] | sometimes | derived by igs (regular expression over the caption) | `posts.hashtags` | `Post.hashtags`; `extras.hashtags` | versions | no | No positions kept. Instagram serves no hashtag entities logged out. |
| `mentions` | Usernames mentioned in the caption | list of text | ["fake_user_2"] | sometimes | derived by igs (regular expression over the caption) | `posts.mentions` | `Post.mentions`; `extras.mentions` | versions | yes | Usernames, not IDs; no positions. |
| `accessibility_caption` | Instagram's automatic image description | text | Photo of example scene | often | grid query (profile timeline); post page (HTML with embedded JSON) | `posts.accessibility_caption` | `Post.accessibility_caption`; `extras.accessibility_caption`; `Media.alt_text` on carousel children | latest | no | Share not measured. |
| `location` | Tagged place: `pk`, name, lat, lng | object | {pk: 200000001, name: "Example Place", lat: 33.3, lng: 44.4} | sometimes (10%) | post page (HTML with embedded JSON) (`location`) | `posts.location` (JSON) | `Post.location`; `extras.location` | latest | no | 10% of post pages. Coordinates when given. |
| `usertags` | Accounts tagged in the media: `user_pk`, username, x, y, child | list of object | [{user_pk: 1000000002, username: fake_user_2, x: 0.5, y: 0.4}] | sometimes (10%) | post page (HTML with embedded JSON) (`usertags`, and carousel children's) | `posts.usertags` (JSON) | `Post.usertags`; `extras.tagged_ids` (IDs only) | latest | yes | 10% of post pages. x and y from 0 to 1. |
| `coauthors` | Collaborators: `user_pk`, username | list of object | [{user_pk: 1000000003, username: fake_user_3}] | rare (6%) | post page (HTML with embedded JSON) (`coauthor_producers`) | `posts.coauthors` (JSON); `post_owners` (role coauthor) | `Post.coauthors`; `content.post.v1 Post.coauthor_ids` | latest | yes | 6% of post pages. A collaboration also shows in each co-author's reels tab. |
| `music` | title, artist, audio_id, original_audio | object | {title: "Example Song", artist: "Example Artist", audio_id: "900000001", original_audio: false} | often (50%) | post page (HTML with embedded JSON) (`clips_metadata`: `music_info` or original sound) | `posts.music` (JSON) | `Post.music`; `extras.music` | latest | no | 50% of post pages: 200 licensed tracks in the sample, the rest original audio. `is_explicit`, `audio_filter_infos` and similar keys dropped. |
| `topics` | Instagram's related topics | list of text | ["Example topic"] | often (89%) | post page (HTML with embedded JSON) (`related_topic_pills[].topic_name`) | `posts.topics` | `Post.topics`; `extras.topics` | latest | no | 89% of post pages. `topic_id` dropped. |
| `ai_label` | AI-generated label text | text | AI info | rare (7%) | post page (HTML with embedded JSON) (`ai_label_info`) | `posts.ai_label` | `Post.ai_label`; `extras.ai_label` | latest | no | 7% of post pages. |
| `like_count` | Likes | int64 (optional) | 68 | often (97%) | post page (HTML with embedded JSON) (`like_count`); reels-tab query for reels | `posts.like_count` (NULL when hidden); `post_metrics.like_count` (reels) | `Post.like_count`; `content.post.v1 Post.likes` (absent when hidden) | latest | no | Exact. When the owner hides likes, Instagram answers 0 to 3: igs stores nothing and sets `counts_hidden`. Hidden on 3% of post pages. |
| `counts_hidden` | The owner hid likes | bool | false | always | post page (HTML with embedded JSON) (`like_and_view_counts_disabled`); reels-tab query | `posts.counts_hidden`; `post_metrics.counts_hidden` | `Post.counts_hidden`; `extras.counts_hidden`; `Metrics.counts_hidden` | latest | no | True on 3% of post pages and 6,074 of 325,681 reels (1.9%). |
| `comment_count` | Comments, as Instagram counts them | int64 (optional) | 5 | always | post page (HTML with embedded JSON); reels-tab query | `posts.comment_count`; `post_metrics.comment_count` (reels) | `Post.comment_count`; `content.post.v1 Post.comments` | latest | no | Exact. Whether it includes replies is unknown. Compared with comments collected for completeness. |
| `commenting_disabled` | Comments turned off | bool | false | always | post page (HTML with embedded JSON) (`comments_disabled`, `commenting_disabled_for_viewer`) | `posts.commenting_disabled` | `Post.commenting_disabled`; `extras.commenting_disabled` | latest | no |  |
| `play_count` | Views (reels) | int64 (optional) | 7739 | often (99.4%) | reels-tab query (`play_count`); the post page serves none logged out | `posts.play_count`; `post_metrics.play_count` | `ig.post_metrics.v1 PostMetrics.play_count`; `content.post.v1 Post.views`; `Metrics.views` | snapshots | no | Reels only: 99.4% of reels carry views. Photos and carousels: no views logged out. Exact integers. |
| `shares, saves, reposts` | Share and save counts | int64 | - | logged_in_only | - | not stored | not published | - | no | Not served logged out (post page and reels tab checked). |
| `is_pinned` | Pinned to the grid or reels tab | bool (optional) | false | rare | derived by igs (pinned posts come first in the grid; the reels tab lists pinned reels) | `posts.is_pinned` | `Post.is_pinned`; `extras.is_pinned` | latest | no | Inferred among the first 3 grid items; not a served flag. |
| `children` | Carousel items: media_pk, position, media_type, accessibility caption, media IDs | list of object | [{media_pk: 3000000000000000002, position: 0}] | sometimes | post page (HTML with embedded JSON) (`carousel_media`) | `posts.child_count`; `media` rows (`parent_media_pk`, `position`) | `Post.children`; `content.post.v1 Post.media_count` | latest | no | The grid's `carousel_media_count` is dropped (the post page gives the children). |
| `media_ids` | igs's media IDs, in order | list of text (UUIDv5) | ["00000000-0000-5000-8000-000000000001"] | always | derived by igs | `posts.media_ids` | `Post.media_ids` | latest | no | Deterministic from the platform media and variant. |
| `video_duration_ms` | Video length | int64 (ms) | 41000 | always | post page (HTML with embedded JSON) (`video_duration`, else the DASH manifest) | `posts.video_duration_ms` | `Post.video_duration_ms`; `extras.video_duration_ms`; `Media.duration_ms` | latest | no | Videos only. Median 41 s in the sample. Milliseconds. |
| `width, height` | Media size | int32 | 1080 | always | post page (HTML with embedded JSON) (`original_width`, `original_height`) | `posts.width`, `posts.height` | `Post.width`, `Post.height`; `Media.width`, `height` | latest | no |  |
| `has_audio` | Video has sound | bool (optional) | true | always | post page (HTML with embedded JSON) | `posts.has_audio` | `Post.has_audio` | latest | no | Videos only. |
| `permalink` | The post's URL | text | https://www.instagram.com/p/FAKEcode01/ | always | derived by igs (from shortcode) | `posts.permalink` | `Post.permalink`; `content.post.v1 Post.url` | latest | no |  |
| `discovered_via` | grid, tagged or reels | enum | grid | always | derived by igs | `posts.discovered_via` | `Post.discovered_via`; `extras.discovered_via` | latest | no | `tagged` is never used: the tagged tab needs a login (tested 5 Nov). |
| `status` | visible or unavailable | enum | visible | always | derived by igs (a re-read finds the post gone) | `posts.status` | `Post.status`; `content.post.v1 Post.is_deleted` | latest | no | Unavailable means deleted, archived or made private: Instagram doesn't say which. Noticed by re-reads: within the 7-day refresh window, and at each post's one full re-read at 30 days (REQ-008 fix 14). |
| `walled post pages` | Post page served only a login wall | - | - | rare (2.6%) | post page (HTML with embedded JSON) | job counter `posts_gated` | **nothing published** for the post | - | no | 2.6% of post pages. After three fresh identities see the wall, igs skips the post and publishes no event, though the grid gave its ID, time, type and thumbnail. Fix planned. |
| `dropped post-page keys` | `seo_canonical_url`, `organic_tracking_token`, `is_dash_eligible`, `number_of_qualities`, `if_is_crawler`, `video_dash_manifest` (used for duration only), `is_trending_in_clips`, `topic_id`, player settings | mixed | - | always | post page (HTML with embedded JSON) | not stored (raw response only) | not published | - | no | Dropped: delivery and player data. |
| `paid partnership, sponsor` | Branded-content flag | bool | - | unknown | post page (HTML with embedded JSON) | not stored | not published | - | no | Not seen in any captured logged-out answer. |
| `bookkeeping` | `content_hash`, `first_seen_at`, `last_seen_at`; `comments_state`, `comments_collected`, `extra` | mixed | - | always | derived by igs | `posts.*` (`comments_state`, `comments_collected` and `extra` are never written) | `Observation` | latest | no | Per-post coverage lives in the job's `completeness`, not on the post row. |

### post_metrics (unified: post_metrics)

**Fetched:** The reels tab on every visit, back to `max_age_days` (365); the post page on each read and re-read (latest values only).

A row is stored only when a count changed. **Only reels have a metrics series.** Photos, carousels and non-reel videos get their likes and comments from post-page re-reads, which update `posts` (latest) and publish a new `content.post.v1` version, but no metrics event and no `post_metrics` row. Planned fix: a metrics event per post-page read.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `media_pk` | The post | int64 | 3000000000000000001 | always | reels-tab query | `post_metrics.media_pk` | `ig.post_metrics.v1 PostMetrics.media_pk`; `content.metrics.v1 Envelope.native_id` (target post) | key | no |  |
| `owner_pk` | The account | int64 | 1000000001 | always | reels-tab query | `post_metrics.owner_pk` | `PostMetrics.owner_pk`; `Metrics.parent_id` | key | yes |  |
| `taken_at` | Posting time | timestamp | 2026-11-01T18:30:00Z | always | derived by igs (from the pk) | - | `PostMetrics.taken_at`; `Envelope.published_at` | key | no |  |
| `captured_at` | When igs read it | timestamp (UTC) | 2026-11-05T09:00:00Z | always | derived by igs | `post_metrics.captured_at` (monthly partitions) | `PostMetrics.captured_at`; `Envelope.observed_at` | key | no |  |
| `play_count` | Views | int64 (optional) | 7739 | always | reels-tab query | `post_metrics.play_count` | `PostMetrics.play_count`; `Metrics.views` | snapshots | no | Exact. |
| `like_count` | Likes | int64 (optional) | 125 | often | reels-tab query | `post_metrics.like_count` (NULL when hidden) | `PostMetrics.like_count`; `Metrics.likes` (absent when hidden) | snapshots | no |  |
| `comment_count` | Comments | int64 (optional) | 6 | always | reels-tab query | `post_metrics.comment_count` | `PostMetrics.comment_count`; `Metrics.comments` | snapshots | no |  |
| `counts_hidden` | Likes hidden | bool | false | always | reels-tab query | `post_metrics.counts_hidden` | `PostMetrics.counts_hidden`; `Metrics.counts_hidden` | snapshots | no |  |

### comment (unified: comment)

**Fetched:** Every top-level comment of each new post, newest first, about 24 a page (`max_comments_per_post: 0` = no cap); again on each re-read within 7 days, stopping at stored ones; skipped when the comment count is unchanged.

Comment = top-level comment. Replies: none logged out. A comments page is about 4 to 9 KB.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `comment_pk` | The comment's ID | int64 | 17900000000000001 | always | comments query, and the first comments embedded in the post page (`pk`) | `comments.comment_pk` | `ig.comment.v1 Comment.comment_pk`; `content.comment.v1 Envelope.native_id` | key | no |  |
| `media_pk` | The post | int64 | 3000000000000000001 | always | derived by igs (task context) | `comments.media_pk` | `Comment.media_pk`; `content.comment.v1 Post.post_id`; Kafka key of both topics | key | no | Keyed by post, so a post's comments stay in order. |
| `owner_pk` | The post's account | int64 | 1000000001 | always | derived by igs | `comments.owner_pk` | `Comment.owner_pk`; `Comment.account_id` | key | yes |  |
| `post_taken_at` | The post's time | timestamp | 2026-11-01T18:30:00Z | always | derived by igs | `comments.taken_at` (partition key) | `Comment.post_taken_at` | key | no |  |
| `text` | Comment text | text | example comment text | always | comments query, and the first comments embedded in the post page (`text`) | `comments.text`; `comment_edits.text` (each replaced text) | `Comment.text`; `content.comment.v1 Comment.text` | versions | yes | As received; emoji kept. Mentions and hashtags in comments are not extracted. Empty when redacted. |
| `created_at` | When it was posted | timestamp (UTC) | 2026-11-01T19:00:00Z | always | comments query, and the first comments embedded in the post page (`created_at`, Unix s) | `comments.created_at` | `Comment.created_at`; `Envelope.published_at` | latest | no | Exact to the second. |
| `like_count` | Likes on the comment | int64 (optional) | 2 | always | comments query, and the first comments embedded in the post page (`comment_like_count`) | `comments.like_count`; `comment_metrics.like_count` | `Comment.like_count`; `Comment.likes`; `Metrics.likes` (target comment) | snapshots | no | Exact. |
| `child_comment_count` | Number of replies | int64 (optional) | 1 | always | comments query, and the first comments embedded in the post page | `comments.child_comment_count`; `comment_metrics.child_comment_count` | `Comment.child_comment_count`; `Comment.replies`; `Metrics.replies` | snapshots | no | **What igs keeps instead of replies:** Instagram serves no replies logged out, only their count. |
| `parent_comment_pk` | Parent comment (replies) | int64 (optional) | - | logged_in_only | comments query, and the first comments embedded in the post page (`parent_comment_id`) | `comments.parent_comment_pk` | `Comment.parent_comment_pk`; `Comment.parent_comment_id` | key | no | Always empty logged out. |
| `depth` | 0 = comment | int32 | 0 | always | derived by igs | `comments.depth` | `Comment.depth` | latest | no | Always 0 logged out. |
| `is_op` | Written by the post's account | bool | false | always | derived by igs (author pk = post owner) | `comments.is_op` | `Comment.is_op`; `extras.is_op` | latest | no |  |
| `status` | visible or deleted | enum | visible | always | derived by igs | `comments.status`, `comments.deleted_at` | `Comment.status`; `Comment.is_deleted` | latest | no | Detected after a complete read of the post's comments (REQ-008 fix 14): the stored comments it no longer returned become `deleted`, with their text kept, and are published with status deleted and no text. A comment seen again becomes visible. |
| `redacted` | Personal fields removed (erasure) | bool | false | always | derived by igs | `comments.redacted` | `Comment.redacted`; `extras.redacted` | latest | no |  |
| `edit tracking` | Edit count and time of the latest edit seen | int, timestamp | 1 | rare | derived by igs (text changed between reads) | `comments.edit_count`, `comments.edited_at` | a new `ig.comment.v1` version | versions | no | Instagram serves no edited flag on comments. |
| `giphy_media_info` | A GIF attached to the comment | object | - | unknown | comments query, and the first comments embedded in the post page | not stored | not published | - | no | Dropped. The `comment_gif` media kind exists but nothing fills it. |
| `other comment keys` | `has_translation`, `is_covered` (hidden behind a warning), `restricted_status`, `id` | mixed | - | always | comments query, and the first comments embedded in the post page | not stored | not published | - | no | Dropped. `id` duplicates `pk`. |

### comment_metrics (unified: comment_metrics)

**Fetched:** With each comment read. Stored on first sight and when a count changed; published on every read that carries a count.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `comment_pk` | The comment | int64 | 17900000000000001 | always | comments query, and the first comments embedded in the post page | `comment_metrics.comment_pk` | `content.metrics.v1 Envelope.native_id` (target comment) | key | no |  |
| `captured_at` | When igs read it | timestamp (UTC) | 2026-11-05T09:00:00Z | always | derived by igs | `comment_metrics.captured_at` (monthly) | `Envelope.observed_at` | key | no |  |
| `media_pk, owner_pk` | The post and its account | int64 | 3000000000000000001 | always | derived by igs | `comment_metrics.*` | `Metrics.parent_id` (the post) | key | no |  |
| `like_count` | Likes | int64 (optional) | 2 | always | comments query, and the first comments embedded in the post page | `comment_metrics.like_count` | `Metrics.likes` | snapshots | no |  |
| `child_comment_count` | Replies | int64 (optional) | 1 | always | comments query, and the first comments embedded in the post page | `comment_metrics.child_comment_count` | `Metrics.replies` | snapshots | no |  |

### comment_author (unified: author)

**Fetched:** With each comment.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `user_pk` | Author's account ID | int64 | 1000000002 | always | comments query, and the first comments embedded in the post page (`user.pk`) | `comment_authors.author_pk`; `comments.author_pk` | `Comment.author.user_pk`; `content.comment.v1 Author.author_id` | key | yes | Empty with `privacy.pii_mode: pseudonymized` (default `full`). |
| `username` | Author's handle | text | fake_user_3 | always | comments query, and the first comments embedded in the post page | `comment_authors.username` | `Author.username`; `content Author.name` | latest | yes | Display names aren't served with comments: `name` carries the handle. |
| `is_verified` | Blue tick | bool | false | always | comments query, and the first comments embedded in the post page | `comment_authors.verified` | `Author.is_verified`; `Author.verified` | latest | no |  |
| `profile_pic_url` | Author's picture | text (URL) | https://cdn.example.invalid/fake_author.jpg?oe=6A1B2C3D | always | comments query, and the first comments embedded in the post page | `comment_authors.profile_pic_url` | `Author.profile_pic_url`; `Author.picture_url` | latest | yes | Expires. |
| `author_key` | HMAC of the user pk | bytes (32) | - | always | derived by igs | `comment_authors.author_key`; `comments.author_key` | `Author.author_key` | key | no | Pseudonym; stable for the same HMAC key. |
| `profile_url` | Author's profile URL | text | https://www.instagram.com/fake_user_3/ | always | derived by igs | not stored | `content Author.profile_url` | latest | yes |  |
| `account type` | Person, business or creator | - | - | unknown | comments query, and the first comments embedded in the post page | not stored | not published | - | no | Not served with comments. A page or brand can't be told from a person here. |
| `is_unpublished` | - | bool | false | always | comments query, and the first comments embedded in the post page | not stored | not published | - | no | Dropped. |

### media (unified: media)

**Fetched:** Metadata for every post, carousel child, reel, highlight item and profile picture seen. Files are downloaded only for jobs that ask (`include_media`, **off**).

Sizes on the wire (measured): profile picture 7 to 13 KB. Post images and videos weren't downloaded, so their sizes are unknown.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `media_id` | igs's media ID (UUIDv5 of platform media and variant) | text | 00000000-0000-5000-8000-000000000001 | always | derived by igs | `media.media_id` | `ig.media.v1 Media.media_id`; `content.media.v1 Envelope.native_id` | key | no |  |
| `source_media_id` | Instagram's media pk | text | 3000000000000000001 | always | post page (HTML with embedded JSON) | `media.source_media_id` | `Media.source_media_id` | key | no |  |
| `kind` | photo, video, reel, thumbnail, cover, profile_pic, highlight (also defined, unused: comment_photo, comment_gif, sticker, comment_video) | enum | photo | always | derived by igs | `media.kind` | `Media.kind`; `content.media.v1 Media.type` (only `image` or `video`) | latest | no | The content contract loses the kind (a thumbnail reads as an image). |
| `variant` | Rendition: original, thumbnail, hd, sd | text | original | always | derived by igs | `media.variant` | `Media.variant` (not on `content.media.v1`) | latest | no |  |
| `owner` | owner_type (post, comment, profile, highlight), user_pk, media_pk, parent_media_pk, position, comment_pk | mixed | post | always | derived by igs | `media.*` | `Media.*`; `content.media.v1 owner_type` (post, comment, account), `owner_id`, `position` | key | no |  |
| `source_url` | CDN URL | text (URL) | https://cdn.example.invalid/fake.mp4?oe=6A1B2C3D | always | post page (HTML with embedded JSON); grid query (profile timeline); reels-tab query | `media.source_url` | `Media.source_url`; `content Media.url` | latest | no | Signed; refreshed when a re-read returns a new URL. |
| `source_expires_at` | When the URL stops working | timestamp | 2026-11-08T00:00:00Z | always | derived by igs (the `oe` parameter, hex Unix time) | `media.source_expires_at` | `Media.source_expires_at`; `content Media.url_expires_at` | latest | no | Typically a few days after the read. |
| `width, height` | Pixels | int32 | 1080 | often | post page (HTML with embedded JSON) | `media.width`, `height` | `Media.*`; `content Media.*` | latest | no |  |
| `duration_ms` | Video length | int64 (ms) | 41000 | always | post page (HTML with embedded JSON) | `media.duration_ms` | `Media.duration_ms`; `content Media.duration_ms` | latest | no | Videos only. |
| `mime` | Content type | text | video/mp4 | sometimes | derived by igs | `media.mime` | `Media.mime` (not on `content.media.v1`) | latest | no | Set from the downloaded file. |
| `alt_text` | Accessibility caption | text | Photo of example scene | often | post page (HTML with embedded JSON) | `media.alt_text` | `Media.alt_text`; `content Media.alt_text` | latest | no |  |
| `file lifecycle` | status, size_bytes, stored_at, ready_at, processed_at, deleted_at, delete_after | mixed | discovered | always | derived by igs | `media.*`; `media_acks` | `ig.media.ready.v1` (S3 bucket, key, size, SHA-256, content type); acknowledgements on `ig.media.processed.v1` | latest | no | Kept until every required consumer acknowledged (`media.ttl: 0`), then deleted. |

### hashtag_mention_tag_link (unified: other)

**Fetched:** Part of each post read (see Post).

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `hashtags` | From the caption | list of text | ["fakeTag"] | sometimes | derived by igs (regex) | `posts.hashtags` | `Post.hashtags`; `extras.hashtags` | versions | no | No positions. |
| `mentions` | From the caption | list of text | ["fake_user_2"] | sometimes | derived by igs (regex) | `posts.mentions` | `Post.mentions`; `extras.mentions` | versions | yes | Usernames, no positions or IDs. |
| `usertags` | Tagged accounts with IDs | list | - | sometimes (10%) | post page (HTML with embedded JSON) | `posts.usertags` | `Post.usertags`; `extras.tagged_ids` | latest | yes | 10%. |
| `coauthors` | Collaborators with IDs | list | - | rare (6%) | post page (HTML with embedded JSON) | `posts.coauthors`; `post_owners` | `Post.coauthors`; `Post.coauthor_ids` | latest | yes | 6%. |
| `links in captions` | URLs in caption text | - | - | rare | - | not extracted | not published | - | no | Instagram doesn't make caption URLs clickable; igs doesn't extract them. |
| `bio links` | See Account | list | - | sometimes | profile query (user by username) | `profiles.bio_links` | `Account.links` | latest | no |  |

### location (unified: location)

**Fetched:** Part of each post page.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `location` | pk, name, lat, lng | object | {pk: 200000001, name: "Example Place"} | sometimes (10%) | post page (HTML with embedded JSON) | `posts.location` (JSON) | `Post.location`; `extras.location` | latest | no | 10% of post pages. No separate location table; no address or city fields are served. |

### music (unified: audio)

**Fetched:** Part of each post page.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `music` | title, artist, audio_id, original_audio | object | {title: "Example Song"} | often (50%) | post page (HTML with embedded JSON) | `posts.music` (JSON) | `Post.music`; `extras.music` | latest | no | 50% of post pages. |

### highlight (unified: post)

**Fetched:** The list with each profile read (daily); items re-read only when the list changed (`highlights_hash`).

The highlights list (titles, covers) comes with the profile query and is real. Highlight items are unverified.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `highlight_id` | Highlight ID | int64 | 18000000000000001 | always | highlights list inside the profile query | `highlights.highlight_id` | `ig.highlight.v1 Highlight.highlight_id`; `extras.highlight_id` on each item | key | no |  |
| `title` | Highlight title | text | Example title | always | highlights list inside the profile query | `highlights.title` | `Highlight.title`; `extras.highlight_title` | latest | no |  |
| `cover` | Cover image | ImageRef | https://cdn.example.invalid/fake_cover.jpg?oe=6A1B2C3D | always | highlights list inside the profile query | `highlights.cover_url` | `Highlight.cover`; `ig.media.v1` kind `cover` | latest | no | Expires. |
| `item_count` | Items in the highlight | int32 | 5 | unknown | highlight items query | `highlights.item_count` | `Highlight.item_count` | latest | no |  |
| `items[]` | Each story in the highlight: item_pk, taken_at, media_type, media_ids | list | [{item_pk: 3100000000000000001}] | unknown | highlight items query | `posts` with `kind = highlight` | `Highlight.items`; each item a `content.post.v1` of kind `highlight` (url `/stories/highlights/<id>/`) | latest | no | **Not verified on real data**: the parser is tested on a hand-made fixture only. Whether items are served logged out is unknown. |

### story (unified: post)

**Fetched:** Not collected.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `stories` | Current 24-hour stories | - | - | unknown | - | not stored | not published | - | no | Not attempted. The profile query's `latest_reel_media` (time of the newest story) is dropped. Whether stories are served logged out is unknown; likely login-only. |

### job (unified: coverage)

**Fetched:** One event per finished job.

Not mapped to `content.*`: there's no `collection.coverage.v1` yet.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `job_id, kind, status, subject_ref, subject_id, created_at, finished_at` | The job | mixed | on_demand | always | derived by igs | `jobs.*` | `ig.job.v1 JobEvent.*` (`securebrand.common.v1`) | latest | no | status: queued, running, completed, partial, failed, cancelled. |
| `counters` | requests, wire_bytes, page_loads, blocked, posts, posts_detailed, posts_gated, posts_unavailable, posts_truncated, comments, comment_pages, reels, highlights, drift, transport_errors and more | map of int64 | {requests: 42} | always | derived by igs | `jobs.counters` | `JobEvent.counters` | latest | no |  |
| `completeness` | posts, posts_complete, posts_truncated, median_ratio, min_ratio, comments_reported, comments_collected | object | {median_ratio: 1.0} | always | derived by igs | `jobs.completeness` | `JobEvent.completeness` | latest | no | Comments collected against the count Instagram reports. |
| `errors, truncation reasons` | Why something is missing | list of text | ["timeline truncated: rate limit"] | sometimes | derived by igs | `jobs.error`; `crawl_cursors.truncated_reason` | `JobEvent.errors` | latest | no |  |

### discovery_candidate (unified: discovery_candidate)

**Fetched:** Continuously, when enabled: sightings as profiles are read; vets of the most-seen candidates (one profile read and 12 captions each, lane p2).

Nothing about discovery is published to Kafka today. `discovery.candidate.v1` doesn't exist yet.

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `username` | Candidate handle (lower case) | text | fake_user_4 | always | sightings in collected posts and comments | `discovery_candidates.username` | not published | key | yes |  |
| `user_pk` | Candidate ID once known | int64 | 1000000004 | often | profile query (user by username) (vet read) | `discovery_candidates.user_pk` | not published | latest | yes |  |
| `sightings` | tags, coauthors, mentions, comments, iraq_context (counts) | int | 3 | always | derived by igs | `discovery_candidates.*` | not published | latest | no |  |
| `sources` | Registered accounts it was met in | list of int64 | - | always | derived by igs | `discovery_sources` (username, source_pk, first_seen_at) | not published | snapshots | yes | The "Iraqi audience" signal. |
| `priority, score, signals` | Order to vet in; Iraq score and its parts | float, JSON | 4.5 | always | derived by igs (`internal/ig/discovery/score.go`) | `discovery_candidates.priority`, `score`, `signals` | not published | latest | no |  |
| `vet` | What the vet read found: found, private, user_pk, username, full_name, biography, category, links, followers, verified, business, captions | JSON | - | often | profile query (user by username); grid query (profile timeline) | `discovery_candidates.vet`; `discovery_vets.vet` (each read) | not published | snapshots | yes |  |
| `status` | new, vetting, registered, below, private, gone, failed, rejected, capped | enum | registered | always | derived by igs | `discovery_candidates.status`; `discovery_vets.status` | not published | versions | no | `private` is the second list (R8). `registered` adds the account under the API client `discovery`. |
| `times` | first_seen_at, last_seen_at, vetted_at, decided_at | timestamp | - | always | derived by igs | `discovery_candidates.*` | not published | latest | no |  |
| `keywords` | The editable Iraq list (term, normalized, language en/ar/ckb/kmr, group, enabled) | table | fake keyword | always | `configs/discovery/iraq.yaml` (fbs's list) and the API | `discovery_keywords` | not published | latest | no | Read every 5 minutes. |

### other (unified: other)

**Fetched:** -

| Field | Meaning | Type | Synthetic example | Present | Source | Stored at | Published at | History | PD | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| `erasure events` | A person's or profile's data erased | message | - | rare | API | `erasure_requests`; `suppression` | `ig.erasure.v1` | versions | yes | Dormant unless used. |
| `live, playlists, community posts` | - | - | - | unknown | - | not stored | not published | - | no | Not on Instagram (live: not collected). |
| `tagged tab` | Posts others tagged the account in | - | - | logged_in_only | the profile's tagged tab | not stored | not published | - | no | Tested 5 Nov: every visit redirected to the login page. Stays out of igs. |
| `raw responses` | Each answer igs received | zstd JSONL line: at, query, class, vars, body or text | - | always | every request | S3 raw bucket `raw/` (all, `capture_raw: all`) and `raw-failures/` | `Observation.raw_ref` = `s3://bucket/key#line` | snapshots | yes | No parser version on the line (it's on the event). Kept forever (`raw_payload_days: 0`). |

## 3. Answers

### 1. IDs

- Account `user_pk`, post `media_pk`, comment `comment_pk`, highlight `highlight_id` and location `pk` are numeric (int64), and stable. igs's content events carry them as text.
- One object, two IDs: a post has `media_pk` and `shortcode` (convertible; both stored). A profile's `id` equals its `pk`. Comments have `pk` and `id` (the same number).
- Media IDs are igs's own UUIDv5 values, derived from the media pk and variant, so they're the same on every read.
- Handles change. A rename is seen when the same `user_pk` returns a new username: igs writes a snapshot row and publishes a new version. A registration by handle that answers "no user" is marked unavailable, because renamed, deleted and misspelled all look the same.

### 2. Time

- Post, comment and highlight-item times are exact Unix seconds (UTC). No relative times ("2d ago") appear in what igs reads.
- A post's time can also be derived from its pk; igs uses that when only the pk is known (highlight items, reels).
- No edit times are served; igs records when it first saw the new text (`post_edits.replaced_at`).

### 3. Text

- Instagram's limits: captions 2,200 characters, bios 150. igs hasn't measured the longest comment it has seen. igs doesn't truncate anything.
- Line breaks and emoji are kept as received (UTF-8). Arabic and Kurdish are stored as received; normalization is only computed for matching.
- No mention or hashtag positions are served: igs extracts hashtags and mentions from the caption with a regular expression, without positions.
- No language field and no translations are served (comments carry a `has_translation` flag, dropped).
- Text is never cut ("see more") in the answers igs reads.

### 4. Counts

- Followers, following, posts, likes, comments, reel views and comment likes are exact integers. Nothing in these answers is rounded.
- Owners can hide likes. Instagram then returns 0 to 3. igs stores no likes (NULL) and sets `counts_hidden` (3% of post pages, 1.9% of reels). Views and comments stay real.
- Absent counts are left empty, never 0.

### 5. Edits and deletions

- Caption edits: detected when a re-read (within 7 days) returns a new caption. The old one goes to `post_edits`, and a new version is published.
- Comment edits: detected the same way (`comment_edits`, `edit_count`, `edited_at`).
- Bio and link changes: a new version is published; Postgres keeps the latest only.
- Post deletion: a re-read that finds the post gone marks it `unavailable` (deleted, archived or private; Instagram doesn't say which). Within the 7-day refresh window, and at the post's one full re-read at 30 days (REQ-008 fix 14).
- Comment deletion (REQ-008 fix 14): after a read of a post's comments from the first page to the last, whose comments and reply counts add up to the post's count, the stored comments it no longer returned are marked `deleted` (text kept) and published. Such reads: a refresh that finds fewer comments than before, full runs, and the 30-day re-read.
- Speed: as fast as the re-read schedule: about 3 h, 9 h, 21 h, 45 h and 93 h after posting on the 3-hour tier.

### 6. Relationships

- A comment carries its post's `media_pk` and the post's owner; its Kafka key is the post.
- Replies: none logged out. The comment's `child_comment_count` is kept; `parent_comment_pk` and `depth` are always empty and 0.
- Co-authors: `Post.coauthors` and `post_owners` (role coauthor). A collaboration appears in each co-author's reels tab.
- Tagged accounts: `Post.usertags`, with their IDs.
- Shares, quotes and duplicates: Instagram has none to read logged out.
- Cross-posts: the profile's linked Facebook page name only (its ID is dropped today). No per-post cross-post flag is read.

### 7. Authors

- A comment's author: ID (pk), handle, verified flag and picture URL. No display name and no account type, so a brand can't be told from a person from a comment alone.
- Authors get an HMAC `author_key`. With `privacy.pii_mode: pseudonymized`, the ID and handle aren't kept (default: `full`).

### 8. Media

- Kinds: photo, video (feed and reel), carousel children, thumbnail, highlight cover, highlight items, profile picture. Renditions: photo original and thumbnail; video HD, SD and thumbnail.
- Sizes: profile pictures 7 to 13 KB (measured). Post media weren't downloaded, so their sizes are unknown.
- All CDN URLs are signed and expire a few days after the read (the `oe` parameter). igs records the expiry time.
- Downloaded today: nothing (`include_media: false`). Metadata is always published. The downloader, the acknowledgements and deletion after analysis are built and tested on fakeig.
- What analysis would need: thumbnails or first frames for images and video, and audio for speech. To agree with the analysis session (Q8).

### 9. Iraq signals

- Account level (discovery's score): bio and name text in Arabic, Kurdish (Sorani letters), Iraqi dialect; Iraqi cities and governorates in three scripts; +964 or 07xx phone numbers; `.iq` links; post locations in Iraq; how many registered Iraqi accounts link to it. Another country without Iraq counts against.
- Post level: location (10% of posts), caption language and keywords, tagged and co-author accounts.
- Reliability: location is precise but rare; phone and `.iq` are strong but rare; keywords and dialect are common but noisy (fixed false positives such as words that only contain a city name). The Iraqi-audience count is the strongest signal for foreign accounts aimed at Iraq. Not measured as precision or recall.

### 10. Profile links

- Bio: yes. Every bio link (URL and title): yes. Website: as the first bio link (no separate `external_url` is served). Linked Facebook page: the name only; the ID is dropped today, fix planned.
- No history in Postgres beyond the latest; each change is a new event version.

### 11. Raw responses

- Every answer, not just failures (`capture_raw: all`): zstd-compressed JSON lines in the raw S3 bucket (`raw/`, and `raw-failures/`). Each event's `raw_ref` points at its line (`s3://bucket/key#line`).
- Kept forever (`raw_payload_days: 0`). The line has no parser version (it's on the event).
- Measured sizes on the wire: page load 132 to 138 KB, post page 117 to 119 KB, grid page 10 KB (12 posts) or 24 KB (50), comments 4 to 9 KB, profile 5 to 6 KB, reels page 16 KB (50 reels).

### 12. Volumes (per account per day)

- Posts: about 2.1 (measured: 269,703 posts from 348 accounts over a year).
- Comments: median 5 to 6 per post (measured on post pages and reels), but very skewed: estimate 10 to 200 per account per day.
- Metrics observations: per reel on every visit (a year back) plus about 5 post re-reads per new post on the 3-hour tier. Estimate: 8 visits x the account's reels in the last year, stored only on change. Not measured.
- Media files: none downloaded today. If downloaded: about 2 posts' media a day, size unknown.
- Bytes on the wire: see `igs-operations.md` section 13; estimate 1 to 5 MB per account per day, depending mostly on how many reels the account has.

### 13. Privacy

- Personal data: account and author IDs, handles, names, bios, pictures, comment text, tagged and mentioned accounts, pronouns.
- HMAC: `author_key` for every author and profile (`privacy.author_hmac_key`). `pii_mode: pseudonymized` drops author IDs and handles; the default is `full`.
- Erasure: built and dormant (`ig.erasure.v1`, suppression list); used only on request.

## 4. Not available logged out

- replies to comments (only the reply count is served)
- views on photos and carousels; shares, saves and reposts on everything
- the tagged tab (login page on every visit, tested 5 Nov)
- account category, business flag and contact fields (absent from every logged-out answer captured)
- comment author display names and account types
- a few post pages (2.6%) behind a login wall

## 5. Planned (not built)

Proposals only; each needs the owner's approval. All are additions: nothing is renamed or removed.

| Field | Entity | Why | Change |
|---|---|---|---|
| linked Facebook page ID | account | Cross-platform linking. **Done (REQ-008 fix 9).** | `Profile.linked_facebook_page_id` and a link with `platform` `fb` on `content.account.v1`; the name no longer in `Link.url`. |
| is_business / category as optional | account | `false` and empty mean unknown today. | Added optional fields; old ones kept. |
| post metrics from post pages | post_metrics | Photos and carousels have no metrics series. | Publish `ig.post_metrics.v1` (likes, comments) from each post-page read. |
| comment deletions | comment | E7. **Done (REQ-008 fix 14).** | After a complete re-read (refresh window, or the one at 30 days), comments that are gone are published as deleted. |
| walled posts | post | 2.6% of posts publish nothing. | Publish the grid's fields with a `gated` reason. |
| parser version on raw lines | raw | E8 asks for it. | Add `parser` to each raw line. |
| bio and link history | account | Only the latest is in Postgres. | A `profile_edits` table, like `post_edits` (optional: the event stream and ClickHouse may cover it). |
| comment GIFs | comment | Dropped today. | Media of kind `comment_gif` from `giphy_media_info`. |
