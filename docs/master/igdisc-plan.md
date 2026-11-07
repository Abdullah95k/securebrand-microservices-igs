# Instagram keyword discovery (igdisc): the plan

By the igdisc session, 6 November 2026 (Iraq time, UTC+3), from `Abdullah95k/securebrand-microservices-igs` `main` at `a788e90`, for the owner and the master session (REQ-002).

**This is a plan only.** Nothing is built. Nothing was run against Instagram or the shared pilot server. Every example is synthetic.

Read with:
- the contract v1.0.0 ([`securebrand-contracts`](https://github.com/Abdullah95k/securebrand-contracts/releases/tag/v1.0.0), `proto/securebrand/discovery/v1/candidate.proto`);
- fbs's shared keyword list (`configs/discovery/iraq.yaml`, identical in fbs and igs);
- igs's discovery runbook ([`igs-operations.md` § Profile discovery](../runbooks/igs-operations.md#profile-discovery)) and igs's gaps file ([`igs-gaps.md`](igs-gaps.md));
- the owner's recording steps ([`docs/igdisc/har-recording.md`](../igdisc/har-recording.md)) and igdisc's status ([`docs/igdisc/status.md`](../igdisc/status.md)).

---

## In short

1. **It lives in the igs repository,** in its own folder (`internal/ig/igdisc`), its own migrations folder, its own docs folder, its own branch and pull requests. It reuses igs's identities, exits and pacing, vetting, Iraq score, keyword table, fake Instagram and registration. A few of igs's own files need small hooks; each one goes to the igs session as a separate small PR.
2. **Logged-out keyword search on Instagram most likely needs a login now.** The best evidence (a scraping vendor's changelog) dates the hashtag pages' move behind a login to 9 October 2024. The search box, keyword results and location pages look the same, but nothing here is measured yet. Your recording (option a) will settle it. If it confirms a login wall, **igdisc will not search Instagram by keyword at all,** because getting past a login wall is out (your decision of 6 November).
3. **So the plan has three tracks:**
   - **Track 1 always works, and costs no new requests to find accounts.** igs already reads every registered account's newest posts every 3 hours, with their comments. igdisc matches those captions, comments and post places against the shared keyword list. Accounts met in a matching post or comment in the last 6 hours, and not yet listed, become candidates with the keyword as evidence.
   - **Track 2 (hashtag and location pages) gets built only if your recording shows they work logged out.**
   - **Track 3 is outside Instagram:** search engines index public posts of professional Instagram accounts (since 10 July 2025). No session on the platform runs a search-engine tool (master, 6 November). Any Instagram candidates other tools send arrive as ordinary candidates, and igdisc treats them like any other origin.
4. **One Iraq check:** igs's existing score, with the keyword evidence added as a small, capped signal. A keyword match alone never adds an account.
5. **What it publishes:** `discovery.candidate.v1`, once when an account is found (outcome PENDING) and once when it's decided. Every field is mapped in §3.6. **Nothing is published or registered until the platform creates the topic.**
6. **Priority:** a new lowest lane, p3, below backfill. It gets no guaranteed slots, at most a set share of each exit's rate (10% proposed), and it pauses whenever regular collection falls behind. That limits speed, not numbers: **no caps in production;** the `CAPPED` outcome appears only in tests and the canary.
7. **Private accounts go to the second list,** as `OUTCOME_SECOND_LIST` with scope `PROFILE_ONLY`. They are never scheduled for regular collection.
8. **Track 1 cost:** about 2 to 3 requests per new account checked, and nothing per keyword. **If Track 2 works:** about 8 to 16 requests per hashtag form per day, roughly 2,600 to 5,200 a day for the whole list. That's 12% to 24% of one exit's day, before vets.

---

## 1. Where it lives

### 1.1 What igdisc reuses from igs

| igs part | Where | What igdisc uses it for |
|---|---|---|
| Logged-out identities, exits, pacing (`exit_rps` 0.25, `identity_rps` 0.33), rests, breakers, AIMD, daily budget | `internal/scrape`, `internal/crawl`, `internal/ig/client` | Every Instagram request igdisc makes (Track 2) and every vet |
| Lanes and slots | `internal/ig/app`, `internal/ig/topics` | A new lane p3, below backfill (§3.3) |
| Vetting: a profile read plus 12 captions, 2 or 3 requests, publishing nothing | `internal/ig/discovery/vetter.go` | Checking every account igdisc finds |
| The Iraq score | `internal/ig/discovery/score.go` | The one relevance check (§4) |
| Candidate store, origins, intake, accept and reject, erasure | `ig.discovery_candidates`, `discovery_vets`, `discovery_sources`, `intake.go` | igdisc's candidates join the same store, with origin `keyword` (Track 1), `hashtag` or `location` (Track 2), or the outside tool's name (Track 3) |
| The shared keyword table, the spelling fold and the word matcher | `ig.discovery_keywords`, `internal/keywords` | Matching captions and comments (Track 1); the hashtag forms (Track 2) |
| Registration | `dispatch.Register`, the API client `discovery` | Adding accounts that pass |
| fakeig, the e2e harness, Compose, the Helm chart, alerts and dashboards | `internal/fakeig`, `internal/ig/e2e`, `internal/testenv`, `deploy/` | Tests and deployment |

### 1.2 What igdisc adds

| New part | Where | Track |
|---|---|---|
| **Keyword sightings:** match captions, comments and post places against the keyword list, and record the evidence | `internal/ig/igdisc` (built: `recorder.go`, `keywords.go`) | 1 |
| **The evidence table** `ig.igdisc_evidence`: account, keyword ID, language, the matching post or comment, its time, how it was met. Kept forever; erasure deletes it | `migrations/igdisc` | 1, 2 |
| **Hashtag forms:** each keyword written as Instagram hashtags (`Sadr City` becomes `sadrcity`; `مدينة الصدر` becomes `مدينة_الصدر` and `مدينةالصدر`), deduplicated | `internal/ig/igdisc/forms` | 2 |
| **Search stage and planner:** one search per hashtag form or Iraqi place every interval, lane p3 | `internal/ig/igdisc/search`, `internal/ig/igdisc/plan` | 2, only if the recording allows |
| **Iraqi places table** `ig.igdisc_locations`, built from the post places igs already reads: places inside Iraq whose names match the place list | `migrations/igdisc`, `internal/ig/igdisc/plan` | 2 |
| **Candidate publisher:** `discovery.candidate.v1`, found and decided, checked by the contract's `conformance` package | `internal/ig/igdisc/candidate` | all |
| **Priority guard:** lane p3, the exit share, the pause rule | `internal/ig/igdisc/guard`, with a hook in the crawler | all |
| **fakeig search pages:** hashtag, location and top-search answers, each with a login wall switch | `internal/fakeig/search.go` (a new file) | tests |
| Metrics `igs_igdisc_*`, alerts, a runbook section | `internal/ig/igdisc`, `deploy/helm/igs`, `docs/igdisc/` | all |

### 1.3 Keeping out of the igs session's way

- **My folders:** `internal/ig/igdisc/` (under `internal/ig/` so igs's integration job, which tests `./internal/ig/...`, runs igdisc's database tests without a change to CI), `migrations/igdisc/` (its own goose version table, so migration numbers never clash with igs's `migrations/ig/`), `docs/igdisc/`, and this file in `docs/master/`.
- **My branch:** `claude/focused-gauss-hdwmvo`. The igs session works on `claude/ecstatic-hypatia-zj27mg`. Every PR is a draft until the owner approves.
- **Keys and names:** tables `ig.igdisc_*`, metrics `igs_igdisc_*`, Valkey keys `igs:igdisc:*`, config section `ig.igdisc`.
- **Hooks in igs's own files.** Each is a separate small PR for the igs session to review (your approval of 6 November):

  | Hook | igs file | Size |
  |---|---|---|
  | Call igdisc's matcher after a post or comments are parsed | `internal/ig/discovery/collector.go` | a few lines |
  | Origin `keyword`, and evidence-aware vet priority | `internal/ig/discovery/vetter.go` | small |
  | Call the candidate publisher on each decision | `internal/ig/discovery/vetter.go` | a few lines |
  | Lane p3 (no guaranteed slots) | `internal/ig/topics`, `internal/ig/app/crawler.go`, `api/proto/securebrand/igs/task/v1` (an added enum value) | small |
  | The `search` stage and the planner loop (Track 2 only) | `cmd/igs-crawler`, `cmd/igs-scheduler` | a few lines each |
  | Config keys `ig.igdisc.*` | `configs/default.yaml`, `internal/config` | small |
  | A crawler-search Deployment, off by default | `deploy/helm/igs` | small |

- **Overlap to agree with the igs session:** igs's own gaps file proposes an exit share and a pause rule for vets (`igs-gaps.md`, Plans 1, items 3a and 3b). igdisc needs the same thing. It should be built once, by whichever session goes first. The same file's proposed daily caps (Plans 1, item 1) are overtaken by your no-caps decision.

---

## 2. What a logged-out visitor can search on Instagram today

**Nothing in this section has been measured yet.** It's desk research plus what igs has measured itself, and your recording (option a) will confirm or correct it. Third-party claims are marked as such.

| Surface | Address | Logged out? | What it returns | Evidence |
|---|---|---|---|---|
| **Hashtag page** | `/explore/tags/<tag>/` | **Most likely no: a login wall** | Logged in: top posts and reels for the tag. The "Recent" tab, which listed the newest posts, was tested away in 2022 and may be gone for everyone | Apify's scraper changelog: Instagram put hashtag pages fully behind a login on 9 October 2024, and Apify switched to Google search for hashtags. Other vendors still claim logged-out access. Not confirmed by Meta |
| **Search box** (accounts, hashtags and places as you type) | the search panel; `/web/search/topsearch/?query=` | **Unknown, likely no** | Logged in: matching accounts, tags and places, not posts | One 2026 blog post claims the old endpoint works without a login; nothing confirms it |
| **Keyword results** (posts whose captions match a word, not just a tag) | `/explore/search/keyword/?q=` | **Most likely no** | Logged in: a grid of matching posts and reels, ranked, not by time | Launched in the app in 2020 and widened since. In 2025, tapping a hashtag started opening a full search results page. No logged-out evidence |
| **Location page** | `/explore/locations/<id>/<slug>/` | **Unknown, likely no** | Logged in: top and recent posts at a place | A 2019 report said location pages began requiring a login. Vendors' location scrapers today say a session may be needed |
| **Audio page** | `/reels/audio/<id>/` | Unknown | Reels using a sound | Nothing found |
| **Explore page** | `/explore/` | Most likely no | Recommended posts | General guides say you need to be logged in |
| **Suggested or similar accounts** on a profile | profile page | Unknown | Accounts related to that profile | Not in igs's field inventory; check in the recording |
| **Tagged tab** | `/<username>/tagged/` | **No: measured** | | igs's test on 5 November (U7): the tab sent the visitor to the login page |
| **Profile, grid, reels tab, post page, comments** | the pages igs reads | **Yes: measured daily** | Everything igs collects | igs's recordings (30 October), probe and Phase 0 (348 profiles, 0.62% blocks) |
| **Search engines** (outside Instagram) | Google, Bing | Yes, no Instagram login involved | Public posts of professional (business and creator) accounts, indexed since 10 July 2025; personal accounts and stories are not indexed. Indexing lags, so a post may be found hours or days after it appears | Many reports of the 10 July 2025 change. Google's Custom Search JSON API is closed to new customers and ends on 1 January 2027. Bing's search APIs were retired on 11 August 2025. So a search-engine route needs another search provider |

**Plainly:** keyword search, hashtag search and location search on Instagram very probably need a login today. If your recording confirms that, igdisc can't find accounts by searching Instagram, and Track 1 (§3.1) becomes its main source. The other two ways to search Instagram by keyword would be a logged-in account, which is ruled out, or a third-party service that does the search for us. I don't propose the second: we wouldn't know its method or its terms, and the data would leave our control. I won't pursue either unless you ask.

### 2.1 How to measure it safely

1. **Your recording (option a; your choice of 6 November):** a logged-out browser on your own connection, about 15 minutes, one HAR file per surface, following [`docs/igdisc/har-recording.md`](../igdisc/har-recording.md). That puts no load on igs's exits and is exactly what a visitor sees. I'll write only request shapes, statuses, counts and times into a findings note, as igs did with C1. The files stay out of git.
2. **Only if the recording shows something works logged out, and only with your approval: a tiny probe on production** (D-026):
   - one new identity, one exit, at most 30 requests, on lane p3;
   - in a quiet hour (03:00 to 05:00 Iraq time);
   - it stops at the first wall or rate limit;
   - it records status, wall or not, the number of posts, the newest and oldest post times, and the bytes;
   - it stores no posts or accounts, only those counts;
   - never on the shared server.
3. **Then a canary week** on production (D-012), with a test-only cap (§3.8), before it's uncapped.

---

## 3. The plan

### 3.1 Components, by track

**Track 1: keyword sightings in what igs already reads.** Built whatever the recording says.
- When the crawler parses a post or a page of comments, igdisc's matcher checks:
  - the caption;
  - each comment;
  - the post's place name.
- It checks them against the enabled keyword list (the same table and the same word matcher discovery uses, reloaded every 5 minutes).
- Only content created in the last `igdisc.window` counts. That's 6 hours by default, following fbs's window and R8.
- For each match it records evidence for the accounts met there that aren't listed yet:
  - the author of a matching comment (only with `pii_mode: full`, as discovery already requires);
  - accounts tagged, mentioned or co-authoring in a matching caption;
  - every non-listed account on a post placed in Iraq.
- Each piece of evidence raises the account's vet priority, so it's checked soon.
- The vet and the score then decide (§4).
- **Cost:** no new requests to find an account. Each new account checked costs a vet, 2 or 3 requests.
- **Limit:** Track 1 only sees what happens around accounts already on the list. Network discovery (M11) has the same limit. That's why Tracks 2 and 3 exist.

**Track 2: hashtag and location pages.** Built only if the recording shows they work logged out.
- The planner (the leader scheduler) queues one search per hashtag form, and per Iraqi place, every `interval` (3 hours proposed). It skips a search whose last run hasn't finished, as fbs does.
- The `search` stage reads the page, or its query, on lane p3. It reads the first page, plus one more if the oldest post on the first is still inside the 6-hour window.
- Each post in the window gives its owner:
  - already listed: recorded as `ALREADY_LISTED`, at most once per account per day;
  - not listed: a candidate with the hashtag or place as evidence.
- **Iraqi places:** built from the post places igs already reads. A place goes in when its coordinates fall inside Iraq and its name matches the place list. Turning places on and off works like keywords.

**Track 3: from outside Instagram.**
- Instagram accounts that other tools find arrive as candidates for igs's collector: links that fbs, yts and tks find, and any outside tool's finds. The Google discovery tool is outside the platform's scope (master, 6 November).
- Today they arrive through igs's intake (`POST /v1/discovery/candidates`). Once the platform creates the topic, they'll come from `discovery.candidate.v1` with `target_platform: ig`.
- Consuming that topic is the igs collector's side. I'll propose it to the igs session; I won't build it in igs's code myself.

### 3.2 Requests and proxy cost

**Assumptions** (to be corrected by the recording and the canary):
- 332 enabled spellings in the shared list today (52 more are off), which make about 324 distinct hashtag forms;
- a search answer is about 30 KB, like igs's other query pages (10 to 50 KB);
- igs's pace: 0.25 requests a second per exit, so **one exit gives about 21,600 requests a day**;
- a vet is 2 or 3 requests, about 60 to 100 KB.

| Track | Per keyword per day | Whole list per day | Bytes per day | Share of one exit's day |
|---|---|---|---|---|
| 1. Keyword sightings | 0 requests | 0 | 0 | 0% |
| 2. Hashtag pages, every 3 hours, 1 or 2 pages each | 8 to 16 requests | 2,600 to 5,200 | 80 to 160 MB | 12% to 24% |
| 2. Iraqi places, say 200 places, same rhythm | 8 to 16 per place | 1,600 to 3,200 | 50 to 100 MB | 7% to 15% |
| **Vets** (every track): per new account checked | | 1,000 new accounts a day gives 2,500 to 3,000 requests | 60 to 100 MB | 12% to 14% |
| Identity start-ups (one page load, 132 KB, per identity per 500 to 750 requests) | | 5 to 25 | about 3 MB | negligible |

- **What it costs:** igs runs on ISP exits priced per IP, so the real cost is **exit time**, not bandwidth.
  - At the proposed 10% share of each exit, one exit gives discovery about 2,160 requests a day.
  - Track 2's hashtags then need the shares of 2 to 3 exits, the places 1 to 2 more, and 1,000 vets a day 1 to 2 more: about 6 exits' shares at most.
  - Spread across a production fleet, that is a few percent of its capacity.
  - If an exit is billed by the GB, Track 2 plus vets is about 0.2 to 0.4 GB a day.
- **The number of new accounts a day isn't known.** It's high at the start and falls as the list fills up. The canary will measure it: `igs_igdisc_found_total` and `igs_discovery_vets_total`.

### 3.3 The priority rule

Discovery must never take the sessions or exits regular collection needs. Four guards, all on by default:

1. **Lowest lane.** A new lane p3, below p0 (on demand), p1 (scheduled) and p2 (backfill).
   - It gets slots only when no other lane is waiting.
   - Its `lane_min` is 0, unlike the other lanes' 12 guaranteed slots, so it never holds a slot another lane wants.
   - Its tasks yield first.
   - Vets of igdisc's candidates also run on p3. Network discovery's vets stay where igs has them.
2. **Exit share.** A separate rate key in Valkey gives p3 at most `igdisc.exit_share` (10% proposed) of each exit's `exit_rps`. Regular work keeps the rest, even when discovery has a backlog.
3. **Pause rule.** The planner issues no searches and no p3 vets while any of these hold:
   - the schedule is more than one interval behind;
   - most exits of a tier are resting;
   - the daily budget is holding backfill.
4. **Own identities.** igdisc's searches use identities tagged for discovery only. A wall caused by a search then retires a discovery identity, not one that regular collection is using. If the recording or canary shows that search surfaces draw walls faster than igs's pages do, a dedicated exit tier for discovery is the next step (`igs-gaps.md` Plans 1, item 3c).

**Tests** (§3.5) show the guards hold: a discovery backlog doesn't delay a scheduled run.

### 3.4 The relevance check

One check for every source: igs's existing score (`score.go`), described in §4. The keyword evidence adds a small, capped signal. The threshold stays 4, so a keyword match alone never adds an account.

### 3.5 The fake Instagram for tests

- **fakeig gains a new file, `internal/fakeig/search.go`:**
  - hashtag pages, location pages and top search, for its synthetic world;
  - posts with times spread over the last day;
  - a login-wall switch per surface, so tests cover both "works logged out" and "walled";
  - ground truth: which synthetic accounts mention which keywords.
- **Tests:**
  - `TestIgdiscKeywordSightings`: comments and captions in Arabic, Sorani, Kurmanji and English find the right accounts, and content older than 6 hours doesn't count.
  - `TestIgdiscAlreadyListed`: listed and rejected accounts aren't candidates again.
  - `TestIgdiscPrivateToSecondList`.
  - `TestIgdiscPriority`: under a discovery backlog, scheduled runs keep their time.
  - `TestIgdiscWallStops`: a walled surface stops its searches and alerts, and nothing tries to get past it.
  - `TestIgdiscCandidateEvents`: every event passes the contract's `conformance.Candidate` check with `Production: true`. `CAPPED` is accepted only in test mode.
  - `TestIgdiscHashtagSearch` and `TestIgdiscLocationSearch` (Track 2 only).
  - `TestIgdiscErasure`: evidence rows go with the person.
- **CI:** these run in igs's existing jobs, against fakeig and Redpanda in CI, never a shared Kafka.

### 3.6 What it publishes, and how it maps to the contract

**Topic:** `discovery.candidate.v1`, plain protobuf, headers `sb-type` and `sb-contract: 1.0.0`, key: target platform and account reference.

**When:** as the contract says, the source publishes when it finds an account, and the target platform's collector publishes again with the outcome. Here both are igs's repository:
- igdisc publishes **found** (outcome `PENDING`) when an account first becomes a candidate from a source;
- igs's vetter publishes **decided** with the outcome;
- an account already listed gets one `ALREADY_LISTED` event per source per day at most, so the platform can see how much discovery overlaps the list without a flood.

**Before the topic exists:** nothing is published. The flag `ig.igdisc.publish` stays false, and candidates flow inside igs's own database. igdisc never creates topics or registers subjects; the platform does both (contract README §11).

| Field | igdisc's value | From |
|---|---|---|
| `event_id` | A hash of found-by, account, source, evidence and outcome, so a repeat delivery has the same ID | computed |
| `target_platform` | `ig` | |
| `account` | `id` `ig:account:<user pk>` once known; `native_id` the pk; `handle` the username; `url` `https://www.instagram.com/<username>/`; `display_name` and `verified` from the vet | the post or comment, then the vet |
| `found_at` | When igdisc saw the matching content (UTC) | the crawl |
| `found_by` | `igdisc` (keyword, hashtag, location); `igs` for network discovery; the outside tool's name for Track 3 | |
| `source_platform` | Empty (found on Instagram), or the other platform for Track 3 | |
| `source` | Track 1: `SOURCE_COMMENTER`, `SOURCE_TAG`, `SOURCE_MENTION` or `SOURCE_COAUTHOR`, with keyword evidence. Track 2: `SOURCE_HASHTAG_PAGE`, or `SOURCE_LOCATION_PAGE` (18, contract v1.1.0) | |
| `evidence[]` | `keyword_id` (`contracts.KeywordID(group, language, spelling)`, contract v1.1.0); `keyword` (the spelling); `content_time` (when the matching post or comment was made, v1.1.0); `language` (`en`, `ar`, `ckb`, `kmr`); `content_id` `ig:post:<media pk>` or `ig:comment:<comment pk>`; `url` the post's link; `sightings` and `distinct_sources` from the candidate store; `via` `caption`, `comment`, `post_place`, `hashtag_page` or `location_page` | `ig.igdisc_evidence` |
| `relevance` | `score`; `relevant` (score at or above the threshold); `matched_keyword_ids`; `matched_rules` from the score's parts: `named-iraq`, `phone-964`, `tld-iq`, `place-<governorate>`, `kurdistan`, `lang-ckb`, `dialect-iq`, `script-arabic`, `audience-<n>`, `located-IQ`, `coauthor`, `other-country` | the vet's score |
| `is_private` | From the vet; unset until then | the vet |
| `outcome` and `outcome_reason` | See the next table | the vetter |
| `scope` | `FULL` when added; `PROFILE_ONLY` for the second list | |
| `decided_at` | When decided | |
| `provenance` | `collector` `igs`; version; parser version; `source_operation` (the query or page read) and `source_variant` (its doc_id); `job_ids` (the vet job); `raw_ref`; `exit_country` | the crawl |

| igs's decision | `outcome` | `outcome_reason` |
|---|---|---|
| registered | `OUTCOME_ADDED` | |
| already on the schedule | `OUTCOME_ALREADY_LISTED` | |
| private, and passes the Iraq check from its profile | `OUTCOME_SECOND_LIST` | `private` |
| private, and doesn't pass | `OUTCOME_NOT_RELEVANT` | `private` |
| below the threshold | `OUTCOME_NOT_RELEVANT` | `not_relevant` |
| no such account | `OUTCOME_DORMANT` | `gone` |
| public, no posts | `OUTCOME_DORMANT` | `never_posted` |
| the vet failed after its retries | `OUTCOME_UNAVAILABLE` | `unreadable` |
| rejected by the owner | `OUTCOME_EXCLUDED` | |
| held by a cap (tests and canary only) | `OUTCOME_CAPPED` | |

### 3.7 Private accounts (R8)

- A private account is never scheduled for regular collection.
- The vet can read only its profile (name, bio, counts, picture), so its score comes from the profile alone.
- **Proposal:** a private account that passes the Iraq check from its profile goes to the second list (`OUTCOME_SECOND_LIST`, scope `PROFILE_ONLY`); one that doesn't is `NOT_RELEVANT`. **Question O2** (§7): should every private account found go to the second list, whatever the check says?

### 3.8 No caps in production

- Production runs with no limit on how many accounts discovery adds: `max_new_per_day` stays 0.
- The contract's conformance check rejects `CAPPED` in production, and igdisc runs that check in CI with `Production: true`.
- Caps exist for tests and the canary only. The canary week runs at, say, 50 a day so the owner can review what is added, and then the cap goes to 0.
- The guards in §3.3 slow discovery down when regular collection needs the exits. They never drop a candidate: a candidate waits and is checked later.

### 3.9 Data kept

- **Kept forever** (D-004, M11b): every piece of evidence, every vet and every decision.
- **Erasure:** an erasure request deletes a person's rows, evidence included.
- **What isn't kept:** search pages themselves, beyond igs's usual raw capture. No account or post that is only found reaches `content.*` or analysis. Only registered accounts do, as today.

---

## 4. Judging Iraq relevance

**Today's check:** igs scores what the vet reads: the profile and 12 recent captions. Network evidence (who links to the account) adds at most 4. An account is added at 4 points.

None of the reliabilities below has been measured as precision or recall. They are judgements from how each signal fails, and the canary will measure them (end of this section).

| Signal | Points today | Reliability | How it goes wrong |
|---|---|---|---|
| Iraq named (any script) or 🇮🇶 in the name or bio | 3 | **Strong** | A foreign news or charity account about Iraq; a "not Iraq" contrast. Rare |
| Iraqi phone: +964 or 00964 | 3 | **Strong** | Almost never |
| Iraqi mobile written 07xx… (11 digits) | 3 (same rule) | **Medium.** A finding for the igs session: a UK mobile written without spaces (07700900123, synthetic) also matches the pattern | UK and other 07 numbers |
| `.iq` link | 2 | **Strong** | Rare |
| A place in an Iraqi governorate in the name or bio | 2; 3 for two governorates | **Strong to medium** | Names shared with places elsewhere ("Kurdistan" alone, cities in Iran or Syria); place words that are also everyday words are off in the list |
| A place only in captions | 1 | **Medium to weak** | Tourists, foreign news, diaspora |
| Kurdistan named | 1.5 | **Medium** | Also means the Kurdish parts of Turkey, Syria and Iran |
| Sorani Kurdish letters | 1.5 | **Medium** | Sorani is also written in Iran |
| Iraqi dialect markers | 1; 2 for three or more | **Medium** | Neighbouring dialects share words |
| Mostly Arabic script | 1 | **Weak** | Twenty-odd countries write Arabic |
| Another country named, with no Iraqi signal | −2 | **Medium** | Iraqis abroad naming where they live |
| Iraqi audience: how many registered accounts link to it | 1 to 3 | **Strong for influencers and foreign accounts aimed at Iraq** | A world star Iraqi fans tag (capped at 2 for a million followers or more) |
| Seen on posts placed in Iraq | 1.5 | **Strong but rare** (10% of posts carry a place) | Tourist posts |
| **New: keyword evidence** (igdisc) | **0.5 per distinct place of the shared list (Iraq, the Kurdistan Region, a governorate) met in someone else's content in the window, at most 1, inside the network cap of 4.** One place in several spellings (Basra, البصرة, بصرة) counts once | **Weak alone:** a hashtag like `#baghdad` is used by tourists, news and diaspora | Counted only on others' content, because the account's own captions are already in the vet's 12 |
| **New: located at an Iraqi place page** (Track 2) | Proposed: the same weight as "seen on posts placed in Iraq" (1.5) | **Strong** | Tourist posts |

**How it will be measured:**
- In the canary week you label a sample of 100 decisions: 50 added and 50 not added, across tracks and languages. That gives the precision of what was added, and how many good accounts fell below.
- The threshold and the keyword weight are then set from that sample.
- `reject` counts per day stay as an ongoing quality metric. Each reject is also published as `EXCLUDED`.

---

## 5. Gaps against the service standard (04), as a plan

`SESSION-PACK.md` didn't reach this session, so this section follows the standard's items as igs's [`igs-gaps.md`](igs-gaps.md) lists them, item by item. igdisc runs inside igs's binaries, chart and pipelines, so most items are **inherited**: igdisc meets them exactly as igs does, gaps included, and igs's fixes cover it. Only igdisc's own work is listed.

| Item | igdisc | Plan |
|---|---|---|
| A1–A5 Packaging, config, secrets | Inherited | igdisc adds config keys and a Deployment (off by default) to igs's chart |
| A6 Migrations | **New** | igdisc's own goose folder and version table; additive only; the previous release's check igs plans covers it too |
| B1 Health | Inherited | `/readyz` of the search stage checks the keyword table and its lane |
| B2–B5 Limits, browser, shutdown, resume | Inherited | Searches checkpoint like igs's tasks; no browser |
| B6 Fleet-wide limits | **New, and shared with igs's gap** | The exit share and p3 depend on Valkey. igs's gap (the limiter fails open) must be fixed first, or discovery could exceed its share when Valkey is down. igdisc's guard **fails closed**: no Valkey, no p3 |
| B7 Iraq time | Inherited | igdisc's admin output prints Iraq time, labelled |
| C1–C4 Retries, retention, reaper, breakers | Inherited | A walled surface opens its own breaker, per surface |
| C5 Identity and exit health | **New** | Discovery identities are counted and alerted separately: `igs_igdisc_identities{state}` |
| C6–C7 Lanes, budget | **New** | Lane p3 and the pause rule (§3.3). The budget counts p3 separately |
| D1 Logged out | Met by design | No login, ever |
| D2 No CAPTCHA or challenge solving | Met by design | A wall stops that surface's searches and raises an alert; nothing tries to get past it |
| D3 Pacing measured | **New** | Searches' own pace is measured in the probe and the canary before the share is raised |
| D4 Exits named by address in logs | Inherited | Fixed by igs's fix |
| D5 Measured cost | **New** | §3.2's figures are estimates; the canary report replaces them |
| E1–E2 One stream, platform topics | **New** | `discovery.candidate.v1` only. No `ig.*` topic for candidates |
| E3 Schemas | Met by design | igdisc registers no subject and creates no topic; the platform does |
| E4 Envelope and provenance | **New** | `Candidate.provenance` filled (§3.6) |
| E5–E10 Keys, absence, edits, raw, links, coverage | Mostly N/A | Candidates have none of these, except "absent is not zero": `is_private` unset until known |
| F1 Metrics | **New** | `igs_igdisc_searches_total{surface,outcome}`, `igs_igdisc_found_total{track,source}`, `igs_igdisc_evidence_total{language}`, `igs_igdisc_lane_waiting`, `igs_igdisc_share_used` |
| F2 Logs | **New** | No usernames or content in logs; candidate IDs only |
| F3 Alerts | **New** | Surface walled (searches failing for 30 minutes), discovery found nothing for 24 hours, p3 used more than its share, candidate events failing conformance |
| F4–F5 Dashboard, health commands | **New** | A discovery row on igs's dashboard; `igsctl igdisc status` (read-only) |
| G1–G3 CI, fake platform, load | **New** | §3.5's tests; a load test with a discovery backlog against scheduled runs |
| G4 Secret scanning | Inherited | igs's planned gitleaks job covers igdisc's files |
| G5 Conformance | **New, and ready** | The contract's `conformance.Candidate` check, run in CI from day one |
| H1 Runbook | **New** | `docs/igdisc/` runbook: turning surfaces on and off, the share, walls, the canary |
| H2 Backups | Inherited | igdisc's tables are in igs's Postgres |
| H3 Canary | **New** | A week with a test-only cap and your labelled sample, then uncapped |
| H4 Measured sizing | **New** | The canary report |
| I1–I4 Security | Inherited | No new credentials; admin endpoints use igs's admin scope |

---

## 6. Proposals for shared things

**For the shared keyword list** (fbs's `iraq.yaml`; proposed to fbs as a patch, `UPSTREAM.md`):
- **K1. Per-platform switches.** Optional `platforms:` on a group or a spelling, for example `platforms: {ig: false}`. Then a spelling that floods one platform can be turned off there without turning it off everywhere. Unset means every platform.
- **K2. Stable keyword IDs.** `Relevance.matched_keyword_ids` and `Evidence.keyword_id` need IDs that mean the same in every service. Each service's table numbers its rows itself. Proposal: the ID is `<group>:<language>:<folded spelling>`, for example `baghdad:ar:بغداد`. It's the same everywhere, needs no central counter, and survives a reload.
- **K3. Hashtag forms (optional).** igdisc generates them (§1.2). A spelling whose hashtag form is wrong or ambiguous could carry an explicit `ig_tag:`.

**For the contract** (fields only added, so a minor version):
- **C1. `SOURCE_LOCATION_PAGE`,** for accounts found on a place's page.
- **C2. `Evidence.content_time`:** when the matching post or comment was made, so "a mention in the last 6 hours" can be checked downstream.

**For the igs session:**
- the hooks in §1.3, as separate small PRs;
- the 07xx finding in §4;
- building the exit share and pause rule once, for both (`igs-gaps.md` Plans 1, items 3a and 3b);
- consuming `discovery.candidate.v1` for Instagram once the topic exists (Track 3).

---

## 7. Order of work, and what's needed from whom

**Order, after your approval:**

| Step | What | Needs | Effort |
|---|---|---|---|
| 0 | Your recording; my findings note | Your recording | 0.5 day after it arrives |
| 1 | Track 1: keyword sightings, the evidence table, keyword-aware priority. **In progress (7 Nov):** igdisc's own code in #65; the hooks into igs's files in a second PR stacked on it, one commit per hook, for the igs session to review | The hooks PR merged after #65 | 1.5 days |
| 2 | Lane p3, the exit share, the pause rule, discovery identities | Agreement with the igs session on who builds the share | 1.5 days |
| 3 | The candidate publisher and conformance in CI; publishing off until the topic exists | | 1 day |
| 4 | fakeig search pages and the e2e tests | | 1 day |
| 5 | Runbook, alerts, dashboard row, `igsctl igdisc status` | | 0.5 day |
| 6 | Track 2: hashtag and place searches and the planner | The recording shows logged-out access; your approval of the probe | 2 to 3 days |
| 7 | Canary week on production, your labelled sample, then uncapped | Your go | a week |

**Questions for you:**
- **O1.** Approve the plan, Track 1 first, with Track 2 only if your recording allows it?
- **O2.** Private accounts: second list only when they pass the Iraq check from their profile (my proposal), or every private account found?
- **O3.** Discovery's share of each exit: 10% to start (my proposal)?
- **O4.** For the canary week, a cap of 50 new accounts a day while you label the sample, then none?

**O1 to O4: decided by the master under D-027 (6 November, 18:15); the owner confirmed D-027 and said to start step 1 (7 November).**
- **O1:** approved. Track 1 first; Track 2 only if the recording shows logged-out access. Logged out only, and stop at any login wall or challenge.
- **O2:** the proposal. A private account goes to the second list only if it passes the Iraq check from its profile.
- **O3:** 10% to start. Agree with the igs session on who builds the share.
- **O4:** 50 a day during the canary week, then no cap once the labelled sample is reviewed.

**Questions for the master, answered on 6 November (18:15):**
- **M1. Keyword IDs:** `<group>:<language>:<folded spelling>`, with the folding defined once in the contract.
  - Contract v1.1.0 (published 6 November, `5e13d2f`) adds `contracts.KeywordID(group, language, spelling)` and `contracts.FoldKeyword`. igdisc uses those helpers, not copies of its own, and computes the ID from the keyword list's stored spelling.
  - After this session pointed out a mismatch, `FoldKeyword` is now fbs's and igs's `Normalize`. Checked on 6 November: all 393 spellings of the shared list, plus ZWNJ, tatweel, harakat and Arabic-Indic digit cases, fold the same in both. For example, هه‌ولێر and هەولێر both give `erbil:ckb:ههولێر`.
- **M2. Contract additions:** both are in v1.1.0: `SOURCE_LOCATION_PAGE = 18`, and `Evidence.content_time = 9`, of type `content.v1.Time`, so the estimate rule (D-013) applies to it. v1.1.0 also adds `conformance.Options{Production: true, Canary: true}`, which allows `CAPPED` during the canary week only.
- **M3. The Google discovery tool:** outside the platform's scope; no session runs it. igdisc doesn't plan on it. If it ever sends Instagram links, they arrive as ordinary candidates with `target_platform: ig`.
- **M4. The topic:** `discovery.candidate.v1` comes in the platform's first production apply (3 partitions, 7 days, replication 3), once the owner has fixed some GitHub secrets. The master will say when it exists. The platform registers its schema.

**Answered by the contract, no longer open:**
- Who acts on a candidate: the source publishes it when found, and the target platform's collector publishes the outcome (`candidate.proto`).
- Registration: the platform registers every shared subject from the contract's CI, and collectors never register them (contract README §11). So igdisc registers nothing, `discovery.*` included.

---

## Sources

Desk research, 6 November 2026. The vendor and blog sources are third-party claims, not Meta's.

- Apify, Instagram Scraper changelog (hashtags behind a login, 2024-10-09): https://apify.com/apify/instagram-scraper/changelog
- Apify, Instagram API Scraper issue (the same, from a maintainer): https://apify.com/apify/instagram-api-scraper/issues/unable-to-fecth-any-KNZ7aQdnW78MCvzWo
- SociaVault, searching hashtags logged out leads to a login wall: https://sociavault.com/blog/instagram-hashtag-search
- TechCrunch, the 2022 test removing the Recent tab: https://techcrunch.com/2022/04/19/instagram-removes-recent-tab-hashtag-pages/amp/
- Techedt, hashtag taps open a full search results page (2025): https://www.techedt.com/instagram-revamps-hashtag-search-to-enhance-discovery
- Social Media Today, keyword search added (2020): https://www.socialmediatoday.com/news/instagram-adds-keyword-search-in-addition-to-profiles-and-tags/
- The Next Web, location pages and profiles requiring a login (2019): https://thenextweb.com/news/instagram-now-forces-people-to-sign-in-to-view-public-profiles
- PPC Land, Instagram posts searchable on Google from 10 July 2025: https://ppc.land/instagram-content-becomes-searchable-on-google-starting-july-10/
- Firecrawl, Google's Custom Search JSON API closed to new customers and ending 1 January 2027: https://www.firecrawl.dev/blog/google-search-json-api-alternative
- Microsoft, Bing Search API retirement (11 August 2025): https://learn.microsoft.com/en-us/lifecycle/announcements/bing-search-api-retirement
- igs's own measurements: [`build-status.md`](../instagram/build-status.md) (U7, U11, Phase 0) and [`har-findings-2026-10-30.md`](../instagram/har-findings-2026-10-30.md).
