# igs coverage plan: scope, cost per account, three tiers

For the platform's production plan (owner's D-054, 11 Nov 2026). The owner's target:

1. every Iraqi page, account or channel on Instagram, based in Iraq or run by Iraqis;
2. every account, from anywhere, that Iraqi audiences follow, comment on or interact with.

The list keeps growing through discovery; it has no fixed size.

**How to read the numbers:**

| Mark | Means |
|---|---|
| **M** | Measured in the pilot (7 to 11 Nov) or in Phase 0 (31 Oct to 1 Nov) |
| **D** | Derived from measured numbers |
| **G** | A guess: the method is given, the number is not measured |

Counts only. No account names anywhere in this document.

---

## 1. Size of the scope

**On the list now (M, 11 Nov):** 234 profiles on the schedule.
- 51 from the pilot list.
- About 183 added by discovery since 7 Nov. Discovery was held most of that time, because the exits were resting.

**Estimate of the target (G).** "Account" here means a public account that publishes for an audience: businesses, media, creators and public figures. Personal accounts are excluded: there are millions of them and they don't fit the purpose.

| | Low | High | How |
|---|---|---|---|
| (1) Iraqi accounts | 20,000 | 300,000 | Instagram's users in Iraq are in the low tens of millions (G, public ad-reach figures, not checked here). Accounts with 10,000 followers or more are about 0.1% of users (low). Those with 1,000 or more are about 1 to 2% (high) |
| (2) Accounts elsewhere with an Iraqi audience | 5,000 | 100,000 | Arab regional media, celebrities and brands with Arabic content. The low figure counts only those Iraqi accounts link to; the high figure counts any with a measurable Iraqi share of commenters |
| **Total** | **about 25,000** | **about 400,000** | |

The high figure isn't a ceiling. With a follower floor of 500, (1) alone could pass a million.

**To tighten these numbers from igs's own store** (read-only, schema `ig`, counts only):

```sql
SELECT status, count(*) FROM ig.discovery_candidates GROUP BY 1;
```
That's how many accounts discovery has met, by outcome: `new` (not vetted yet), `registered`, `below`, `capped`, `private`, `gone`.

```sql
SELECT count(*) FROM ig.discovery_candidates WHERE status = 'new' AND priority >= 2;
```
That's how many are waiting to be vetted.

```sql
SELECT count(*) FROM ig.comment_authors;
```
That's how many distinct commenters igs has seen, the raw pool for route (2).

The acceptance rate (`registered` ÷ everything vetted) times the pool waiting gives the next order of magnitude.

---

## 2. How igs finds accounts

All routes stay logged out (D-051: no getting around bot checks).

| Route | Status | Growth |
|---|---|---|
| **Network:** accounts tagged, mentioned, co-authoring or commenting on tracked accounts' posts, weighted by how many tracked accounts link to them, plus posts located in Iraq | **Built** (M11, M11c). It costs no requests: it reads what collection already fetched | About 50 accounts a day in the pilot (M, 183 in about 4 days, held most of the time). The ceiling is the vets: 2 to 3 requests each, capped at 10% of every exit (D-027). On 10 IPs at 0.05 requests a second that's at most about 1,500 vets a day (D). At an unmeasured 10 to 30% acceptance, that's 150 to 450 new accounts a day (G). It scales with IPs and with the list itself |
| **Iraqi signals in the account's own words:** Iraq or 🇮🇶, +964 or 07xx numbers, `.iq` links, governorates, Sorani letters, Iraqi dialect, Arabic script | **Built** (scoring). Decides acceptance; finds nothing by itself | – |
| **Cross-platform:** Instagram links on Iraqi Facebook, TikTok and YouTube accounts, and links out of tracked Instagram bios | **Built** for links out (`discovery.candidate.v1`). Links in depend on fbs, tks and yts sending candidates to igs's intake | As fast as the other collectors find them |
| **Intake:** lists from the platform, up to 1,000 per call, vetted first | **Built** | Immediate, at vet speed |
| **Hashtags and keywords, location pages** | **Not built.** Logged out, Instagram very likely sends these to the login page (G, by the tagged-tab result of 5 Nov: login needed). A read-only probe would settle it, with the owner's go | Unknown |
| **Who an account follows, or its followers** | **Not possible logged out.** Instagram needs a login for these lists | – |
| **Route (2), an Iraqi share of commenters:** scoring a foreign account by how many of its commenters are Iraqi | **Partly built.** Iraqi accounts that link to it already score; commenters only with `pii_mode: full`. A full version reads a candidate's comments: about 10 to 30 requests per candidate (G) | Bounded by the vet share; about a tenth of the network route's speed (G) |

**One rule conflicts with D-054 (2).** The scorer deliberately leaves out world stars and Gulf accounts with nothing Iraqi of their own: at most 2 points from the network for accounts with a million followers. If the owner wants those whose audience is largely Iraqi, the scorer needs an Iraqi-share rule. That's a change for the owner to approve.

---

## 3. What one tracked account costs

At today's intervals: the long tail every 3 hours (232 of 234 accounts), warm every hour, hot every 10 minutes. Highlights are off (D-052). Reels are read 7 days back on each visit, plus a weekly full pass (fix 5).

| Item | Per account | Mark |
|---|---|---|
| Requests a day, long tail (3 h) | About 70; 45 to 190 by reels volume | D (Phase 0's per-request costs, with fix 5) |
| Requests a day, warm (1 h) | About 150 to 250 | D |
| Requests a day, hot (10 min) | About 300 to 450 | D |
| Requests one IP sustains a day | About 4,300 at `exit_rps` 0.05, with no rate limits apart from highlights. At 0.1, about 8,600 at most, but about 1% rate-limited, so effectively 5,000 to 7,000 | **M** (11 Nov). The old estimate of 13,000 to 20,000 doesn't hold |
| **Accounts one IP carries (long tail)** | **50 to 100** | D |
| Proxy data | 2.0 to 4.4 MB a day, so 60 to 130 GB a month per 1,000 accounts if billed per GB | M (Phase 0) |
| CPU and memory | Small per account: igs's work is mostly waiting on the per-IP pace. Per 1,000 accounts, about 0.1 to 0.2 vCPU and 150 to 300 MB extra (G). The base set of pods (about 5.5 vCPU and 6 GiB requested) carries up to about 10,000 to 20,000 accounts (G; the load test did 231 requests a second on 4 CPUs, M) | G |
| PostgreSQL | 0.6 to 6 MB a month (20 to 200 MB a day per 1,000 accounts), plus 1.5 to 3 MB once for the backfill | D |
| S3, raw responses | 60 to 120 MB a month under `igs/30d/` (30-day rule, so it doesn't grow past 30 days), plus failures and a 1% sample kept for good (about 1%) | D |
| ClickHouse (the platform's writer) | About 0.2 to 3 MB a month: content rows for 1 to 2 posts and 10 to 200 comments a day (G, about 0.5 KB a row compressed) | G |
| Media | 0 while `include_media` is off. When on, files live until analysis acknowledges them, at most 72 h | – |
| Posts and comments a day | 1 to 2 posts, 10 to 200 comments, as new or edited items | D (Phase 0: about 2,000 posts and 10,000 to 200,000 comments a day per 1,000 accounts) |

Re-reads publish items again, with a new `event_id` and the same `content_hash`. Analysis should dedupe by `content_hash`, so only new or edited items are analysed.

---

## 4. Three tiers

IP price: static ISP IPs at about $1 to $5 a month each (the readiness review's assumption; the owner's contract decides). At the high tier, per-GB residential pricing may be cheaper, shown separately. Nothing here is a quote.

| | Today | Low | High |
|---|---|---|---|
| Accounts | 234 | 25,000 | 400,000 |
| Requests a day | About 16,000 steady (D), more during catch-up | About 1.75 million (about 20 a second) | About 28 million (about 325 a second) |
| **IPs** | 3 to 5 needed; 10 today | **250 to 500** | **4,000 to 8,000** |
| IP cost a month (ISP) | 10 × today's price | $250 to $2,500 | $4,000 to $40,000 |
| Or per GB (residential) | – | 1.5 to 3.3 TB a month | 24 to 53 TB a month |
| Crawler pods | 1 profile, 1 post, 1 retry | 4 to 8 | 40 to 80 |
| Other pods | As now: 2 each for persister, normalizer, API and scheduler, plus registry-sync and media | As now; persister and normalizer at 2 to 4 | Persister and normalizer at 6 to 12 |
| Nodes (4 vCPU, 8 GB) | igs's share of today's cluster | 3 to 4 | 15 to 30 (G) |
| PostgreSQL growth | 5 to 50 MB a day | 0.5 to 5 GB a day; backfill 40 to 75 GB once | 8 to 80 GB a day; backfill 0.6 to 1.2 TB once. Needs partition tiering by year one (G) |
| S3 raw (30-day window) | 15 to 30 GB held | 1.5 to 3 TB held | 24 to 48 TB held |
| Posts a day | About 250 to 500 | 25,000 to 50,000 | 400,000 to 800,000 |
| Comments a day | About 2,000 to 50,000 | 250,000 to 5 million | 4 million to 80 million |
| **At D-053's 10% sample** | 25 to 50 posts; 200 to 5,000 comments | 2,500 to 5,000 posts; 25,000 to 500,000 comments | 40,000 to 80,000 posts; 0.4 to 8 million comments |

**The biggest lever on every row** is the interval for quiet accounts. Visiting accounts that post less than weekly once a day instead of every 3 hours cuts their requests about 8 times. With most of a large list quiet (G), IPs at the high tier fall to roughly 1,500 to 3,000. That's a configuration change (tiers), for the owner's approval.

---

## 5. What limits coverage first, and what lifts it

1. **Per-IP rate limits (the binding limit, M).**
   - About 4,300 requests a day per IP is safe today, so IPs are about 70% of the cost at every tier.
   - **Lifted by:** more IPs, which scale linearly; daily visits for quiet accounts; fewer requests per visit.
2. **Login walls on fresh page loads (M: page walls; 8 in an hour paused the pilot).**
   - Page loads, the profile and post pages, are what Instagram walls. Each new session and each post page is one.
   - **Lifted by:** reading post pages less often; a per-exit daily page-load budget (`exit_page_loads_per_day`, off, once the pilot shows what an IP tolerates); more IPs.
3. **What logged-out access can't reach (M).**
   - Highlights are refused now (D-052).
   - The tagged tab, stories, follower and following lists, and very likely hashtag and location pages need a login.
   - **Lifted by:** only logged-in collection. That would need the owner's decision, real accounts, and a review against D-051. Not proposed here.
4. **Captchas and checkpoints:** none seen logged out (M, checkpoint 0 throughout). If they appear, igs rests the exit and never solves them (D-051).
5. **Proxy quality (M).**
   - 3 of 10 ISP IPs were flagged in Phase 0.
   - The page-wall and rate-limit rests fall unevenly: page loads per exit ranged from 11 to 164 an hour.
   - **Lifted by:** a spare pool to swap flagged IPs out; at the high tier, a residential pool billed per GB.
6. **Discovery's speed.**
   - Vets get 10% of each exit and wait while exits rest, so the list grows with the IPs.
   - **Lifted by:** a larger discovery share once collection is steady (`ig.discovery.exit_share`, owner's D-027), and the intake route for lists from the other collectors.

**What would firm these numbers most:**
- a week at today's pace, giving requests per account per day and per IP;
- the three queries in §1;
- the proxy price per IP or per GB.
