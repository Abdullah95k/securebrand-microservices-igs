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

**On the list now (M, 11 Nov 21:21Z, `igs_discovery_candidates`):**
- 534 accounts registered by discovery, plus the 51 pilot accounts. That's about 585 on the schedule.
- 396,153 candidates are waiting to be vetted, at priority 2 or above.
- Vetted so far: 534 registered, 671 below the threshold, 795 private, 974 failed, and 40 in progress.
- **Acceptance over 7 days: 17.3%** of all vets, or 27% of the vets that reached a decision (failed vets left out).
- **Sightings in the last day:** commenters 93,014, tags 1,078, co-authors 767, mentions 140.

**What the measured numbers say:**

1. **The pool is mostly commenters, so mostly personal accounts.** Discovery has no follower floor (`ig.discovery.min_followers: 0`). A public personal account whose bio or captions read as Iraqi passes at threshold 4.
   - Without a floor, the list grows toward Iraq's public Instagram users.
   - With a floor, it stays with accounts that publish for an audience: businesses, media, creators and public figures.
   - **The floor is the scope decision for the owner.**
2. **The current pool alone gives about 20,000 to 70,000 accounts** (D).
   - That's 396,153 × 17.3% = about 68,600, if acceptance holds.
   - It's likely lower, because vets go highest priority first, so acceptance falls further down the pool (G: 5 to 17%).
3. **The pool keeps growing.** It reached about 400,000 in about 4 days from a few hundred tracked accounts. That's about 100,000 new candidates a day (D), and it grows with the list.
4. **Vetting is the bottleneck.** At 10% of 10 exits at 0.05 requests a second, igs vets about 1,500 to 2,000 accounts a day (D). The current pool would take about 200 days.
   - 32% of vets failed. That's mostly exits resting or refusals (G), worth a look.
   - Lifted by more IPs, or a larger discovery share (`exit_share`, D-027).

**Estimate of the target:**

| Scope | Count | How |
|---|---|---|
| Today | About 585 | M |
| **Low:** accounts that publish for an audience, with a floor of about 1,000 followers | **About 50,000** | D/G: the current pool's yield (20,000 to 70,000), less the share under the floor, plus (2), accounts elsewhere with an Iraqi audience (5,000 to 20,000) |
| **High:** no floor, the list a year out at today's growth | **About 400,000** | G: about 100,000 candidates a day at 5 to 17% acceptance, bounded by vet speed and IPs |
| **Ceiling:** no floor, every public Iraqi account | **About 1 to 5 million** | G: Iraq's Instagram users, in the low tens of millions (public ad-reach figures, not checked here); 20 to 40% public; most identifiable as Iraqi |

**To tighten these numbers from metrics igs already exports** (no SQL needed):
- `igs_discovery_candidates{status}`: candidates by outcome. `new` counts only those due a vet, at `min_priority` 2 or above.
- The acceptance rate: `sum(increase(igs_discovery_vets_total{outcome="registered"}[7d])) / sum(increase(igs_discovery_vets_total[7d]))`.
- `igs_discovery_sightings_total{source}`: how fast each network source meets accounts.

**The same counts from igs's store,** for anyone allowed to read it (read-only, schema `ig`, counts only):

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

IP price: **$3 an IP a month, unlimited data** (the owner's figure, O-5, 5 Nov). The provider's real price and stock are still to be confirmed. Per-GB residential pricing is shown for comparison only.

Assumptions:
- An account that publishes for an audience costs about 70 requests a day at the 3-hour interval.
- A quiet account (posting less than weekly) visited once a day costs about 10 (D).
- A list without a follower floor is mostly quiet personal accounts: about 80% (G).
- One IP carries about 4,300 requests a day (M).

| | Today | Low | High | Ceiling, per million |
|---|---|---|---|---|
| Accounts | About 585 | About 50,000, floor of about 1,000 followers | About 400,000, no floor | 1 million, no floor (1 to 5 million in all) |
| Visits | Every 3 h | Every 3 h; quiet accounts daily | Every 3 h; quiet accounts daily | Mostly daily |
| Requests a day | About 41,000 | 2.0 to 3.5 million | About 8.8 million (28 million with every account at 3 h) | 10 to 25 million |
| **IPs** | **About 10: today's 10 are nearly full** | **300 to 1,000** | **1,500 to 3,000** (4,000 to 8,000 at 3 h for all) | **2,500 to 6,000** |
| IP cost a month, at $3 an IP | $30 | $900 to $3,000 | $4,500 to $9,000 ($12,000 to $24,000 at 3 h for all) | $7,500 to $18,000 |
| Crawler pods | 1 profile, 1 post, 1 retry | 8 to 16 | 20 to 40 | 25 to 60 |
| Persister and normalizer | 2 each | 2 to 4 each | 4 to 8 each | 6 to 12 each |
| Nodes (4 vCPU, 8 GB) | igs's share of today's cluster | 4 to 6 | 10 to 20 | 12 to 30 (G) |
| PostgreSQL growth a day | 10 to 120 MB | 1 to 10 GB; backfill 75 to 150 GB once | 3 to 26 GB; backfill 0.4 to 1 TB once | 2 to 20 GB; backfill 0.5 to 1.5 TB once. Partition tiering needed from the high tier (G) |
| S3 raw held (30-day window) | 35 to 75 GB | 3 to 6 TB | 7 to 15 TB | 6 to 15 TB |
| Posts a day | 600 to 1,200 | 50,000 to 100,000 | 110,000 to 320,000 | 100,000 to 500,000 |
| Comments a day | 6,000 to 120,000 | 0.5 to 10 million | 0.8 to 18 million | 0.5 to 10 million |
| **At D-053's 10% sample** | 60 to 120 posts; 600 to 12,000 comments | 5,000 to 10,000 posts; 50,000 to 1 million comments | 11,000 to 32,000 posts; 80,000 to 1.8 million comments | 10,000 to 50,000 posts; 50,000 to 1 million comments |

A personal account posts 0.1 to 0.5 times a day and draws 0 to 5 comments (G). An audience account posts 1 or 2 times a day and draws 10 to 200 comments (D, Phase 0). Analysis volume follows the audience accounts, whatever the floor.

**The two levers, both for the owner:**
- **A follower floor** (`ig.discovery.min_followers`). It sets the scope: low, high or ceiling.
- **Daily visits for quiet accounts** (scheduler tiers). It cuts a mostly quiet list's IPs about 3 times.

**Today's IPs are nearly full.** About 585 accounts at 70 requests a day is about 41,000 requests, against 10 IPs × about 4,300 (M). The list grows by about 300 to 500 accounts a day at today's vet speed (D: 1,500 to 2,000 vets a day at 17 to 27% acceptance). So new IPs, daily visits for quiet accounts, or a floor are needed within days, not weeks.

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
