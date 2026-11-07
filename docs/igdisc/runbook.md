# igdisc runbook: Instagram keyword discovery

How to turn on, watch and check Instagram keyword discovery. The plan is in [`igdisc-plan.md`](../master/igdisc-plan.md), and progress in [`status.md`](status.md). Times are Iraq time (UTC+3). Every example is synthetic.

## What Track 1 does

Track 1 reads nothing from Instagram. It looks at what igs already reads: the posts and comments of every registered account, read every 3 hours.

1. **It finds recent mentions.** A post or comment made in the last 6 hours (`window`) that names a keyword of the shared list (`ig.discovery_keywords`: Iraq, the Kurdistan Region, the governorates and their towns, in four languages) is a mention.
2. **It notes who was met there.** Each account tagged, co-authoring or mentioned in such a post, or writing such a comment, gets the keyword as evidence (`ig.igdisc_evidence`). Comment authors count only with `pii_mode: full`.
   - Accounts already on the schedule, rejected accounts and erased accounts are skipped.
   - The post's own account is never counted.
3. **It hands them to discovery.** Each one becomes a discovery candidate with origin `keyword`, and its vet priority goes up, so it's checked soon.
4. **Discovery decides, as always.** Its usual vet (the profile and 12 captions, 2 or 3 requests) and Iraq score decide. The keyword evidence adds one signal, `keyword_context`: 0.5 for each different place the account was met near, at most 1.
   - That is never enough alone: the threshold is 4.
   - Private accounts go to the second list only if they pass the check (O2), once candidates are published (step 3).

Discovery's rules apply unchanged:
- **It waits for regular collection:** no new vets while the schedule is behind, or while most exits of a tier rest.
- **No caps in production.**
- **All data is kept.** Erasure deletes a person's rows, evidence included.

## Turning it on

It is off by default. It needs profile discovery on (`ig.discovery.enabled`), which is on in the chart's values. In the chart's values, under `config.ig`:

```yaml
igdisc:
  enabled: true
```

The other keys keep their defaults: `window: 6h`, `flush_every: 10s`, `priority: 2`.

Or, for one deployment, set this environment variable on the crawlers and the scheduler:

```bash
IGS_IG__IGDISC__ENABLED=true
```

Its table comes with `igsctl migrate up` (the chart's migration hook), which applies igdisc's migrations after igs's own. They keep their own goose versions in `ig.igdisc_goose_db_version`. This command lists both sets:

```bash
kubectl -n igs exec deploy/igs-api -- igsctl migrate status
```

## Watching it

- **Metric:** `igs_igdisc_evidence_total{via,language}`, the new evidence written. `via` is `caption`, `comment` or `post_place`.
- **Discovery's own metrics still apply:** `igs_discovery_vets_total{outcome}` and `igs_discovery_candidates{status}`.
The two queries below run from a machine that can reach igs's database, with its read-only connection string in `READ_DSN`. igs's image has no `psql`.

- **The evidence of the last day, by place:**

```bash
psql "$READ_DSN" -c "SELECT grp, language, via, count(*) FROM ig.igdisc_evidence WHERE found_at > now() - interval '1 day' GROUP BY 1, 2, 3 ORDER BY 4 DESC"
```

- **What happened to the accounts it found:**

```bash
psql "$READ_DSN" -c "SELECT status, count(*) FROM ig.discovery_candidates WHERE 'keyword' = ANY(origins) GROUP BY 1"
```

- **Why an account was or wasn't added:** its vet in `ig.discovery_vets`. `signals->>'keyword_context'` is the keyword part of the score, and `sightings->>'keywords'` is how many places it was met near.

## Tuning

- **A keyword brings in mostly unrelated posts:** turn it off in the shared list (`PATCH /v1/discovery/keywords/{id}` with `{"enabled": false}`). Crawlers reload the list within 5 minutes.
- **A longer or shorter window:** `ig.igdisc.window`. R8 asks for about 6 hours. igs reads each account every 3 hours, so a 6-hour window sees every new post at least once.
- **Turning it off:** set `ig.igdisc.enabled: false`. The evidence already written stays, and discovery goes on without the signal.

## What it does not do yet

- **No `discovery.candidate.v1` events (step 3).** Nothing is published until the platform creates the topic.
- **No searches on Instagram (Track 2).** That waits for the owner's recording of what a logged-out visitor can search.
- **No separate exit share for discovery (step 2).** Vets still use igs's lane p2, with discovery's pause rule.
