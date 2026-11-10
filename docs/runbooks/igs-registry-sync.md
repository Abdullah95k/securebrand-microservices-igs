# igs registry sync: following Instagram's doc_id rotations

Instagram's web app sends each GraphQL query as a persisted query: a
`doc_id` names the query on Instagram's side. Instagram changes those ids
from time to time, and an old id then fails (`PersistedQueryNotFound`,
classed `registry_stale`). igs keeps its ids in the registry (`ig.queries`);
`igs-registry-sync` keeps the registry current without a deploy.

## What a run does

Every `ig.registry_sync.interval` (30 minutes):

1. Chooses its canary profiles and loads each one's page.
   - **From the store** (`ig.registry_sync.store_canaries`, 3 in
     production): each run draws 200 accounts at random from the ones igs
     has, keeping only active public accounts with posts or highlights.
     From those it picks, in order, the account that has the most of what
     the queries still need (highlights, reels, a post of the last 30 days
     with comments, posts), until every need is met, then the richest
     accounts left. So the first canary is normally one with highlights,
     and over many runs every account takes its turn.
   - **From `ig.registry_sync.canary_pages`** (usernames of public
     profiles with regular posts, comments, reels and highlights): these
     fill what the store can't, for example on a new install with an empty
     store. They are never repeated.

   A run loads `store_canaries` or the length of `canary_pages`, whichever
   is larger, so choosing from the store adds no page loads. igs checks the
   highlights query only on a canary that has highlights. Without one,
   highlights stays unchecked, the run doesn't count as a success, and
   after 3 hours `IgsIgRegistrySyncNotRunning` fires.

Its requests go through the crawlers' exits and honour their rests, but
keep a pace of their own on each exit: a fifth of `ig.exit_rps`. Before,
they queued for the crawlers' slots. The fleet-wide pacer has no queue, so
with about 20 crawler sessions per exit each check waited about 80
seconds, and every run hit its 10-minute timeout while the crawlers read
(the pilot, 7 Nov 2026). With the crawlers stopped, the same runs finished.
2. Scans the page and its script bundles (`static.cdninstagram.com`) for
   Relay operation ids. That Instagram's bundles declare them, as Facebook's
   do, is open question U6; when they do not, and `ig.browser.enabled` is
   on, a rendered canary page's own requests supply them (POST bodies and
   GET query strings alike).
3. Records ids it does not know as candidate entries of the queries with
   that friendly name.
4. Checks the active entry and the candidates with real requests on a
   canary profile, in the order their inputs chain: profile (gives the
   user id and highlights), grid (gives posts), comments, reels, highlights.
5. Promotes a candidate that parses when the bundles currently declare it,
   or when the active entry failed. The old active entry stays as a
   fallback (`keep_fallbacks`), which the GraphQL client switches to on a
   stale id. Candidates that fail `retire_after` checks in a row retire.

Crawlers pick promotions up on their next registry reload
(`ig.registry_refresh`, 30 seconds). Every run is stored in
`ig.registry_sync_runs`: `GET /v1/admin/registry-sync/runs` (an admin key)
lists the latest with their reports, and `igs-registry-sync --once` runs one
now.

## Reading a run

`igs-registry-sync --once` prints the report and exits 3 when degraded:

| Field | Meaning |
|---|---|
| `status` | `ok`: every query has a working active entry. `degraded`: some query failed with nothing to promote, or could not be checked. `error`: no canary loaded, or the database failed. `skipped`: another replica holds the lock |
| `discovered`, `rendered` | doc_ids found in the bundles, and in a rendered page's requests |
| `not_found` | registry queries no bundle declared: Instagram may have renamed the operation |
| `checks` | the verdict per entry: `ok`, `stale`, `drift` (accepted, but the parser misses required fields), `inconclusive` (blocked, no data) |
| `promoted` | doc_ids made active, with why |
| `failing` | queries whose active entry is stale or drifted with no working replacement |
| `unchecked` | queries no canary gave a verdict on |

## When a query is failing

1. Run `igs-registry-sync --once` and read `checks` for the query.
2. **stale, nothing discovered:** the bundles no longer declare it, or the
   operation was renamed. Turn on `ig.browser.enabled` (the render finds ids
   from the page's own requests), or record a HAR of a profile in a browser
   and set the new doc_id by hand:
   `PUT /v1/admin/queries/<name>` with the query's friendly name, doc_id,
   parser and variables (an admin key; see `api/openapi/igs.v1.yaml`).
3. **drift:** Instagram changed the response shape. The parser needs a
   change: capture fixtures with `igs-spike phase0 --from capture` (or the
   probe's raw answers and `igs-spike fixtures`) and send them to a build
   session.
4. **inconclusive on every canary:** the canaries are blocked or empty. Check
   the tier (`ig.registry_sync.tier`) and pick canaries with recent posts,
   comments, reels and highlights.

## Metrics

| Metric | Meaning |
|---|---|
| `igs_registry_sync_runs_total{status}` | runs by status |
| `igs_registry_sync_last_success_timestamp_seconds` | the last run that checked every query |
| `igs_registry_query_healthy{query}` | 1 while the query's active entry works |
| `igs_registry_promotions_total{query}` | promotions |
| `igs_registry_checks_total{query,status,verdict}` | checks |

The alerts on them come with igs's chart (M10).
