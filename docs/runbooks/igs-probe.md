# igs probe: what logged-out Instagram serves a crawler (checkpoint C2)

The probe answers the questions the build cannot answer without real
Instagram traffic, for a short list of public profiles:

| # | Question | What the probe does |
|---|---|---|
| U1 | Does the profile grid keep paging past the 24 posts the browser showed? How deep before a wall? | Pages the grid until it ends, hits a wall or reaches 32 pages (about 390 posts) |
| U3 | Do queries work without the browser's client-state fields? | Sends every query without them |
| U5 | Is a grid cursor tied to the identity that received it? | Replays a cursor from a second identity |
| U8 | Which cookies does Instagram set? | Lists the cookie names after each page load |
| U9 | Can a profile be read by its id alone? | Asks for the grid by id, with no cursor |

It also collects what it reads, for you to look at:
- each profile's username, link, name, bio and bio links, follower and following counts, verified and private flags, linked Facebook page and highlights;
- the profile picture, saved as an image;
- every post it pages through: link, type, caption, image description and thumbnail link;
- comments (commenter, text, time and likes): the newest post's first 10 by default, more with `--comment-posts` and `--comment-pages` (see Reference).

It runs on the same machine as Facebook's Phase 0 (a droplet), with the
same `phase0/proxies.yaml`, through the ISP proxies only (the owner's
choice: it never uses the residential tier unless asked). It paces like a
person: one request every 3 seconds or so, about 50 requests per profile, so
**2 to 3 minutes per profile**.

**The output holds personal data** (names, bios, comments). It stays in
`phase0/`, which git ignores. Share it privately, and never commit it.

## What you need

| | |
|---|---|
| A machine | The Phase 0 droplet (Linux with Go 1.21 or later; the build fetches Go 1.27 itself) |
| Proxies | The same `phase0/proxies.yaml` as Facebook's Phase 0; the probe uses its `isp` tier |
| Profiles | 5 to 10 public profiles: a big brand, a small account, at least two with more than 50 posts, one with highlights |

## 1. Get the code on the droplet

Log in to the droplet (from your Mac's Terminal):

```bash
ssh root@YOUR_DROPLET_IP
```

Clone the igs repository next to fbs's. Use the same login you used for
`securebrand-microservices`; if git asks for a password, give a GitHub
personal access token.

```bash
cd ~ && git clone https://github.com/Abdullah95k/securebrand-microservices-igs
```

```bash
cd ~/securebrand-microservices-igs
```

Check Go (Facebook's Phase 0 installed it). If it is missing, install it with
`apt-get install -y golang-go`.

```bash
go version
```

Later, to get updates: `git pull --ff-only origin main`.

## 2. Rehearse (about a minute, no traffic to Instagram)

```bash
make igs-probe-dryrun
```

It runs the whole probe against a built-in fake Instagram. Its summary must
say `bootstrapped 3/3`. The output goes to `phase0/out-ig-dryrun/`.

## 3. Fill in the two files

Copy Facebook's proxies file (the same tiers and credentials):

```bash
cp ~/securebrand-microservices/phase0/proxies.yaml phase0/proxies.yaml
```

```bash
chmod 600 phase0/proxies.yaml
```

Then the profiles:

```bash
cp phase0/ig-profiles.example.txt phase0/ig-profiles.txt
```

```bash
nano phase0/ig-profiles.txt
```

One profile per line: a username, `@username` or the profile link. Delete
the placeholder lines. Save with Ctrl-O, Enter, then leave with Ctrl-X.

## 4. Run

Start a `tmux` session first, so the run continues if your connection
drops:

```bash
tmux new -s igs
```

```bash
make igs-probe
```

This runs the ISP tier only. To also measure the droplet's own IP, run
`make igs-probe IGS_PROBE_TIERS=direct,isp`.

It prints a line when each profile is done, with its comments and the
running totals, for example
`profile 81/348 some_brand: bootstrap ok, grid 1212 posts, stop cap, 10 comments; so far 52495 posts, 790 comments`,
and on a long grid a line every 10 pages (about every 30 seconds), for example
`some_brand: 132 posts so far, back to 2026-10-28`.

**To check the totals without disturbing the run,** log in and, without
attaching, print its latest line:

```bash
tmux capture-pane -p -J -S - -t igs | grep 'so far' | tail -1
```

The CSVs (below) are also saved after every profile, so they can be opened
or copied while the run goes.

To leave it running, close the Terminal window or press **Ctrl-B, then D**.
If the connection drops, log in again and reattach with `tmux attach -t igs`.

**Ctrl-C stops the run.** It still writes what it read; the profile it was
reading is marked `stopped` (partial), and the summary says the run stopped
early. A stop is not counted as a wall. The summary also says how to
continue: run the same command again with `--start-at N` (the profile it
stopped at) in `IGS_PROBE_ARGS` and a **new** `IGS_PROBE_OUT`, so the first
part's files are kept.

At the end of each tier it prints a short summary, for example:

```
read 3120 posts and 80 comments from 8 profiles
bootstrapped 8/8; grid: 6 ended, 2 capped, walls map[] at [] posts, 8 paged past 24 (max 384)
cursor from another identity: 8 worked, 0 failed; grid by pk alone: 8 worked, 0 failed
```

## 5. What it writes

For each tier, `phase0/out-ig/<tier>/` holds:

| File | Holds |
|---|---|
| `profiles.csv` | One row per profile: link, name, counts, bio, highlights, picture file, how far the grid went |
| `posts.csv` | One row per post read: link, type, caption, thumbnail link |
| `comments.csv` | The comments read, with the post each belongs to |
| `images/` | The profile pictures |
| `raw/` | Instagram's raw answers, to check their shape: per profile the first answer of each query and every failed or empty one, and the page HTML of the first 3 profiles |
| `report.json` | Everything above, plus every request's outcome, timing and size, and the answers to U1 to U9 under `summary` |

and `phase0/out-ig/<tier>.tar.gz` packs the folder for downloading. The
three CSVs grow while the run goes (one profile at a time); `report.json`
and the `.tar.gz` are written at the end.

## 6. Send the results back

Download the bundles to your Mac (run this in a Terminal on the Mac, not on
the droplet):

```bash
scp "root@YOUR_DROPLET_IP:securebrand-microservices-igs/phase0/out-ig/*.tar.gz" ~/Downloads/
```

Then attach them to the next build session, or put them in the Drive
folder and share the link. The session records the answers in
`docs/instagram/build-status.md` and the HAR findings, and keeps the files
themselves out of git.

## Reference

| Setting | Default | Change with |
|---|---|---|
| Profiles file | `phase0/ig-profiles.txt` | `IGS_PROBE_PROFILES=...` |
| Tiers | `isp` | `IGS_PROBE_TIERS=direct,isp` |
| Output | `phase0/out-ig/` | `IGS_PROBE_OUT=...` |
| Proxies | `phase0/proxies.yaml` | `PHASE0_CONFIG=...` |
| Grid depth | 32 pages after the first (396 posts) | `IGS_PROBE_ARGS="--max-grid-pages 100"` |
| Backfill window | none | `IGS_PROBE_ARGS="--max-age-days 365"`: stop each profile's grid at posts older than a year |
| Comments | the newest post, first page (10) | `IGS_PROBE_ARGS="--comment-posts 10 --comment-pages 3"`: the newest 10 posts, up to 30 comments each (each page is one more request, about 3 seconds) |
| Start | the first profile | `IGS_PROBE_ARGS="--start-at 81"`, with a new `IGS_PROBE_OUT`: continue a stopped run at profile 81 |
| Profiles at once | 1 | `IGS_PROBE_ARGS="--parallel 6"`: 6 profiles at a time, each by its own identity and ISP IP, each at the same pace, so about 6 times faster. Keep it below your number of ISP IPs (see below) |
| Rotation | 500 | `--identity-requests 500`: each identity (and so each IP) retires after 500 to 750 requests and the next IP takes over, so IPs rest in turn. One IP met Instagram's rate limit after 1,100 to 1,300 requests in the full run |
| Waiting | 20 minutes | `--max-wait 20m`: when every IP is cooling down after blocks, each profile waits this long before it is given up |

**How many at once.** Each worker uses its own ISP IP at the same pace as a
single run, so the load per IP does not change, as long as there are more
IPs than workers (one more IP serves the cursor check, and spares replace
blocked ones). With 10 IPs, use 6. Count your ISP endpoints without showing
the credentials (endpoints named by port on one host, such as
`isp.decodo.com:10001`, count one IP per port):

```bash
python3 -c "t=open('phase0/proxies.yaml').read();s=t.split('name: isp',1)[1].split('- name:',1)[0];print(sum(1 for l in s.splitlines() if l.strip().startswith('- ')),'ISP endpoints')"
```

A block cools down the IP that got it for a while: for endpoints given as
IP addresses, its whole /24; for endpoints named by host and port, that
port alone. Watch the progress lines for `wall:`; while every IP is cooling
down, a profile waits (`--max-wait`) rather than failing.

**The full list.** The 348 accounts, back one year or 1,200 posts each (whichever comes first), with results kept apart from the test's:

```bash
make igs-probe IGS_PROBE_PROFILES=phase0/ig-profiles-all.txt IGS_PROBE_OUT=phase0/out-ig-all IGS_PROBE_ARGS="--max-age-days 365 --max-grid-pages 100"
```

A busy news account posts 30 or more times a day, so it reaches the 1,200-post cap after a few weeks; a quieter one reaches the year first. At one request every 3 seconds, a capped account takes about 6 minutes, so plan on a day or more.

## Speed tests (one switch at a time)

Three switches may make comment collection faster. Each is a guess about
what Instagram tolerates, so test them one at a time on the 5-profile test
list, each in its own output folder, and compare each `speed:` summary line
with the baseline's. About 5 minutes each.

| # | Switch | Works if the `speed:` line shows |
|---|---|---|
| 0 | none (baseline) | pages of up to 12 posts and 10 comments; note the comments a second |
| 1 | `--grid-page-size 50 --comment-page-size 50`: ask for 50 per page | pages of up to more than 12 and 10, answers all `ok` |
| 2 | `--comment-rps 1`: comment requests at 1 a second (everything else stays at 0.3) | more comments a second than the baseline, no `rate_limited` |
| 3 | `--comment-hop 4`: each comment page from the next of 4 extra identities | more comments a second, answers all `ok` (an invalid cursor would mean cursors are tied to the identity), `4 hop identities` |

```bash
make igs-probe IGS_PROBE_OUT=phase0/out-speed-0 IGS_PROBE_ARGS="--max-grid-pages 3 --comment-posts 10 --comment-pages 30"
```

```bash
make igs-probe IGS_PROBE_OUT=phase0/out-speed-1 IGS_PROBE_ARGS="--max-grid-pages 3 --comment-posts 10 --comment-pages 30 --grid-page-size 50 --comment-page-size 50"
```

```bash
make igs-probe IGS_PROBE_OUT=phase0/out-speed-2 IGS_PROBE_ARGS="--max-grid-pages 3 --comment-posts 10 --comment-pages 30 --comment-rps 1"
```

```bash
make igs-probe IGS_PROBE_OUT=phase0/out-speed-3 IGS_PROBE_ARGS="--max-grid-pages 3 --comment-posts 10 --comment-pages 30 --comment-hop 4"
```

The command behind it, for other options (pacing, grid depth, fingerprint):
`go run ./cmd/igs-spike probe --help`.
