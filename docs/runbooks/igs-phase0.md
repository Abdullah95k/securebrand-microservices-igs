# igs Phase 0: GO or NO-GO per proxy tier (checkpoint C3)

Phase 0 measures what logged-out Instagram serves through each proxy tier,
on about 150 public profiles, before igs is scaled up. It runs the same
reads as the probe (page load, profile, grid, reels tab, post pages,
comments, highlights) at the probe's pace, and says **GO** or **NO-GO** for
each tier:

| Check | GO needs | Why |
|---|---|---|
| Bootstrap | at least 95% of profiles' page loads get tokens (after retries on other exits) | without tokens nothing can be read |
| Block rate | at most 5% of queries answered by a rate limit, a login wall or a checkpoint | blocks cost requests and exits |
| Complete grids | at least 90% of grids read back to the age window (365 days) or their end | the backfill must reach the window |
| Latency | the 95th percentile query under 5 seconds | slow tiers stretch every run |

The thresholds come from your probe runs (C2); change them with
`--min-bootstrap`, `--max-block-rate`, `--min-complete` and `--max-p95-ms`.
Grids that stop at the kit's own page cap (`--max-grid-pages`) are reported
apart, never counted against the tier.

The kit writes **anonymous** results: profiles are numbered by their line in
your file, never named, and the few raw answers it keeps as test fixtures are
scrubbed of usernames, names, ids and text first. The bundle you send back
holds only those files, and the kit refuses to write it if a proxy password
or user name shows up in it.

It takes about as long as the probe per profile (2 to 3 minutes, 3 profiles
at once by default), so 150 profiles are about 2 hours per tier.

## What you need

| | |
|---|---|
| A machine | The Phase 0 droplet, with this repository cloned (see [`igs-probe.md`](igs-probe.md), step 1) |
| Proxies | `phase0/proxies.yaml`, as for the probe; Phase 0 compares the tiers you list (for example `isp` and `residential`) |
| Profiles | about 150 public profiles, one per line, in `phase0/ig-profiles.txt`: big and small accounts, a few with highlights and reels, a few that post daily |

## 1. Update the code

Log in to the droplet:

```bash
ssh root@YOUR_DROPLET_IP
```

```bash
cd ~/securebrand-microservices-igs
```

```bash
git pull
```

## 2. Rehearse (no traffic)

This runs every step against a fake Instagram on the droplet itself and must
end with `phase0: GO`:

```bash
make igs-phase0-dryrun
```

## 3. List the profiles

Put one profile per line in `phase0/ig-profiles.txt` (usernames, `@names` or
profile links). To start from the probe's list:

```bash
cp phase0/ig-profiles.example.txt phase0/ig-profiles.txt
```

Then add lines with `printf`, for example:

```bash
printf 'brand_one\nbrand_two\n' >> phase0/ig-profiles.txt
```

## 4. Run it

Inside tmux, so a dropped connection does not stop it:

```bash
tmux new -s igs
```

For the ISP tier only:

```bash
make igs-phase0
```

To compare ISP with residential (the brief asks for both, when you want to):

```bash
make igs-phase0 IGS_PHASE0_TIERS=isp,residential
```

If it stops (Ctrl-C, a dropped session), resume from the step it was in,
for example:

```bash
make igs-phase0 IGS_PHASE0_ARGS="--from run"
```

The steps are `preflight` (each tier loads a page; exit IPs), `docids`
(finds the queries' current doc_ids in Instagram's scripts and checks each
query), `capture` (keeps a few scrubbed answers and checks the parsers still
read them), `smoke` (3 profiles; prints the estimated time and cost of the
full run), `run` (every profile), `report` and `bundle`.

## 5. Read the result

The last lines say `GO` or `NO-GO` per tier and the recommended tier.
`phase0/out-ig-phase0/report.txt` has the details: bootstrap, block rate,
grid stops, depth in days, latency and bytes per request kind, and the
projected monthly cost. A failed check comes with what to try.

The exit code says the same: 0 GO, 3 NO-GO, 4 needs action (the doc_ids or
the parsers need a code change: send the bundle), 1 an error, 130 stopped.

## 6. Send the bundle

On your Mac:

```bash
scp "root@YOUR_DROPLET_IP:securebrand-microservices-igs/phase0/out-ig-phase0/igs-phase0-bundle.tar.gz" ~/Downloads/
```

Attach it to the next build session, or share it by Drive link. The session
sets igs's config, sizing, budget and alert thresholds from it.
