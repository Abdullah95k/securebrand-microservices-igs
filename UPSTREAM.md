# Upstream: fbs

This repository (igs) owns a copy of the code of fbs, the Facebook page scraper. The decision and its reasons are in [ADR 0004](docs/adr/0004-ig-code-organisation.md). igs work never changes fbs's repository.

- **Upstream repository:** [`Abdullah95k/securebrand-microservices`](https://github.com/Abdullah95k/securebrand-microservices)
- **Copied from:** `f1377a9` (main, 2026-10-31), with its full history, so `git blame` reaches back into fbs.
- **Seed changes:** `.github/workflows/release.yml` removed (on a `v*` tag it would have published `ghcr.io/<owner>/fbs` and fbs's Helm chart); this file and a note at the top of the README added.

To port an fbs change: `git remote add upstream https://github.com/Abdullah95k/securebrand-microservices`, `git fetch upstream`, then `git cherry-pick <sha>` while the paths still match, or port it by hand. Record it below either way.

## Ported from fbs

| Date | fbs commit | What | igs PR |
|---|---|---|---|

## igs fixes fbs may want

Fixes made in igs's copy of shared code that fbs still lacks. fbs's own sessions decide whether to port them.

| Date | igs PR | Defect | fbs location |
|---|---|---|---|
