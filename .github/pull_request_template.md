## Why

<!-- The problem or the request (REQ-..., fix number, gap), in a few lines. -->

## What

<!-- What changes, by area. Name anything other services use (topics, event fields, schemas, endpoints): additions only. -->

## Tests

<!-- The tests that prove it, and that they fail without the change. -->

- [ ] Schema: migrations only add, so the previous release runs on the new schema (`scripts/migration-check.py`; a breaking one is marked `-- igs:breaking:` with its two-release plan).
- [ ] No credentials, real usernames, names, comment text or HARs in the change.

## For the platform

<!-- What changes in production when this is pinned, or "nothing". -->
