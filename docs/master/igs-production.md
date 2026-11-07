# igs on the production cluster

What the platform needs to build and install igs (REQ-008 fix 11a; the master's checklist of 6 November). Times are Iraq time. Nothing here is a secret.

## Build

| What | Value |
|---|---|
| Commit | The full SHA of `main` at the time of the first production run (sent then; every merge to `main` passed CI) |
| Image | One image, `igs`, holding every binary; each Deployment picks its command |
| Dockerfile | `deploy/docker/Dockerfile` |
| Context | the repository root |
| Target | the default (the last stage: distroless, static binaries, non-root). The `browser` target (Chromium) is not needed: the browser fallback is off |
| Go | 1.27 (the build stage's image) |

## Install

| What | Value |
|---|---|
| Chart | `deploy/helm/igs` |
| Values | `deploy/helm/igs/values-production.yaml` |
| Namespace | `igs` |
| Image keys | `image.repository`, `image.tag` (`image.browserTag` stays empty) |
| Command | `helm upgrade --install igs deploy/helm/igs -n igs -f deploy/helm/igs/values-production.yaml --set image.repository=<registry>/igs --set image.tag=<sha>` |

The values file has five placeholders to fill: the image (above), the three canary profiles for `igs-registry-sync`, and KEDA's spelling of the Kafka SASL mechanism and TLS (`keda.sasl`, `keda.tls`, for example `scram_sha512` and `enable`).

## Secrets igs reads (namespace `igs`)

**`platform-credentials`** (the platform's). Its keys reach the pods as environment variables and igs reads them by these names; a production pod stops at start, naming the missing keys, if any it needs is absent:

| Keys | igs uses them for |
|---|---|
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SSLMODE` | its database (schema `ig`; the migrate hook creates it, so the user needs DDL rights there) |
| `KAFKA_BROKERS`, `KAFKA_USER`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM`, `KAFKA_TLS` | Kafka; KEDA's lag triggers use `KAFKA_BROKERS`, `KAFKA_USER` and `KAFKA_PASSWORD` too |
| `VALKEY_HOST` and `VALKEY_PORT`, or `VALKEY_ADDR`, or `VALKEY_URL`; `VALKEY_PASSWORD`, `VALKEY_TLS`, `VALKEY_DB` | Valkey. **The exact `VALKEY_*` names were not given**: igs accepts any of these spellings. Please confirm which the Secret has |
| `S3_ENDPOINT` (a host or an `https://` URL), `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET_RAW`, `S3_BUCKET_MEDIA` | raw responses and media files |
| `S3_BUCKET_BACKUPS` | read, not used yet: igs runs no backup job of its own |
| `SCHEMA_REGISTRY_URL` | the registry; igs registers no shared subject |
| `CONTRACT_VERSION` | recorded; used when igs adopts the contract (REQ-019) |

**`proxies`**, key `proxies.yaml`: the ISP exits, as a config layer (`ig.proxy.tiers`, one tier named `isp`); `phase0/proxies.example.yaml` shows the shape.

**`igs-keys`**, key `IGS_IG__PRIVACY__AUTHOR_HMAC_KEY`: igs's own key for the authors' HMAC (32 or more random characters, never fbs's). `platform-credentials` has no such key, and igs won't start in production without one. **Open question:** the platform adds this Secret, or the key goes into `platform-credentials` under that name.

## What igs does and doesn't do on shared stores

- **Kafka:** creates no topic and registers no schema (`hooks.topics` and `hooks.schemas` are off). It reads and writes the topics the platform created in `infra/production/kafka.tf`.
- **S3:** creates no bucket, sets no lifecycle rule, and keeps to its prefixes:
  - `igs/keep/` in `S3_BUCKET_RAW`: raw failures, kept;
  - `igs/30d/` in `S3_BUCKET_RAW`: the other raw responses. A platform rule may expire them after 30 days;
  - `igs/` in `S3_BUCKET_MEDIA`: media files, deleted by igs once the analysis service acknowledged them. igs's orphan sweep lists and deletes under `igs/` only.
- **Postgres:** schema `ig` in `POSTGRES_DATABASE`, created and migrated by the pre-install/upgrade hook (`igsctl migrate up`).
- **Valkey:** every key starts with `igs:`.

## Scaling

KEDA's maxima equal the partitions of the topics each component consumes (the platform's sizes): crawler-profile 6, crawler-post 12, persister 12, normalizer 12, media-downloader 3.
