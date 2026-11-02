-- +goose Up
-- igs's schema (ADR 0005 §4). Every statement runs in schema ig: the
-- migrator creates it and pins the search path.
SET LOCAL search_path TO ig;

-- Profiles --------------------------------------------------------------------
-- Keyed by the user pk, never the username, which changes (D8).
CREATE TABLE profiles (
    user_pk               bigint PRIMARY KEY,
    username              text,
    full_name             text,
    biography             text,
    bio_links             jsonb,
    verified              boolean,
    private               boolean,
    memorialized          boolean,
    follower_count        bigint,
    following_count       bigint,
    media_count           bigint,
    profile_pic_url       text,
    linked_facebook_page  text,
    category              text,
    is_business           boolean,
    highlight_count       int,
    highlights_hash       bytea,
    author_key            bytea NOT NULL,
    status                text NOT NULL DEFAULT 'active',  -- active | private | unavailable | suppressed
    extra                 jsonb,
    content_hash          bytea,
    first_seen_at         timestamptz NOT NULL DEFAULT now(),
    last_scraped_at       timestamptz,
    last_profile_read_at  timestamptz
);
CREATE UNIQUE INDEX profiles_username_idx ON profiles (lower(username)) WHERE username IS NOT NULL AND status <> 'suppressed';
CREATE INDEX profiles_author_key_idx ON profiles (author_key);

-- Daily follower history and username history, monthly partitions.
CREATE TABLE profile_snapshots (
    user_pk          bigint NOT NULL,
    captured_at      timestamptz NOT NULL,
    username         text,
    full_name        text,
    follower_count   bigint,
    following_count  bigint,
    media_count      bigint,
    verified         boolean,
    private          boolean,
    PRIMARY KEY (user_pk, captured_at)
) PARTITION BY RANGE (captured_at);

-- Posts: monthly partitions on taken_at, so a post and its comments share a
-- partition and retention is a DROP. --------------------------------------------
CREATE TABLE posts (
    taken_at               timestamptz NOT NULL,
    media_pk               bigint NOT NULL,
    owner_pk               bigint NOT NULL,
    shortcode              text NOT NULL,
    media_type             smallint NOT NULL,     -- 1 photo | 2 video | 8 carousel
    product_type           text,                  -- feed | clips | carousel_container | igtv
    caption                text,
    caption_edited         boolean,
    hashtags               text[],
    mentions               text[],
    accessibility_caption  text,
    location               jsonb,
    usertags               jsonb,
    coauthors              jsonb,
    music                  jsonb,
    topics                 text[],
    ai_label               text,
    like_count             bigint,                -- NULL when hidden
    counts_hidden          boolean NOT NULL DEFAULT false,
    comment_count          bigint,
    commenting_disabled    boolean,
    play_count             bigint,                -- reels: from the reels tab
    is_pinned              boolean,
    child_count            int,
    video_duration_ms      bigint,
    width                  int,
    height                 int,
    has_audio              boolean,
    media_ids              text[],
    discovered_via         text NOT NULL DEFAULT 'grid',
    comments_state         text NOT NULL DEFAULT 'none',  -- complete | partial | truncated | none
    comments_collected     bigint,
    permalink              text,
    extra                  jsonb,
    content_hash           bytea NOT NULL,
    status                 text NOT NULL DEFAULT 'visible',
    first_seen_at          timestamptz NOT NULL,
    last_seen_at           timestamptz NOT NULL,
    PRIMARY KEY (taken_at, media_pk)
) PARTITION BY RANGE (taken_at);
CREATE INDEX posts_owner_idx ON posts (owner_pk, taken_at DESC);
CREATE INDEX posts_hashtags_idx ON posts USING gin (hashtags);

-- Media pk to owner and time (the partition key), and the unique shortcode.
CREATE TABLE post_index (
    media_pk   bigint PRIMARY KEY,
    owner_pk   bigint NOT NULL,
    taken_at   timestamptz NOT NULL,
    shortcode  text NOT NULL
);
CREATE UNIQUE INDEX post_index_shortcode_idx ON post_index (shortcode);
CREATE INDEX post_index_owner_idx ON post_index (owner_pk, taken_at DESC);

-- Collab posts appear on several grids.
CREATE TABLE post_owners (
    media_pk  bigint NOT NULL,
    user_pk   bigint NOT NULL,
    role      text NOT NULL,   -- owner | coauthor
    PRIMARY KEY (media_pk, user_pk)
);
CREATE INDEX post_owners_user_idx ON post_owners (user_pk);

-- Engagement time series: a row only when values changed. Daily partitions.
CREATE TABLE post_metrics (
    media_pk       bigint NOT NULL,
    captured_at    timestamptz NOT NULL,
    owner_pk       bigint NOT NULL,
    like_count     bigint,
    comment_count  bigint,
    play_count     bigint,
    counts_hidden  boolean NOT NULL DEFAULT false,
    PRIMARY KEY (media_pk, captured_at)
) PARTITION BY RANGE (captured_at);

-- Comments: top level only logged out; parent and depth leave room for
-- replies. Partitioned like posts. ------------------------------------------------
CREATE TABLE comments (
    taken_at           timestamptz NOT NULL,   -- the post's
    media_pk           bigint NOT NULL,
    comment_pk         bigint NOT NULL,
    owner_pk           bigint NOT NULL,        -- the post's owner
    parent_comment_pk  bigint,
    depth              smallint NOT NULL DEFAULT 0,
    author_key         bytea,
    author_pk          bigint,                 -- NULL when pseudonymized or erased
    text               text,
    created_at         timestamptz,
    like_count         bigint,
    is_op              boolean,
    child_comment_count bigint,
    extra              jsonb,
    status             text NOT NULL DEFAULT 'visible',
    redacted           boolean NOT NULL DEFAULT false,
    edit_count         integer NOT NULL DEFAULT 0,
    edited_at          timestamptz,
    deleted_at         timestamptz,
    content_hash       bytea NOT NULL,
    first_seen_at      timestamptz NOT NULL,
    last_seen_at       timestamptz NOT NULL,
    PRIMARY KEY (taken_at, media_pk, comment_pk)
) PARTITION BY RANGE (taken_at);
CREATE INDEX comments_post_idx ON comments (media_pk, created_at, comment_pk);
CREATE INDEX comments_author_idx ON comments (author_key);

CREATE TABLE comment_authors (
    author_key       bytea PRIMARY KEY,
    author_pk        bigint,
    username         text,
    verified         boolean,
    profile_pic_url  text,
    first_seen_at    timestamptz NOT NULL,
    last_seen_at     timestamptz NOT NULL
);
CREATE INDEX comment_authors_pk_idx ON comment_authors (author_pk);
CREATE INDEX comment_authors_seen_idx ON comment_authors (last_seen_at);

-- Media (metadata always; files only with ig.limits.include_media) ----------------
CREATE TABLE media (
    media_id           uuid PRIMARY KEY,
    source_media_id    text NOT NULL,
    variant            text NOT NULL,
    kind               text NOT NULL,
    owner_type         text NOT NULL,   -- post | comment | profile | highlight
    user_pk            bigint,
    media_pk           bigint,
    parent_media_pk    bigint,
    position           int,
    comment_pk         bigint,
    source_url         text,
    source_expires_at  timestamptz,
    width              int,
    height             int,
    duration_ms        bigint,
    mime               text,
    alt_text           text,
    status             text NOT NULL DEFAULT 'discovered',
        -- discovered | stored | ready | deleted | failed | expired | skipped
    s3_bucket          text,
    s3_key             text,
    size_bytes         bigint,
    sha256             bytea,
    attempts           int NOT NULL DEFAULT 0,
    retryable_failures int NOT NULL DEFAULT 0,
    last_error         text,
    delete_after       timestamptz,
    discovered_at      timestamptz NOT NULL DEFAULT now(),
    stored_at          timestamptz,
    ready_at           timestamptz,
    processed_at       timestamptz,
    deleted_at         timestamptz
);
CREATE INDEX media_post_idx ON media (media_pk);
CREATE INDEX media_user_idx ON media (user_pk);
CREATE INDEX media_ready_idx ON media (delete_after) WHERE status = 'ready';
CREATE INDEX media_comment_idx ON media (comment_pk) WHERE comment_pk IS NOT NULL;

CREATE TABLE media_acks (
    media_id  uuid NOT NULL REFERENCES media (media_id) ON DELETE CASCADE,
    consumer  text NOT NULL,
    status    text NOT NULL,
    error     text,
    acked_at  timestamptz NOT NULL,
    PRIMARY KEY (media_id, consumer)
);

-- Highlights (items read only when the list changed) ------------------------------
CREATE TABLE highlights (
    highlight_id  bigint PRIMARY KEY,
    user_pk       bigint NOT NULL,
    title         text,
    cover_url     text,
    item_count    int,
    status        text NOT NULL DEFAULT 'visible',
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX highlights_user_idx ON highlights (user_pk);

CREATE TABLE highlight_items (
    highlight_id  bigint NOT NULL REFERENCES highlights (highlight_id) ON DELETE CASCADE,
    item_pk       bigint NOT NULL,
    taken_at      timestamptz,
    media_type    smallint,
    media_ids     text[],
    PRIMARY KEY (highlight_id, item_pk)
);

-- Crawl state ----------------------------------------------------------------------
CREATE TABLE crawl_cursors (
    scope_type           text NOT NULL,   -- profile_grid | profile_reels
    scope_id             bigint NOT NULL,
    ordering             text NOT NULL DEFAULT 'default',
    cursor               text,
    newest_seen_at       timestamptz,
    oldest_seen_at       timestamptz,
    state                text NOT NULL,   -- complete | partial | truncated
    truncated_reason     text,
    last_full_at         timestamptz,
    last_incremental_at  timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope_type, scope_id, ordering)
);

CREATE TABLE api_clients (
    id               uuid PRIMARY KEY,
    name             text NOT NULL,
    key_prefix       text NOT NULL UNIQUE,
    key_hash         bytea NOT NULL,
    scopes           text[] NOT NULL,
    rate_per_minute  int,
    created_at       timestamptz NOT NULL DEFAULT now(),
    revoked_at       timestamptz
);

CREATE TABLE jobs (
    job_id           uuid PRIMARY KEY,
    api_client_id    uuid REFERENCES api_clients (id),
    kind             text NOT NULL,   -- on_demand | scheduled | backfill
    lane             smallint NOT NULL,
    mode             text NOT NULL,   -- full | incremental
    profile_ref      text NOT NULL,   -- u:<username> | id:<pk>
    user_pk          bigint,
    params           jsonb NOT NULL,
    status           text NOT NULL,   -- queued | running | finalizing | completed | partial | failed | cancelled
    idempotency_key  text,
    counters         jsonb,
    completeness     jsonb,
    error            jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    dispatched_at    timestamptz,
    finished_at      timestamptz
);
CREATE UNIQUE INDEX jobs_idem_idx ON jobs (api_client_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_profile_idx ON jobs (profile_ref, created_at DESC);
CREATE INDEX jobs_status_idx ON jobs (status, created_at) WHERE status IN ('queued', 'running', 'finalizing');

-- Scheduled re-scrapes (every 3 hours, D2), one row per profile reference.
CREATE TABLE profile_schedule (
    profile_ref           text PRIMARY KEY,
    user_pk               bigint,
    enabled               boolean NOT NULL DEFAULT true,
    tier                  text NOT NULL DEFAULT 'T1',
    interval_seconds      int NOT NULL DEFAULT 10800,
    next_run_at           timestamptz NOT NULL,
    options               jsonb NOT NULL DEFAULT '{}',
    max_age_days          int,
    max_posts             bigint,
    consecutive_failures  int NOT NULL DEFAULT 0,
    paused_until          timestamptz,
    last_job_id           uuid,
    last_run_at           timestamptz,
    adaptive              boolean NOT NULL DEFAULT false,
    posts_per_day         double precision,
    last_post_at          timestamptz,
    tier_changed_at       timestamptz,
    next_profile_read_at  timestamptz,       -- the daily profile refresh
    created_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX profile_schedule_due_idx ON profile_schedule (next_run_at) WHERE enabled;

CREATE TABLE profile_registrations (
    api_client_id  uuid NOT NULL REFERENCES api_clients (id),
    profile_ref    text NOT NULL REFERENCES profile_schedule (profile_ref) ON DELETE CASCADE,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (api_client_id, profile_ref)
);

-- Comment refresh of posts older than the grid overlap.
CREATE TABLE post_refresh (
    media_pk            bigint PRIMARY KEY,
    user_pk             bigint NOT NULL,
    taken_at            timestamptz NOT NULL,
    next_refresh_at     timestamptz NOT NULL,
    last_refresh_at     timestamptz,
    refreshes           int NOT NULL DEFAULT 0,
    unchanged           int NOT NULL DEFAULT 0,
    last_comment_count  bigint,
    recrawls            int NOT NULL DEFAULT 0
);
CREATE INDEX post_refresh_due_idx ON post_refresh (user_pk, next_refresh_at);

-- GDPR ---------------------------------------------------------------------------
CREATE TABLE erasure_requests (
    id             uuid PRIMARY KEY,
    api_client_id  uuid REFERENCES api_clients (id),
    subject_type   text NOT NULL,   -- author | profile
    author_key     bytea,
    user_pk        bigint,
    input          jsonb NOT NULL,
    status         text NOT NULL,   -- pending | done | failed
    stats          jsonb,
    error          text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz
);
CREATE INDEX erasure_pending_idx ON erasure_requests (created_at) WHERE status = 'pending';

CREATE TABLE suppression (
    subject_type  text NOT NULL,   -- author | profile
    subject_key   text NOT NULL,   -- hex author_key or user pk
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_type, subject_key)
);

-- Query registry (hot-reloaded by crawlers), seeded with configs/ig/queries.seed.yaml.
CREATE TABLE queries (
    name                text NOT NULL,
    doc_id              text NOT NULL,
    friendly_name       text NOT NULL,
    variables_template  jsonb NOT NULL,
    parser              text NOT NULL,
    parser_version      int NOT NULL,
    status              text NOT NULL,  -- candidate | active | fallback | retired
    priority            int NOT NULL DEFAULT 0,
    source              text NOT NULL,  -- seed | bundle | har | manual
    endpoint            text NOT NULL DEFAULT '',
    headers             jsonb NOT NULL DEFAULT '[]',
    discovered_at       timestamptz NOT NULL DEFAULT now(),
    promoted_at         timestamptz,
    last_ok_at          timestamptz,
    last_checked_at     timestamptz,
    last_check          text,
    check_failures      int NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, doc_id)
);

CREATE TABLE client_params (
    key         text PRIMARY KEY,
    value       text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE registry_sync_runs (
    run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL,
    status       text NOT NULL,     -- ok | degraded | error
    report       jsonb NOT NULL
);
CREATE INDEX registry_sync_runs_started_idx ON registry_sync_runs (started_at DESC);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE registry_sync_runs;
DROP TABLE client_params;
DROP TABLE queries;
DROP TABLE suppression;
DROP TABLE erasure_requests;
DROP TABLE post_refresh;
DROP TABLE profile_registrations;
DROP TABLE profile_schedule;
DROP TABLE jobs;
DROP TABLE api_clients;
DROP TABLE crawl_cursors;
DROP TABLE highlight_items;
DROP TABLE highlights;
DROP TABLE media_acks;
DROP TABLE media;
DROP TABLE comment_authors;
DROP TABLE comments;
DROP TABLE post_metrics;
DROP TABLE post_owners;
DROP TABLE post_index;
DROP TABLE posts;
DROP TABLE profile_snapshots;
DROP TABLE profiles;
