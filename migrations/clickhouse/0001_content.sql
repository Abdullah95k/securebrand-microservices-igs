-- The shared content store (SecureBrand architecture §6), fed by the
-- content.* topics every collector's normalizer publishes. Ids are text
-- (platform + native_id); joins use cityHash64(platform, id), kept as
-- materialized *_key columns. Posts, comments, accounts, authors and media
-- keep their newest version (ReplacingMergeTree(version, is_deleted)): read
-- them with FINAL, or argMax by version. Metrics and snapshots are series,
-- one row per observation (a redelivered one collapses on merge).

CREATE TABLE IF NOT EXISTS posts (
    platform          LowCardinality(String),
    post_id           String,
    post_key          UInt64 MATERIALIZED cityHash64(platform, post_id),
    account_id        String,
    account_key       UInt64 MATERIALIZED cityHash64(platform, account_id),
    kind              LowCardinality(String),
    title             String,
    text              String,
    description       String,
    language          LowCardinality(String),
    published_at      DateTime64(3, 'UTC'),
    url               String,
    views             Nullable(Int64),
    likes             Nullable(Int64),
    shares            Nullable(Int64),
    comments          Nullable(Int64),
    media_count       UInt32,
    coauthor_ids      Array(String),
    quoted_post_id    String,
    extras            String,
    is_deleted        UInt8,
    version           UInt64,
    observed_at       DateTime64(3, 'UTC'),
    collector         LowCardinality(String),
    collector_version LowCardinality(String),
    raw_ref           String,
    event_id          String
) ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY toYYYYMM(published_at)
ORDER BY (platform, post_id);

-- A comment always has its post; a reply also its parent comment.
CREATE TABLE IF NOT EXISTS comments (
    platform          LowCardinality(String),
    comment_id        String,
    comment_key       UInt64 MATERIALIZED cityHash64(platform, comment_id),
    post_id           String,
    post_key          UInt64 MATERIALIZED cityHash64(platform, post_id),
    parent_comment_id String,
    account_id        String,
    author_id         String,
    author_key        String,
    author_name       String,
    text              String,
    language          LowCardinality(String),
    published_at      DateTime64(3, 'UTC'),
    likes             Nullable(Int64),
    replies           Nullable(Int64),
    extras            String,
    is_deleted        UInt8,
    version           UInt64,
    observed_at       DateTime64(3, 'UTC'),
    collector         LowCardinality(String),
    collector_version LowCardinality(String),
    raw_ref           String,
    event_id          String
) ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY toYYYYMM(published_at)
ORDER BY (platform, post_id, comment_id);

CREATE TABLE IF NOT EXISTS post_metrics (
    platform     LowCardinality(String),
    post_id      String,
    post_key     UInt64 MATERIALIZED cityHash64(platform, post_id),
    account_id   String,
    observed_at  DateTime64(3, 'UTC'),
    views        Nullable(Int64),
    likes        Nullable(Int64),
    shares       Nullable(Int64),
    comments     Nullable(Int64),
    reactions    Map(String, Int64),
    counts_hidden UInt8,
    raw_ref      String
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (platform, post_id, observed_at);

CREATE TABLE IF NOT EXISTS comment_metrics (
    platform     LowCardinality(String),
    comment_id   String,
    comment_key  UInt64 MATERIALIZED cityHash64(platform, comment_id),
    post_id      String,
    observed_at  DateTime64(3, 'UTC'),
    likes        Nullable(Int64),
    replies      Nullable(Int64),
    raw_ref      String
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (platform, comment_id, observed_at);

-- Accounts carry what links them to the same brand or person elsewhere:
-- bio, website and every profile link (architecture §7).
CREATE TABLE IF NOT EXISTS accounts (
    platform          LowCardinality(String),
    account_id        String,
    account_key       UInt64 MATERIALIZED cityHash64(platform, account_id),
    handle            String,
    name              String,
    kind              LowCardinality(String),
    verified          UInt8,
    category          String,
    country_signals   Array(String),
    bio               String,
    website           String,
    link_urls         Array(String),
    link_titles       Array(String),
    picture_url       String,
    followers         Nullable(Int64),
    following         Nullable(Int64),
    post_count        Nullable(Int64),
    is_private        UInt8,
    is_business       UInt8,
    extras            String,
    is_deleted        UInt8,
    version           UInt64,
    observed_at       DateTime64(3, 'UTC'),
    collector         LowCardinality(String),
    collector_version LowCardinality(String),
    raw_ref           String,
    event_id          String
) ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (platform, account_id);

CREATE TABLE IF NOT EXISTS account_snapshots (
    platform     LowCardinality(String),
    account_id   String,
    account_key  UInt64 MATERIALIZED cityHash64(platform, account_id),
    observed_at  DateTime64(3, 'UTC'),
    followers    Nullable(Int64),
    following    Nullable(Int64),
    post_count   Nullable(Int64),
    raw_ref      String
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (platform, account_id, observed_at);

-- Comment authors: by id, or by their keyed hash when the collector
-- pseudonymizes them.
CREATE TABLE IF NOT EXISTS authors (
    platform     LowCardinality(String),
    author_ref   String,
    author_id    String,
    author_key   String,
    name         String,
    profile_url  String,
    picture_url  String,
    verified     UInt8,
    version      UInt64,
    observed_at  DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (platform, author_ref);

-- Media files are kept only until their analysis is complete; the rows
-- stay.
CREATE TABLE IF NOT EXISTS media (
    platform         LowCardinality(String),
    media_id         String,
    owner_type       LowCardinality(String),
    owner_id         String,
    type             LowCardinality(String),
    url              String,
    url_expires_at   Nullable(DateTime64(3, 'UTC')),
    width            Int32,
    height           Int32,
    duration_ms      Int64,
    alt_text         String,
    position         Int32,
    storage_key      String,
    analysis_status  LowCardinality(String),
    is_deleted       UInt8,
    version          UInt64,
    observed_at      DateTime64(3, 'UTC'),
    raw_ref          String
) ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (platform, media_id);

-- The entity layer (architecture §7): the real-world brand or person, and
-- how each platform account was linked to it. Filled by entity linking and
-- the client app, not by collectors.
CREATE TABLE IF NOT EXISTS entities (
    entity_id    String,
    type         LowCardinality(String),
    name_ar      String,
    name_ku      String,
    name_en      String,
    category     String,
    is_deleted   UInt8,
    version      UInt64,
    updated_at   DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY entity_id;

CREATE TABLE IF NOT EXISTS entity_accounts (
    entity_id     String,
    platform      LowCardinality(String),
    account_id    String,
    account_key   UInt64 MATERIALIZED cityHash64(platform, account_id),
    -- manual | bio_link | website | name | picture | content | ai
    method        LowCardinality(String),
    confidence    Float32,
    confirmed_by  String,
    confirmed_at  Nullable(DateTime64(3, 'UTC')),
    is_deleted    UInt8,
    version       UInt64,
    updated_at    DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (entity_id, platform, account_id);
