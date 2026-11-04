-- +goose Up
-- M12d: append, don't overwrite (architecture §2.4), and highlights as
-- posts (the owner, 2026-11-04).
SET LOCAL search_path TO ig;

-- Every post has a kind: post (photo or carousel), video, reel, or
-- highlight (an item of one of the profile's highlights, which sits in
-- posts with the highlight it belongs to).
ALTER TABLE posts ADD COLUMN kind text NOT NULL DEFAULT 'post';
ALTER TABLE posts ADD COLUMN highlight_id bigint;
UPDATE posts SET kind = CASE WHEN product_type = 'clips' THEN 'reel' WHEN media_type = 2 THEN 'video' ELSE 'post' END;
CREATE INDEX posts_highlight_idx ON posts (highlight_id) WHERE highlight_id IS NOT NULL;
-- No igs deployment held highlight items yet: the items now live in posts.
DROP TABLE highlight_items;

-- The text a caption or comment had before each edit, kept forever.
CREATE TABLE post_edits (
    media_pk     bigint NOT NULL,
    taken_at     timestamptz NOT NULL,
    owner_pk     bigint NOT NULL,
    replaced_at  timestamptz NOT NULL,   -- when the new caption was seen
    caption      text,                   -- the caption before the edit
    PRIMARY KEY (media_pk, replaced_at)
);
CREATE INDEX post_edits_owner_idx ON post_edits (owner_pk);

CREATE TABLE comment_edits (
    comment_pk   bigint NOT NULL,
    media_pk     bigint NOT NULL,
    taken_at     timestamptz NOT NULL,   -- the post's
    owner_pk     bigint NOT NULL,        -- the post's owner
    author_key   bytea,
    replaced_at  timestamptz NOT NULL,
    text         text,                   -- the text before the edit
    PRIMARY KEY (comment_pk, replaced_at)
);
CREATE INDEX comment_edits_author_idx ON comment_edits (author_key);
CREATE INDEX comment_edits_owner_idx ON comment_edits (owner_pk);

-- A comment's likes and replies each time they changed (architecture §6
-- comment_metrics). Monthly partitions by capture time.
CREATE TABLE comment_metrics (
    comment_pk          bigint NOT NULL,
    captured_at         timestamptz NOT NULL,
    media_pk            bigint NOT NULL,
    owner_pk            bigint NOT NULL,
    like_count          bigint,
    child_comment_count bigint,
    PRIMARY KEY (comment_pk, captured_at)
) PARTITION BY RANGE (captured_at);
CREATE INDEX comment_metrics_owner_idx ON comment_metrics (owner_pk);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE comment_metrics;
DROP TABLE comment_edits;
DROP TABLE post_edits;
CREATE TABLE highlight_items (
    highlight_id  bigint NOT NULL REFERENCES highlights (highlight_id) ON DELETE CASCADE,
    item_pk       bigint NOT NULL,
    taken_at      timestamptz,
    media_type    smallint,
    media_ids     text[],
    PRIMARY KEY (highlight_id, item_pk)
);
DELETE FROM posts WHERE kind = 'highlight';
DROP INDEX posts_highlight_idx;
ALTER TABLE posts DROP COLUMN highlight_id;
ALTER TABLE posts DROP COLUMN kind;
