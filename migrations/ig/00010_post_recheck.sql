-- +goose Up
-- REQ-008 fix 14: each post is read once more, in full, when it is about
-- 30 days old (ig.schedule.post_recheck_after), to catch deletions.
SET LOCAL search_path TO ig;
ALTER TABLE posts ADD COLUMN rechecked_at timestamptz;

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE posts DROP COLUMN rechecked_at;
