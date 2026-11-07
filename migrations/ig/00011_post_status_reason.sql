-- +goose Up
-- Why a post is unavailable (REQ-008 fix 15): login_wall (its page is walled
-- for logged-out readers; stored from the grid's fields, counts NULL) or
-- not_found (gone). NULL while visible.
SET LOCAL search_path TO ig;
ALTER TABLE posts ADD COLUMN status_reason text;

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE posts DROP COLUMN status_reason;
