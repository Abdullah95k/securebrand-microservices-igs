-- +goose Up
-- Where each discovery candidate came from (M11d): network (met in the
-- posts igs reads), or an intake (google, facebook, youtube, manual...).
SET LOCAL search_path TO ig;

ALTER TABLE discovery_candidates ADD COLUMN origins text[] NOT NULL DEFAULT '{}';
UPDATE discovery_candidates SET origins = '{network}' WHERE tags + coauthors + mentions + comments > 0;

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE discovery_candidates DROP COLUMN origins;
