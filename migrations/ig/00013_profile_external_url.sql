-- +goose Up
-- REQ-008 fix 18: the profile's website when served as external_url.
SET LOCAL search_path TO ig;
ALTER TABLE profiles ADD COLUMN external_url text;

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE profiles DROP COLUMN external_url;
