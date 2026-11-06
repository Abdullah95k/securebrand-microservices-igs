-- +goose Up
-- REQ-008 fix 9: the linked Facebook page's ID, beside its name.
SET LOCAL search_path TO ig;
ALTER TABLE profiles ADD COLUMN linked_facebook_page_id text;

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE profiles DROP COLUMN linked_facebook_page_id;
