-- +goose Up
-- Facebook links (D-065): the facebook.com links on monitored accounts'
-- profiles, each sent once to the Facebook collector's candidate API, and
-- again only when that account's bio changes. The scheduler's leader fills
-- and sends them (ig.fb_candidates).
SET LOCAL search_path TO ig;
CREATE TABLE fb_candidates (
    key       text PRIMARY KEY,         -- the link, normalised (lower-case host without www, no query or trailing /)
    ref       text NOT NULL,            -- the link exactly as found
    user_pk   bigint NOT NULL,          -- the monitored account it was first found on
    bio_hash  text NOT NULL,            -- that account's bio, links and website when last seen
    found_at  timestamptz NOT NULL DEFAULT now(),
    pending   boolean NOT NULL DEFAULT true,
    sent_at   timestamptz,
    sends     int NOT NULL DEFAULT 0,
    result    text                      -- fbs's answer, or invalid
);
CREATE INDEX fb_candidates_pending_idx ON fb_candidates (found_at) WHERE pending;
CREATE INDEX fb_candidates_sent_idx ON fb_candidates (sent_at);
CREATE INDEX fb_candidates_user_idx ON fb_candidates (user_pk);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE fb_candidates;
