-- +goose Up
-- Discovery pages (D-055): the hashtag and location pages discovery loads
-- logged out to meet accounts, with when each is next due. The scheduler
-- adds pages and dispatches the due ones (ig.discovery.pages); the crawler
-- records each load's result. Counts only: no post or account is kept here.
SET LOCAL search_path TO ig;
CREATE TABLE discovery_pages (
    kind           text NOT NULL,            -- location | hashtag
    key            text NOT NULL,            -- a location id, or a hashtag without #
    added_at       timestamptz NOT NULL DEFAULT now(),
    due_at         timestamptz NOT NULL DEFAULT now(),
    dispatched_at  timestamptz,
    loaded_at      timestamptz,
    loads          int NOT NULL DEFAULT 0,
    -- Loads in a row that met no account new to discovery: each doubles
    -- the wait before the next load (capped).
    empty_runs     int NOT NULL DEFAULT 0,
    last_class     text,
    last_accounts  int,
    last_new       int,
    PRIMARY KEY (kind, key)
);
CREATE INDEX discovery_pages_due_idx ON discovery_pages (due_at);
CREATE INDEX discovery_pages_dispatched_idx ON discovery_pages (dispatched_at);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE discovery_pages;
