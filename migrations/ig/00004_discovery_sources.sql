-- +goose Up
-- The registered accounts each discovery candidate was met in (M11c): how
-- many different ones tag, mention, co-author with or hear from it is its
-- Iraqi audience, the score's network evidence.
SET LOCAL search_path TO ig;

CREATE TABLE discovery_sources (
    username       text NOT NULL,
    source_pk      bigint NOT NULL,      -- the registered profile's user pk
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (username, source_pk)
);
ALTER TABLE discovery_candidates
    ADD COLUMN sources int NOT NULL DEFAULT 0,             -- distinct rows in discovery_sources
    ADD COLUMN sources_at_decision int NOT NULL DEFAULT 0;  -- sources when last decided

-- +goose Down
SET LOCAL search_path TO ig;
ALTER TABLE discovery_candidates DROP COLUMN sources, DROP COLUMN sources_at_decision;
DROP TABLE discovery_sources;
