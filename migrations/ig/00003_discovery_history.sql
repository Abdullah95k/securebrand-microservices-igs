-- +goose Up
-- Every vet of a discovery candidate, kept for good (the owner, 2026-11-03:
-- keep data forever): what the read found, the score and its parts, and
-- the decision. An erasure request still deletes the person's rows.
SET LOCAL search_path TO ig;

CREATE TABLE discovery_vets (
    username   text NOT NULL,
    vetted_at  timestamptz NOT NULL,
    user_pk    bigint,
    status     text NOT NULL,           -- registered | below | private | gone
    score      double precision,
    signals    jsonb,
    sightings  jsonb NOT NULL,          -- tags, coauthors, mentions, comments, iraq_context at the time
    vet        jsonb NOT NULL,          -- what the read found
    PRIMARY KEY (username, vetted_at)
);
CREATE INDEX discovery_vets_pk_idx ON discovery_vets (user_pk);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE discovery_vets;
