-- +goose Up
-- Profile discovery (M11): accounts the crawl meets in the profiles it
-- reads (tagged, co-authors, mentioned, commenters), vetted with one
-- profile read for Iraqi signals, and registered for the schedule above
-- ig.discovery.threshold.
SET LOCAL search_path TO ig;

CREATE TABLE discovery_candidates (
    username      text PRIMARY KEY,                 -- lower case
    user_pk       bigint,
    -- Sightings, by how the account appeared.
    tags          int NOT NULL DEFAULT 0,
    coauthors     int NOT NULL DEFAULT 0,
    mentions      int NOT NULL DEFAULT 0,
    comments      int NOT NULL DEFAULT 0,
    -- Sightings on posts located in Iraq.
    iraq_context  int NOT NULL DEFAULT 0,
    -- Weighted sightings: the vetting queue's order.
    priority      double precision NOT NULL DEFAULT 0,
    -- new | vetting | registered | below | private | gone | failed | rejected
    status        text NOT NULL DEFAULT 'new',
    attempts      int NOT NULL DEFAULT 0,
    vet_job_id    uuid,
    -- What the vet read (profile and recent captions); cleared once scored.
    vet           jsonb,
    score         double precision,
    signals       jsonb,                            -- the score's parts
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    vetted_at     timestamptz,
    decided_at    timestamptz
);
CREATE INDEX discovery_candidates_queue_idx ON discovery_candidates (priority DESC) WHERE status = 'new';
CREATE INDEX discovery_candidates_status_idx ON discovery_candidates (status, decided_at);
CREATE INDEX discovery_candidates_pk_idx ON discovery_candidates (user_pk);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE discovery_candidates;
