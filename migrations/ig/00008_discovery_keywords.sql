-- +goose Up
-- The shared Iraq place list (M12i): fbs's keyword seed, loaded into an
-- editable table. Discovery matches the enabled terms in candidates' names,
-- bios, links and captions and in post locations. The columns are fbs's
-- discovery_keywords' (less its search times: igs does not search by
-- keyword), so one list can move between the two.
SET LOCAL search_path TO ig;

CREATE TABLE discovery_keywords (
    id          bigserial PRIMARY KEY,
    term        text NOT NULL,
    -- The term folded for matching and de-duplication (keywords.Normalize).
    normalized  text NOT NULL,
    language    text NOT NULL CHECK (language IN ('en', 'ar', 'ckb', 'kmr')),
    -- iraq, kurdistan_region, or a governorate (baghdad, erbil...).
    grp         text NOT NULL DEFAULT '',
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX discovery_keywords_term_idx ON discovery_keywords (normalized, language);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE discovery_keywords;
