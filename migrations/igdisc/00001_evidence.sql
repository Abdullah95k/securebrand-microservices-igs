-- +goose Up
-- Instagram keyword discovery, Track 1 (docs/master/igdisc-plan.md §3.1):
-- an account met in a recent post or comment that names a keyword of the
-- shared list, on a registered account's post. One row per account,
-- keyword and piece of content. Kept forever (owner D-004); erasure
-- deletes a person's rows.
SET LOCAL search_path TO ig;

CREATE TABLE igdisc_evidence (
    -- The account met, lower case, and its user pk when the post gave it
    -- (tags, co-authors and commenters do; mentions don't).
    username      text NOT NULL,
    user_pk       bigint,
    -- The keyword, as the shared list stores it: its contract ID
    -- (contracts.KeywordID: "<group>:<language>:<folded spelling>"), its
    -- spelling and folded spelling, language and group.
    keyword_id    text NOT NULL,
    keyword       text NOT NULL,
    normalized    text NOT NULL,
    language      text NOT NULL,
    grp           text NOT NULL,
    -- The content that named it: ig:post:<media pk> or ig:comment:<comment
    -- pk>, its link when known, and when it was made.
    content_id    text NOT NULL,
    url           text NOT NULL DEFAULT '',
    content_time  timestamptz NOT NULL,
    -- How the account was met near it: the caption of a post it is tagged,
    -- mentioned or co-authoring in; a comment it wrote; or the place of a
    -- post it is on.
    via           text NOT NULL CHECK (via IN ('caption', 'comment', 'post_place')),
    -- The registered account whose post it was.
    source_pk     bigint NOT NULL,
    found_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (username, keyword_id, content_id)
);
CREATE INDEX igdisc_evidence_user_pk_idx ON igdisc_evidence (user_pk) WHERE user_pk IS NOT NULL;
CREATE INDEX igdisc_evidence_source_idx ON igdisc_evidence (source_pk);
CREATE INDEX igdisc_evidence_found_idx ON igdisc_evidence (found_at);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE igdisc_evidence;
