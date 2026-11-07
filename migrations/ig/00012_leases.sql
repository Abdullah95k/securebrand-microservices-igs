-- +goose Up
-- Leases (internal/platform/leader): which process leads (the scheduler)
-- or runs alone (registry-sync), taken and renewed one statement at a
-- time, so they work through a transaction-mode connection pool, where
-- session advisory locks do not.
SET LOCAL search_path TO ig;
CREATE TABLE leases (
    name    text PRIMARY KEY,
    holder  text NOT NULL,
    until   timestamptz NOT NULL
);

-- +goose Down
SET LOCAL search_path TO ig;
DROP TABLE leases;
