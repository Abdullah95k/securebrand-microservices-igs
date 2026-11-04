-- +goose Up
-- Schedule tiers (M12c, architecture §8): hot, warm and long tail. A
-- profile is placed by its posting rate (adaptive) unless pinned to a tier
-- or given its own interval (custom).
SET LOCAL search_path TO ig;

UPDATE profile_schedule SET tier = 'long_tail', adaptive = true WHERE tier = 'T1';
ALTER TABLE profile_schedule ALTER COLUMN tier SET DEFAULT 'long_tail';
ALTER TABLE profile_schedule ALTER COLUMN adaptive SET DEFAULT true;
CREATE INDEX profile_schedule_adaptive_idx ON profile_schedule (tier_changed_at NULLS FIRST) WHERE adaptive;

-- +goose Down
SET LOCAL search_path TO ig;
DROP INDEX profile_schedule_adaptive_idx;
ALTER TABLE profile_schedule ALTER COLUMN adaptive SET DEFAULT false;
ALTER TABLE profile_schedule ALTER COLUMN tier SET DEFAULT 'T1';
UPDATE profile_schedule SET tier = 'T1' WHERE tier IN ('hot', 'warm', 'long_tail', 'custom');
