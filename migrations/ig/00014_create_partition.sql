-- +goose Up
-- Per-component PostgreSQL roles: only the tables' owner may create a
-- partition, and the persister creates one when a row falls outside the
-- scheduler's window (a post from years ago). This function runs as its
-- owner, the tables' owner, and creates exactly the partition its name
-- describes (posts_y2012m04: April 2012; post_metrics_d20261107: one day,
-- both in UTC), so a role that may call it can add a partition and nothing
-- else. It returns false when the partition already exists.
SET LOCAL search_path TO ig;
-- +goose StatementBegin
CREATE FUNCTION create_partition(parent text, part text, fill integer, lz4 text[])
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ig, pg_temp
SET TimeZone = 'UTC'
AS $fn$
DECLARE
    m   text[];
    lo  timestamptz;
    hi  timestamptz;
    col text;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_partitioned_table p
        JOIN pg_class c ON c.oid = p.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ig' AND c.relname = parent
    ) THEN
        RAISE EXCEPTION 'create_partition: % is not a partitioned table of schema ig', parent;
    END IF;
    IF left(part, length(parent) + 1) <> parent || '_' THEN
        RAISE EXCEPTION 'create_partition: % is not a partition name of %', part, parent;
    END IF;
    m := regexp_match(substr(part, length(parent) + 2), '^(?:y(\d{4})m(\d{2})|d(\d{4})(\d{2})(\d{2}))$');
    IF m IS NULL THEN
        RAISE EXCEPTION 'create_partition: % is not a partition name of %', part, parent;
    END IF;
    IF m[1] IS NOT NULL THEN
        lo := make_timestamptz(m[1]::int, m[2]::int, 1, 0, 0, 0, 'UTC');
        hi := lo + interval '1 month';
    ELSE
        lo := make_timestamptz(m[3]::int, m[4]::int, m[5]::int, 0, 0, 0, 'UTC');
        hi := lo + interval '1 day';
    END IF;
    IF fill IS NOT NULL AND (fill < 10 OR fill > 100) THEN
        RAISE EXCEPTION 'create_partition: fillfactor % is out of range', fill;
    END IF;
    IF to_regclass(format('ig.%I', part)) IS NOT NULL THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TABLE ig.%I PARTITION OF ig.%I FOR VALUES FROM (%L) TO (%L)%s',
        part, parent, lo, hi,
        CASE WHEN fill IS NULL THEN '' ELSE format(' WITH (fillfactor = %s)', fill) END);
    -- lz4 needs a server built with lz4 (official images are); without it
    -- the columns keep the default compression.
    FOREACH col IN ARRAY coalesce(lz4, '{}'::text[]) LOOP
        BEGIN
            EXECUTE format('ALTER TABLE ig.%I ALTER COLUMN %I SET COMPRESSION lz4', part, col);
        EXCEPTION WHEN OTHERS THEN
            RAISE DEBUG 'create_partition: lz4 not applied to %.%: %', part, col, SQLERRM;
        END;
    END LOOP;
    RETURN true;
END
$fn$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION create_partition(text, text, integer, text[]) FROM PUBLIC;

-- +goose Down
SET LOCAL search_path TO ig;
DROP FUNCTION create_partition(text, text, integer, text[]);
