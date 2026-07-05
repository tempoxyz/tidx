-- Partition lifecycle state. A partition is "sealed" once it has fully left
-- the hot window: physically reordered by its primary key (CLUSTER), frozen,
-- and recorded here so maintenance never re-seals it. Sealing is advisory —
-- stray writes into a sealed partition (e.g. deferred receipt backfill) are
-- still allowed and merely reduce its clustering slightly.
-- Metadata stays heap: tiny, and independent of the database locale (the
-- orioledb AM only allows C/POSIX/ICU collations on indexed text columns).
CREATE TABLE IF NOT EXISTS partition_state (
    table_name    TEXT NOT NULL,
    partition_idx INT8 NOT NULL,
    sealed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name, partition_idx)
) USING heap;

-- Create any missing RANGE partitions of `parent` covering blocks
-- [from_block, to_block]. Partition k of width `partition_blocks` (from
-- storage_config) covers [k * width, (k + 1) * width).
--
-- When the orioledb extension is installed, new partitions use the orioledb
-- access method with zstd page-level compression (indexes included;
-- primary_compress inherits `compress`). Otherwise they are plain heap.
-- Mixed layouts are fine: each partition has its own access method.
--
-- No-op when `parent` is not a partitioned table, so pre-partitioning
-- deployments (regular tables) keep working unchanged until they are
-- rewritten to the partitioned layout.
CREATE OR REPLACE FUNCTION ensure_block_partitions(parent TEXT, from_block INT8, to_block INT8)
RETURNS VOID AS $$
DECLARE
    width INT8;
    k INT8;
    part TEXT;
    use_orioledb BOOLEAN;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = parent AND n.nspname = current_schema()
    ) THEN
        RETURN;
    END IF;

    SELECT partition_blocks INTO width FROM storage_config;
    IF width IS NULL THEN
        RAISE EXCEPTION 'storage_config.partition_blocks is not set';
    END IF;

    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'orioledb')
    INTO use_orioledb;

    FOR k IN (from_block / width) .. (to_block / width) LOOP
        part := format('%s_p%s', parent, k);
        IF to_regclass(part) IS NULL THEN
            -- Serialize concurrent creation of the same partition (the
            -- advisory lock releases at transaction end); IF NOT EXISTS
            -- makes the loser a no-op.
            PERFORM pg_advisory_xact_lock(hashtext(part));
            IF use_orioledb THEN
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)
                     USING orioledb WITH (compress = 6, toast_compress = 12)',
                    part, parent, k * width, (k + 1) * width
                );
            ELSE
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)
                     USING heap',
                    part, parent, k * width, (k + 1) * width
                );
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
