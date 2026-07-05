-- Partition lifecycle state. A partition is "sealed" once it has fully left
-- the hot window: physically reordered by its primary key (CLUSTER), frozen,
-- and recorded here so maintenance never re-seals it. Sealing is advisory —
-- stray writes into a sealed partition (e.g. deferred receipt backfill) are
-- still allowed and merely reduce its clustering slightly.
CREATE TABLE IF NOT EXISTS partition_state (
    table_name    TEXT NOT NULL,
    partition_idx INT8 NOT NULL,
    sealed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name, partition_idx)
);

-- Create any missing RANGE partitions of `parent` covering blocks
-- [from_block, to_block]. Partition k of width `partition_blocks` (from
-- storage_config) covers [k * width, (k + 1) * width).
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

    FOR k IN (from_block / width) .. (to_block / width) LOOP
        part := format('%s_p%s', parent, k);
        IF to_regclass(part) IS NULL THEN
            -- Serialize concurrent creation of the same partition (the
            -- advisory lock releases at transaction end); IF NOT EXISTS
            -- makes the loser a no-op.
            PERFORM pg_advisory_xact_lock(hashtext(part));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                part, parent, k * width, (k + 1) * width
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
