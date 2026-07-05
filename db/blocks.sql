CREATE TABLE IF NOT EXISTS blocks (
    num             INT8 NOT NULL,
    hash            BYTEA NOT NULL,
    parent_hash     BYTEA NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    timestamp_ms    INT8 NOT NULL,
    gas_limit       INT8 NOT NULL,
    gas_used        INT8 NOT NULL,
    miner           BYTEA NOT NULL,
    extra_data      BYTEA,
    consensus_proposer BYTEA CHECK (consensus_proposer IS NULL OR octet_length(consensus_proposer) = 32),
    PRIMARY KEY (timestamp, num)
) PARTITION BY RANGE (num);
-- Fresh installs get the partitioned layout; pre-partitioning deployments
-- keep their regular table (IF NOT EXISTS skips) until the offline rewrite.
-- Partitions are created on demand by ensure_block_partitions (db/partitions.sql).

CREATE INDEX IF NOT EXISTS idx_blocks_num ON blocks (num DESC);
DROP INDEX IF EXISTS idx_blocks_num_asc;
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks (hash);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp);

-- LZ4 TOAST compression for wide values on heap layouts (PG14+;
-- metadata-only, applies to newly written rows). Skipped when orioledb is
-- installed: orioledb tables use their own zstd compression and reject
-- SET COMPRESSION.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'orioledb') THEN
        ALTER TABLE blocks ALTER COLUMN extra_data SET COMPRESSION lz4;
    END IF;
END $$;
