-- Durable journal of balance keys touched by an in-flight reorg delete.
--
-- Reorg mutations do not trigger incremental materialized views. TIDX records
-- the affected keys here before deleting block-scoped rows, then republishes
-- them to balance_dirty_keys after the delete. ReplacingMergeTree makes the
-- capture/finalize sequence safely replayable after a crash.
CREATE TABLE IF NOT EXISTS balance_reorg_keys (
    from_block Int64,
    token      String,
    holder     String,
    pending    UInt8,
    version    DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (from_block, token, holder)
SETTINGS default_compression_codec = 'ZSTD(1)'
