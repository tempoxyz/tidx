-- Crash-safe journal for balance keys touched by an in-flight reorg delete.
--
-- ALTER DELETE does not trigger incremental materialized views. TIDX captures
-- the affected keys and old high-water blocks here before deletion, publishes
-- dirty events after deletion, and then marks the journal entries complete.
CREATE TABLE IF NOT EXISTS token_balance_reorg_keys_v2 (
    from_block Int64,
    token      String,
    holder     String,
    max_block  Int64,
    pending    UInt8,
    version    DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (from_block, token, holder)
SETTINGS default_compression_codec = 'ZSTD(1)'
