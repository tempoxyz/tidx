-- Durable work queue for holder/token pairs whose current balance must be
-- recomputed from the canonical delta ledger.
--
-- The latest row per pair wins. Inserts into token_holder_deltas mark a pair
-- dirty in the same materialized-view chain as the source write. Once
-- balance_state accepts a recomputed version, balance_state_clean_mv writes a
-- clean row with that same version. A newer source write therefore cannot be
-- accidentally cleared by an older refresh.
CREATE TABLE IF NOT EXISTS balance_dirty_keys (
    token   String,
    holder  String,
    dirty   UInt8,
    version DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (token, holder)
SETTINGS default_compression_codec = 'ZSTD(1)'
