-- Cumulative credit/debit checkpoints for each token/holder pair.
--
-- Keeping the two unsigned totals separately preserves the canonical
-- max(credits - debits, 0) semantics while allowing normal updates to add only
-- the post-checkpoint ledger range. Replayed, backfilled, or reorged ranges
-- publish a newer absolute checkpoint instead.
CREATE TABLE IF NOT EXISTS token_balance_checkpoints (
    token            String,
    holder           String,
    credited         UInt256,
    debited          UInt256,
    checkpoint_from_block Int64,
    checkpoint_block Int64,
    dirty_events     Array(Tuple(UUID, Int64, Int64)),
    version          DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (token, holder)
SETTINGS default_compression_codec = 'ZSTD(1)'
