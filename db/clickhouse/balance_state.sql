-- Versioned current balance per token/holder pair.
--
-- Rows are absolute balances recomputed from token_holder_deltas FINAL, not
-- additive updates. That makes retries idempotent and lets reorg repair publish
-- a newer corrected value (or tombstone) without relying on mutations being
-- observed by an incremental materialized view.
CREATE TABLE IF NOT EXISTS balance_state (
    token      String,
    holder     String,
    balance    UInt256,
    is_deleted UInt8,
    version    DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (token, holder)
SETTINGS default_compression_codec = 'ZSTD(1)'
