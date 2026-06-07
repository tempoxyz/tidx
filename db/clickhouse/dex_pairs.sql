CREATE TABLE IF NOT EXISTS dex_pairs (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    `key`           String,
    base            String,
    quote           String,

    INDEX idx_base  base  TYPE bloom_filter GRANULARITY 1,
    INDEX idx_quote quote TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
