CREATE TABLE IF NOT EXISTS contract_creations (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    tx_hash         String,
    creator         String,
    contract        String,

    INDEX idx_contract contract TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash  tx_hash  TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (creator, block_num, tx_idx)
