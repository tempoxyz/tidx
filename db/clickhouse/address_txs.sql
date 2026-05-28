CREATE TABLE IF NOT EXISTS address_txs (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    tx_hash         String,
    address         String,
    direction       LowCardinality(String),
    counterparty    Nullable(String),

    INDEX idx_counterparty counterparty TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash      tx_hash      TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, block_num, tx_idx, direction)
