CREATE TABLE IF NOT EXISTS address_transfers (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    direction       LowCardinality(String),
    counterparty    String,
    token           String,
    amount          UInt256,

    INDEX idx_token        token        TYPE bloom_filter GRANULARITY 1,
    INDEX idx_counterparty counterparty TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash      tx_hash      TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, block_num, log_idx, tx_hash, direction)
