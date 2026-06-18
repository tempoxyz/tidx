CREATE TABLE IF NOT EXISTS transfer_blocked (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    token           String,
    receiver        String,
    blocked_nonce   UInt64,
    amount          UInt256,
    receipt_version UInt8,
    receipt         String,

    INDEX idx_token    token    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_receiver receiver TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash  tx_hash  TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
