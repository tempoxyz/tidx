CREATE TABLE IF NOT EXISTS admin_key_authorized (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    account         String,
    public_key      String,

    INDEX idx_account    account    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_public_key public_key TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash    tx_hash    TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
