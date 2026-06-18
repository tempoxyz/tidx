CREATE TABLE IF NOT EXISTS receipt_burned (
    block_num          Int64,
    block_timestamp    DateTime64(3, 'UTC'),
    tx_idx             Int32,
    log_idx            Int32,
    tx_hash            String,
    address            String,
    token              String,
    receiver           String,
    blocked_nonce      UInt64,
    blocked_at         UInt64,
    receipt_version    UInt8,
    originator         String,
    recipient          String,
    recovery_authority String,
    caller             String,
    amount             UInt256,

    INDEX idx_token    token    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_receiver receiver TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash  tx_hash  TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
