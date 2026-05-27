CREATE TABLE IF NOT EXISTS token_transfers (
    block_num          Int64,
    block_timestamp    DateTime64(3, 'UTC'),
    tx_idx             Int32,
    log_idx            Int32,
    tx_hash            String,
    token              String,
    `from`             String,
    `to`               String,
    amount             UInt256,
    is_virtual_forward UInt8 DEFAULT 0,

    INDEX idx_token token TYPE bloom_filter GRANULARITY 1,
    INDEX idx_from `from` TYPE bloom_filter GRANULARITY 1,
    INDEX idx_to `to` TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1,
    INDEX idx_virtual_forward is_virtual_forward TYPE set(2) GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (token, block_num, log_idx, tx_hash)
