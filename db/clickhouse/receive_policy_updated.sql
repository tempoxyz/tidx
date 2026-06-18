CREATE TABLE IF NOT EXISTS receive_policy_updated (
    block_num          Int64,
    block_timestamp    DateTime64(3, 'UTC'),
    tx_idx             Int32,
    log_idx            Int32,
    tx_hash            String,
    address            String,
    account            String,
    sender_policy_id   UInt64,
    token_filter_id    UInt64,
    recovery_authority String,

    INDEX idx_account account TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
