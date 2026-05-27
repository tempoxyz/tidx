CREATE TABLE IF NOT EXISTS token_approvals (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    token           String,
    owner           String,
    spender         String,
    amount          UInt256,

    INDEX idx_token   token   TYPE bloom_filter GRANULARITY 1,
    INDEX idx_owner   owner   TYPE bloom_filter GRANULARITY 1,
    INDEX idx_spender spender TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (token, block_num, log_idx, tx_hash)
