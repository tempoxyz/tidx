CREATE TABLE IF NOT EXISTS earn_flows (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash         String,
    log_idx         Int32,
    vault           String,
    account         String,
    caller          String,
    request_id      String,
    kind            LowCardinality(String),
    assets          Int256,
    shares          Int256,

    INDEX idx_account    account    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_caller     caller     TYPE bloom_filter GRANULARITY 1,
    INDEX idx_request_id request_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash    tx_hash    TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (vault, account, block_num, log_idx)
SETTINGS default_compression_codec = 'ZSTD(1)'
