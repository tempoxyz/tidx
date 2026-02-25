CREATE TABLE IF NOT EXISTS logs (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    log_idx         Int32,
    tx_idx          Int32,
    tx_hash         String,
    address         String,
    selector        String DEFAULT '',
    topic0          Nullable(String),
    topic1          Nullable(String),
    topic2          Nullable(String),
    topic3          Nullable(String),
    data            String,

    INDEX idx_selector selector TYPE bloom_filter GRANULARITY 1,
    INDEX idx_address address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic1 topic1 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic2 topic2 TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, selector, block_num, log_idx)
