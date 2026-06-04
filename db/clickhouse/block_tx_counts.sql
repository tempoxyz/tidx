CREATE TABLE IF NOT EXISTS block_tx_counts (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_count        UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num)
