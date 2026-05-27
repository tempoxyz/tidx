CREATE TABLE IF NOT EXISTS token_holder_deltas (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    token           String,
    holder          String,
    balance_delta   Int256,

    INDEX idx_token token TYPE bloom_filter GRANULARITY 1,
    INDEX idx_holder holder TYPE bloom_filter GRANULARITY 1
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (token, holder, block_num)
