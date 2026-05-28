CREATE TABLE IF NOT EXISTS address_holder_deltas (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash         String,
    log_idx         Int32,
    holder          String,
    token           String,
    leg             Int8,
    balance_delta   Int256,

    INDEX idx_token  token  TYPE bloom_filter GRANULARITY 1,
    INDEX idx_holder holder TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (holder, token, block_num, tx_hash, log_idx, leg)
