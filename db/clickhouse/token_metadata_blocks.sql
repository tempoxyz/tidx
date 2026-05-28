CREATE TABLE IF NOT EXISTS token_metadata_blocks (
    token           String,
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    transfer_count  UInt64
) ENGINE = SummingMergeTree(transfer_count)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (token, block_num)
