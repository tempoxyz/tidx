CREATE TABLE IF NOT EXISTS earn_share_prices (
    vault            String,
    bucket           DateTime64(3, 'UTC'),
    block_num        Int64,
    block_hash       String,
    block_timestamp  DateTime64(3, 'UTC'),
    quoted_shares    UInt256,
    quoted_assets    UInt256,

    INDEX idx_block_num block_num TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(block_num)
PARTITION BY toYYYYMM(bucket)
ORDER BY (vault, bucket)
SETTINGS default_compression_codec = 'ZSTD(1)'
