CREATE TABLE IF NOT EXISTS earn_vaults (
    deployed_block  Int64,
    deployed_at     DateTime64(3, 'UTC'),
    tx_hash         String,
    log_idx         Int32,
    factory         String,
    selector        LowCardinality(String),
    vault           String,
    share_token     String,
    fees            String,
    engine          String,
    asset           String,
    owner           String,

    INDEX idx_share_token share_token TYPE bloom_filter GRANULARITY 1,
    INDEX idx_factory     factory     TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
ORDER BY (vault, deployed_block, log_idx)
SETTINGS default_compression_codec = 'ZSTD(1)'
