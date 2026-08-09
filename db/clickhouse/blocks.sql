CREATE TABLE IF NOT EXISTS blocks (
    num             Int64,
    hash            String,
    parent_hash     String,
    timestamp       DateTime64(3, 'UTC'),
    timestamp_ms    Int64,
    gas_limit       Int64,
    gas_used        Int64,
    miner           String,
    extra_data      Nullable(String),
    consensus_proposer Nullable(String),

    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 1,

    PROJECTION prj_hash INDEX hash TYPE basic,
    PROJECTION prj_timestamp_position INDEX (timestamp, num) TYPE basic
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (num)
SETTINGS default_compression_codec = 'ZSTD(1)',
    deduplicate_merge_projection_mode = 'rebuild'
