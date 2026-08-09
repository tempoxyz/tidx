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
    is_virtual_forward UInt8 DEFAULT 0,

    INDEX idx_block_num block_num TYPE minmax GRANULARITY 1,
    INDEX idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1,
    INDEX idx_selector selector TYPE bloom_filter GRANULARITY 1,
    INDEX idx_address address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic1 topic1 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic2 topic2 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic3 topic3 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_virtual_forward is_virtual_forward TYPE set(2) GRANULARITY 1,
    INDEX idx_selector_topic1 (selector, topic1) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_selector_topic2 (selector, topic2) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_selector_topic3 (selector, topic3) TYPE bloom_filter(0.01) GRANULARITY 1,

    PROJECTION prj_address_position (
        SELECT _part_offset ORDER BY address, block_num, log_idx
    ),
    PROJECTION prj_selector_address_position (
        SELECT _part_offset ORDER BY selector, address, block_num, log_idx
    ),
    PROJECTION prj_selector_topic1_position (
        SELECT _part_offset ORDER BY selector, topic1, block_num, log_idx
    ),
    PROJECTION prj_selector_topic2_position (
        SELECT _part_offset ORDER BY selector, topic2, block_num, log_idx
    ),
    PROJECTION prj_selector_topic3_position (
        SELECT _part_offset ORDER BY selector, topic3, block_num, log_idx
    ),
    PROJECTION prj_tx_hash (
        SELECT _part_offset ORDER BY tx_hash
    )
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, selector, block_num, log_idx)
SETTINGS default_compression_codec = 'ZSTD(1)',
    allow_nullable_key = 1,
    deduplicate_merge_projection_mode = 'rebuild'
