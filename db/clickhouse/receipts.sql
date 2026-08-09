CREATE TABLE IF NOT EXISTS receipts (
    block_num               Int64,
    block_timestamp         DateTime64(3, 'UTC'),
    tx_idx                  Int32,
    tx_hash                 String,
    `from`                  String,
    `to`                    Nullable(String),
    contract_address        Nullable(String),
    gas_used                Int64,
    cumulative_gas_used     Int64,
    effective_gas_price     Nullable(String),
    status                  Nullable(Int16),
    fee_payer               Nullable(String),
    `type`                  Nullable(Int16),
    fee_token               Nullable(String),

    INDEX idx_tx_hash   tx_hash   TYPE bloom_filter GRANULARITY 1,
    INDEX idx_from      `from`    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1,
    INDEX idx_contract_address contract_address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_to `to` TYPE bloom_filter(0.01) GRANULARITY 1,

    PROJECTION prj_tx_hash (
        SELECT _part_offset ORDER BY tx_hash
    ),
    PROJECTION prj_fee_payer_position (
        SELECT _part_offset ORDER BY fee_payer, block_num, tx_idx
    )
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, tx_idx)
SETTINGS default_compression_codec = 'ZSTD(1)',
    deduplicate_merge_projection_mode = 'rebuild'
