CREATE TABLE IF NOT EXISTS txs (
    block_num               Int64,
    block_timestamp         DateTime64(3, 'UTC'),
    idx                     Int32,
    hash                    String,
    `type`                  Int16,
    `from`                  String,
    `to`                    Nullable(String),
    value                   String,
    input                   String,
    gas_limit               Int64,
    max_fee_per_gas         String,
    max_priority_fee_per_gas String,
    gas_used                Nullable(Int64),
    nonce_key               String,
    nonce                   Int64,
    fee_token               Nullable(String),
    fee_payer               Nullable(String),
    calls                   Nullable(String),
    call_count              Int16,
    valid_before            Nullable(Int64),
    valid_after             Nullable(Int64),
    signature_type          Nullable(Int16),

    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 1,
    INDEX idx_from `from` TYPE bloom_filter GRANULARITY 1,
    INDEX idx_to   `to`   TYPE bloom_filter GRANULARITY 1,
    INDEX idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1,
    INDEX idx_fee_token fee_token TYPE bloom_filter GRANULARITY 1,
    INDEX idx_from_nonce_key_nonce (`from`, nonce_key, nonce)
        TYPE bloom_filter(0.01) GRANULARITY 1,

    PROJECTION prj_from_position (
        SELECT _part_offset ORDER BY `from`, block_num, idx
    ),
    PROJECTION prj_to_position (
        SELECT _part_offset ORDER BY `to`, block_num, idx
    ),
    PROJECTION prj_fee_payer_position (
        SELECT _part_offset ORDER BY fee_payer, block_num, idx
    ),
    PROJECTION prj_fee_token_position (
        SELECT _part_offset ORDER BY fee_token, block_num, idx
    )
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, idx)
SETTINGS default_compression_codec = 'ZSTD(1)',
    deduplicate_merge_projection_mode = 'rebuild'
