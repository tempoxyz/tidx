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
    INDEX idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, tx_idx)
