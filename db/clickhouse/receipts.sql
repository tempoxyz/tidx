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
    fee_payer               Nullable(String)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, tx_idx)
