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
    signature_type          Nullable(Int16)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, idx)
