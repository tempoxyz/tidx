CREATE TABLE IF NOT EXISTS blocks (
    num             Int64,
    hash            String,
    parent_hash     String,
    timestamp       DateTime64(3, 'UTC'),
    timestamp_ms    Int64,
    gas_limit       Int64,
    gas_used        Int64,
    miner           String,
    extra_data      Nullable(String)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (num)
