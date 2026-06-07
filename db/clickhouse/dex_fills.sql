CREATE TABLE IF NOT EXISTS dex_fills (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    orderId         UInt256,
    maker           String,
    taker           String,
    amountFilled    UInt256,
    partialFill     UInt8,

    INDEX idx_maker    maker    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_taker    taker    TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tx_hash  tx_hash  TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (orderId, block_num, log_idx)
