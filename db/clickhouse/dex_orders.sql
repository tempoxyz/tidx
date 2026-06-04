CREATE TABLE IF NOT EXISTS dex_orders (
    block_num       Int64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_idx          Int32,
    log_idx         Int32,
    tx_hash         String,
    address         String,
    orderId         UInt256,
    maker           String,
    token           String,
    amount          UInt256,
    isBid           UInt8,
    tick            Int16,
    isFlipOrder     UInt8,
    flipTick        Int16,

    INDEX idx_order_id orderId TYPE bloom_filter GRANULARITY 1,
    INDEX idx_maker    maker   TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (token, orderId)
