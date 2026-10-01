CREATE TABLE IF NOT EXISTS dex_order_events (
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
    eventType       String,

    INDEX idx_order_id orderId TYPE bloom_filter GRANULARITY 1,
    INDEX idx_maker    maker   TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (orderId, block_num, log_idx)
-- Point-in-time order-state stream: the union of `OrderPlaced` and
-- `OrderFlipped` events decoded from the raw `logs` stream, one positioned
-- `(orderId, block_num, log_idx) -> (token, isBid, tick)` row per state change.
--
-- The decoded `dex_orders` table captures `OrderPlaced` only, so it is stale
-- for flip orders: a T5+ flip order keeps its `orderId` but mutates
-- `(isBid, tick)` on every fill and emits only `OrderFlipped` (no second
-- `OrderPlaced`). Resolving a fill's true price therefore requires the latest
-- state event *before* the fill, which this table makes available as a plain
-- ASOF join (see `dex_fills_enriched`) instead of an app-side replay of `logs`.
