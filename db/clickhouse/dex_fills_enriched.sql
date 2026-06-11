-- Point-in-time enriched `OrderFilled` stream, refreshed on a schedule.
--
-- `OrderFilled` carries no `(token, isBid, tick)` — those are properties of the
-- resting order at the moment of the fill, and for flip orders they change over
-- the order's life (see `dex_order_events`). Consumers (the swaps feed, OHLC,
-- at-peg / token filters) therefore had to replay the raw `logs` order-state
-- stream per request and re-derive each fill's price and orientation in memory.
--
-- This view resolves that state once, server-side: each fill is ASOF-joined to
-- the latest `dex_order_events` row strictly before its `(block_num, log_idx)`
-- position, exactly mirroring the app's "latest state event before the fill"
-- rule. It then materializes, per fill:
--   * `token` / `isBid` / `tick` — the order state at fill time (flip-correct)
--   * `at_peg`   — 1 iff the fill executed at tick 0 (rate == 1)
--   * `price`    — quote-per-base implied by the tick: (priceScale + tick) /
--                  priceScale, side-independent (priceScale = 100000)
--   * `quote_amount` — the quote-side amount: intDiv(amountFilled *
--                  (priceScale + tick), priceScale)
--   * taker-oriented `source_*` / `destination_*` — the token the taker sent
--                  and received in this fill. For a maker bid (taker sells base)
--                  source = base, destination = quote; for an ask, reversed.
--                  These let a swap's route ends and at-peg status be computed
--                  as plain SQL aggregates over a `(block_num, tx_hash, taker)`
--                  group instead of an app-side chain assembly.
--
-- Quote resolution joins each book base to its `dex_pairs` row. Genesis system
-- books predate the indexer's `PairCreated` decode and have no `dex_pairs` row,
-- so their `quote_token` (and the side that references it) resolves to '' —
-- best-effort, matching that those books are absent from `dex_pairs`.
--
-- Why refreshable (full recompute) rather than an insert-time MV: the ASOF join
-- must see the complete order-events stream, but insert-time MV cascade order
-- between sibling views on a single block is not guaranteed, so a flip emitted
-- in the same block as a later fill could be missed. A periodic full recompute
-- always sees a consistent snapshot and, having no incremental state, is
-- reorg-correct by construction (the same reasoning as `dex_ohlc_1m` /
-- `token_balances_snapshot`). Recompute cost and table size are bounded by the
-- rolling retention window below; older fills fall back to scanning `dex_fills`
-- directly. `FINAL` collapses the `ReplacingMergeTree` sources so duplicate
-- pre-merge parts can't double-count.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time;
-- the sink sets it when applying this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS dex_fills_enriched
REFRESH EVERY 1 MINUTE
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_num, log_idx)
AS
SELECT
    f.block_num AS block_num,
    f.block_timestamp AS block_timestamp,
    f.tx_idx AS tx_idx,
    f.log_idx AS log_idx,
    f.tx_hash AS tx_hash,
    f.orderId AS orderId,
    f.maker AS maker,
    f.taker AS taker,
    f.amountFilled AS amountFilled,
    f.partialFill AS partialFill,
    e.token AS token,
    e.isBid AS isBid,
    e.tick AS tick,
    toUInt8(e.tick = 0) AS at_peg,
    (toFloat64(100000) + e.tick) / 100000 AS price,
    intDiv(f.amountFilled * toUInt256(toInt64(100000) + e.tick), 100000) AS quote_amount,
    p.quote AS quote_token,
    if(e.isBid = 1, e.token, p.quote) AS source_token,
    if(
        e.isBid = 1,
        f.amountFilled,
        intDiv(f.amountFilled * toUInt256(toInt64(100000) + e.tick), 100000)
    ) AS source_amount,
    if(e.isBid = 1, p.quote, e.token) AS destination_token,
    if(
        e.isBid = 1,
        intDiv(f.amountFilled * toUInt256(toInt64(100000) + e.tick), 100000),
        f.amountFilled
    ) AS destination_amount
FROM
(
    SELECT
        block_num,
        block_timestamp,
        tx_idx,
        log_idx,
        tx_hash,
        orderId,
        maker,
        taker,
        amountFilled,
        partialFill,
        toUInt64(block_num) * 4294967296 + toUInt64(log_idx) AS pos
    FROM dex_fills FINAL
    WHERE block_timestamp >= now() - INTERVAL 30 DAY
) AS f
ASOF INNER JOIN
(
    SELECT
        orderId,
        token,
        isBid,
        tick,
        toUInt64(block_num) * 4294967296 + toUInt64(log_idx) AS pos
    FROM dex_order_events FINAL
) AS e
    ON f.orderId = e.orderId AND f.pos > e.pos
LEFT JOIN dex_pairs AS p FINAL ON p.base = e.token
SETTINGS join_use_nulls = 0
