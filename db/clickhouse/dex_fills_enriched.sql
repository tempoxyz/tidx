-- Point-in-time enriched `OrderFilled` stream, resolved at query time.
--
-- `OrderFilled` carries no `(token, isBid, tick)` — those are properties of the
-- resting order at the moment of the fill, and for flip orders they change over
-- the order's life (see `dex_order_events`). Consumers (the swaps feed, at-peg /
-- token filters) otherwise had to replay the raw `logs` order-state stream per
-- request and re-derive each fill's price and orientation in memory.
--
-- This view resolves that state in ClickHouse: each fill is ASOF-joined to the
-- latest `dex_order_events` row strictly before its `(block_num, log_idx)`
-- position, exactly mirroring the app's "latest state event before the fill"
-- rule. It then exposes, per fill:
--   * `token` / `isBid` / `tick` — the order state at fill time (flip-correct)
--   * `at_peg`   — 1 iff the fill executed at tick 0 (rate == 1)
--   * `price`    — quote-per-base implied by the tick: (priceScale + tick) /
--                  priceScale, side-independent (priceScale = 100000)
--   * `quote_amount` — the quote-side amount: intDiv(amountFilled *
--                  (priceScale + tick), priceScale)
--   * taker-oriented `source_*` / `destination_*` — the token the taker sent
--                  and received in this fill. For a maker bid (taker sells base)
--                  source = base, destination = quote; for an ask, reversed.
--                  These let a swap's route ends and at-peg status be expressed
--                  as plain SQL predicates instead of an app-side chain assembly.
--
-- Quote resolution joins each book base to its `dex_pairs` row. Genesis system
-- books predate the indexer's `PairCreated` decode and have no `dex_pairs` row,
-- so their `quote_token` (and the side that references it) resolves to '' —
-- best-effort, matching that those books are absent from `dex_pairs`.
--
-- Why a plain view (query-time join) rather than a materialized view:
--   * Realtime. A fill is filterable the instant it lands; there is no refresh
--     lag and no rolling-window cutoff, because the view reads the live tables.
--   * Correct. The join sees the complete, ordered, reorg-corrected state at
--     query time, so same-block flips and late/out-of-order events resolve
--     right. An insert-time MV can't: sibling MVs (`dex_fills_mv`,
--     `dex_order_events_mv`) off one `logs` insert have no guaranteed execution
--     order, so a flip emitted in the same block as a later fill could be
--     joined before it exists. A refreshable MV is correct but only as fresh as
--     its last recompute.
--   * Cheap enough. The heavy raw-log decode is already materialized and
--     sort-keyed in `dex_order_events` (by `orderId`) and `dex_fills`, so the
--     per-query cost is an ASOF join over decoded, indexed rows — far below the
--     app's current per-request raw-`logs` replay. Callers scope cost with the
--     usual time / token / pagination predicates, applied in SQL before LIMIT.
--
-- `FINAL` collapses the `ReplacingMergeTree` sources so reorg-duplicated
-- pre-merge parts can't double-count.
CREATE VIEW IF NOT EXISTS dex_fills_enriched AS
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
