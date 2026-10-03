-- 1-minute OHLC (candlestick) buckets per trading pair, refreshed on a schedule.
--
-- The OHLC endpoint currently scans up to ~1000 raw `OrderFilled` rows per
-- request, decodes them from `logs`, joins each to its `OrderPlaced`, and
-- buckets in memory — which caps long windows (`truncated: true`) and repeats
-- the join+decode on every call.
--
-- This refreshable materialized view pre-buckets the decoded
-- `dex_fills ⋈ dex_orders` join into 1-minute candles and stores them in its
-- own MergeTree ordered by `(token, bucket)`, so a chart read is a primary-key
-- range scan over candles instead of raw fills. Coarser intervals (5m, 1h, …)
-- re-bucket these 1m candles client-side: high = max(high), low = min(low),
-- open = first bucket's open, close = last bucket's close, volumes summed.
--
-- Why refreshable (full recompute) rather than an insert-time
-- AggregatingMergeTree: candles are keyed by `(token, bucket)` with no
-- `block_num`, so block-scoped reorg cleanup can't prune them and an
-- incremental aggregate would double-count reorged fills. A refreshable MV
-- recomputes and atomically swaps, so it is reorg-correct by construction —
-- the same reasoning as `token_balances_snapshot`.
--
-- Table size and refresh memory are bounded by the rolling retention window
-- below: the join only builds on orders that were filled inside it, not on
-- every order ever placed. Long historical OHLC ranges fall back to scanning
-- `dex_fills` directly. Tune `REFRESH EVERY`, the retention window, and the
-- memory and spill settings to the deployment's fill volume.
--
-- Price math mirrors the API exactly: the DEX prices `1 base =
-- (priceScale + tick) / priceScale quote` with `priceScale = 100000`. The
-- per-fill rate is `(priceScale + tick) / priceScale` for bids and its inverse
-- for asks; the quote amount is `intDiv(amountFilled * (priceScale + tick),
-- priceScale)`.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time;
-- the sink sets it when applying this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS dex_ohlc_1m
REFRESH EVERY 1 MINUTE
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (token, bucket)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
SELECT
    toStartOfInterval(block_timestamp, INTERVAL 1 MINUTE) AS bucket,
    token,
    argMin(rate, (block_num, log_idx)) AS open,
    max(rate) AS high,
    min(rate) AS low,
    argMax(rate, (block_num, log_idx)) AS close,
    sum(amountFilled) AS base_volume,
    sum(quote_amount) AS quote_volume,
    count() AS fill_count
FROM
(
    SELECT
        f.block_num AS block_num,
        f.log_idx AS log_idx,
        f.block_timestamp AS block_timestamp,
        o.token AS token,
        f.amountFilled AS amountFilled,
        intDiv(f.amountFilled * toUInt256(toInt64(100000) + o.tick), 100000) AS quote_amount,
        if(
            o.isBid = 1,
            (toFloat64(100000) + o.tick) / 100000,
            toFloat64(100000) / (toFloat64(100000) + o.tick)
        ) AS rate
    FROM dex_fills AS f
    INNER JOIN
    (
        -- `dex_orders` keeps every order ever placed and the join loads its
        -- right side into memory, so restrict it to orders with a fill in the
        -- window. The lookback is a day wider than the fill window so that
        -- every windowed fill still finds its order if the two `now()` calls
        -- differ.
        SELECT orderId, token, isBid, tick
        FROM dex_orders
        WHERE orderId IN
        (
            SELECT orderId
            FROM dex_fills
            WHERE block_timestamp >= now() - INTERVAL 31 DAY
        )
    ) AS o ON o.orderId = f.orderId
    WHERE f.block_timestamp >= now() - INTERVAL 30 DAY
)
GROUP BY token, bucket
SETTINGS
    max_threads = 4,
    max_memory_usage = 8589934592,
    max_bytes_before_external_group_by = 2000000000
