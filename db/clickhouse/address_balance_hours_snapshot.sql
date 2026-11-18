-- Hourly net balance changes per (holder, token), refreshed on a schedule.
--
-- Stores sum(delta) per UTC hour from the deduplicated address_holder_deltas
-- ledger for every hour that ended at least one hour before the newest
-- indexed delta. Newer hours come from the live tail of the
-- address_balance_hours view, so reorg pruning of recent blocks never leaves
-- a stale row here, and derived-table repair of older ranges is picked up by
-- the next refresh.
--
-- The row with holder = '' and token = '' is a sentinel whose hour is this
-- generation's (exclusive) cutoff. The view reads it with a primary-key seek
-- to know where the snapshot ends and the live tail begins.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time;
-- the sink sets it when applying this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balance_hours_snapshot
REFRESH EVERY 30 MINUTE
ENGINE = MergeTree
ORDER BY (holder, token, hour)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
WITH toDateTime(ifNull((
    SELECT greatest(
        toStartOfHour(toDateTime(max(block_timestamp), 'UTC')),
        toDateTime(3600, 'UTC')
    ) - INTERVAL 1 HOUR
    FROM address_holder_deltas
), toDateTime(0, 'UTC')), 'UTC') AS cutoff
SELECT
    holder,
    token,
    toStartOfHour(toDateTime(block_timestamp, 'UTC')) AS hour,
    toInt256(sumIf(balance_delta, leg = 1)) - toInt256(sumIf(balance_delta, leg = -1)) AS delta
FROM address_holder_deltas FINAL
WHERE block_timestamp < cutoff
GROUP BY holder, token, hour
UNION ALL
SELECT
    '' AS holder,
    '' AS token,
    cutoff AS hour,
    toInt256(0) AS delta
SETTINGS
    optimize_aggregation_in_order = 1,
    max_threads = 4,
    max_memory_usage = 17179869184,
    max_bytes_before_external_group_by = 2000000000
