-- Pre-aggregated address balances, refreshed on a schedule.
--
-- `address_balances` (the plain VIEW) re-aggregates the full
-- `address_holder_deltas` history on every read. Hot addresses can accumulate
-- tens of millions of delta rows, so account balance pages and counts can hit
-- ClickHouse's query timeout. This refreshable materialized view stores one
-- positive-balance row per (holder, token) in its own MergeTree so address
-- balance reads hit a holder-keyed primary range instead of re-aggregating the
-- event history.
--
-- Each refresh atomically publishes the current incremental balance state in
-- holder order. Dirty pairs are resolved by address_balances from the
-- canonical FINAL ledger, preserving reorg correctness without re-aggregating
-- every historical delta.
--
-- ORDER BY (holder, balance, token) so address balance pages/counts filtered by
-- holder can prune to one account before sorting by balance.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balances_snapshot
REFRESH AFTER 15 MINUTE
ENGINE = MergeTree
ORDER BY (holder, balance, token)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
SELECT holder, token, balance
FROM address_balances
SETTINGS
    max_threads = 8,
    max_memory_usage = 34359738368
