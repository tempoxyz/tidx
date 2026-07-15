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
-- The canonical aggregation is performed once by token_balances_snapshot.
-- This dependent refresh only reorders that same atomic generation for the
-- address access pattern, so token- and address-keyed snapshots cannot derive
-- different balances from duplicate or reorged source rows.
--
-- ORDER BY (holder, balance, token) so address balance pages/counts filtered by
-- holder can prune to one account before sorting by balance.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balances_snapshot
REFRESH EVERY 15 MINUTE DEPENDS ON token_balances_snapshot
ENGINE = MergeTree
ORDER BY (holder, balance, token)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
SELECT
    holder,
    token,
    balance
FROM token_balances_snapshot
SETTINGS
    max_threads = 4,
    max_memory_usage = 17179869184,
    max_bytes_before_external_sort = 2000000000
