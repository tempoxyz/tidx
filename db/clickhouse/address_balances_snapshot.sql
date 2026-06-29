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
-- Each refresh recomputes from `address_holder_deltas FINAL` and atomically
-- swaps the result, so reads are reorg-correct but can be up to one refresh
-- interval stale. This mirrors `token_balances_snapshot` with the access
-- pattern inverted for address-held token lookups.
--
-- ORDER BY (holder, balance, token) so address balance pages/counts filtered by
-- holder can prune to one account before sorting by balance.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balances_snapshot
REFRESH EVERY 15 MINUTE
ENGINE = MergeTree
ORDER BY (holder, balance, token)
AS
SELECT
    holder,
    token,
    sum(balance_delta) AS balance
FROM address_holder_deltas FINAL
GROUP BY holder, token
HAVING balance > 0
-- The full-history GROUP BY spans hundreds of millions of delta rows. Spill
-- to disk past this threshold so the periodic refresh completes instead of
-- failing with a memory-limit error. Tune to the box.
SETTINGS max_bytes_before_external_group_by = 2000000000
