-- Pre-aggregated holder balances, refreshed on a schedule.
--
-- `token_balances` (the plain VIEW) re-aggregates the full
-- `token_holder_deltas` history on every read. For tokens with tens of
-- millions of deltas (e.g. PathUSD) that recompute blows past query timeouts,
-- which surfaced as "0 holders" in the explorer.
--
-- This refreshable materialized view atomically publishes the current balance
-- state in token/balance order. balance_state_refresh incrementally reduces
-- historical deltas first, so this refresh scans one row per current pair
-- instead of the complete transfer history.
--
-- ORDER BY (token, balance) so the explorer's "top holders by balance" and
-- "holder count" queries (both filtered by token) hit the primary key.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_snapshot
REFRESH AFTER 15 MINUTE
ENGINE = MergeTree
ORDER BY (token, balance)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
SELECT token, holder, balance
FROM token_balances
SETTINGS
    max_threads = 8,
    max_memory_usage = 34359738368
