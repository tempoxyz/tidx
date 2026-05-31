-- Pre-aggregated holder balances, refreshed on a schedule.
--
-- `token_balances` (the plain VIEW) re-aggregates the full
-- `token_holder_deltas` history on every read. For tokens with tens of
-- millions of deltas (e.g. PathUSD) that recompute blows past query timeouts,
-- which surfaced as "0 holders" in the explorer.
--
-- This refreshable materialized view runs the same aggregation periodically
-- and stores the result in its own MergeTree, so holder counts and holder
-- listings become cheap primary-key reads. Each refresh recomputes the whole
-- dataset and atomically swaps it in, so reads are always consistent (but up
-- to one refresh interval stale).
--
-- ORDER BY (token, balance) so the explorer's "top holders by balance" and
-- "holder count" queries (both filtered by token) hit the primary key.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_snapshot
REFRESH EVERY 15 MINUTE
ENGINE = MergeTree
ORDER BY (token, balance)
AS
SELECT
    token,
    holder,
    sum(balance_delta) AS balance
FROM token_holder_deltas FINAL
GROUP BY token, holder
HAVING balance > 0
-- The full-history GROUP BY spans hundreds of millions of delta rows. Spill
-- to disk past this threshold so the periodic refresh completes instead of
-- failing with a memory-limit error (refreshes run under a background user,
-- so this is the reliable place to bound their memory). Tune to the box.
SETTINGS max_bytes_before_external_group_by = 2000000000
