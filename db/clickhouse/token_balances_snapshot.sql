-- Pre-aggregated holder balances, refreshed on a schedule.
--
-- `token_balances` (the plain VIEW) re-aggregates the full
-- `token_holder_deltas` history on every read. For tokens with tens of
-- millions of deltas (e.g. PathUSD) that recompute blows past query timeouts,
-- which surfaced as "0 holders" in the explorer.
--
-- This is the single canonical full-history balance refresh. It stores the
-- result in its own MergeTree, so holder counts and holder listings become
-- cheap primary-key reads. Each refresh recomputes from the deduplicated delta
-- ledger and atomically swaps the result, preserving the exact duplicate,
-- retry, and reorg semantics of token_balances (with schedule-bounded staleness).
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
SELECT
    token,
    holder,
    if(
        sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
        toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
        toUInt256(0)
    ) AS balance
FROM token_holder_deltas FINAL
GROUP BY token, holder
HAVING balance > 0
-- The source order starts with the GROUP BY keys, allowing ClickHouse to
-- aggregate in order with bounded CPU and memory. Spill remains enabled as a
-- safety valve for very large histories.
SETTINGS
    optimize_aggregation_in_order = 1,
    max_threads = 4,
    max_memory_usage = 34359738368,
    max_bytes_before_external_group_by = 2000000000
