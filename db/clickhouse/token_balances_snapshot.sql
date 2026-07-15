-- Pre-aggregated holder balances, refreshed on a schedule.
--
-- `token_balances` (the plain VIEW) re-aggregates the full
-- `token_holder_deltas` history on every read. For tokens with tens of
-- millions of deltas (e.g. PathUSD) that recompute blows past query timeouts,
-- which surfaced as "0 holders" in the explorer.
--
-- This is the canonical published balance generation. A one-time bootstrap
-- creates cumulative per-holder checkpoints; token_balance_checkpoint_refresh
-- then advances only changed pairs from their post-checkpoint ledger range.
-- Replays and reorgs rebuild only affected pairs from the canonical FINAL
-- ledger. The scheduled publish therefore scans one checkpoint row per pair,
-- not the complete transfer history.
--
-- ORDER BY (token, balance) so the explorer's "top holders by balance" and
-- "holder count" queries (both filtered by token) hit the primary key.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_snapshot
REFRESH EVERY 15 MINUTE DEPENDS ON token_balance_checkpoint_refresh
ENGINE = MergeTree
ORDER BY (token, balance)
SETTINGS default_compression_codec = 'ZSTD(1)'
AS
SELECT
    token,
    holder,
    if(
        credited >= debited,
        toUInt256(credited - debited),
        toUInt256(0)
    ) AS balance
FROM token_balance_checkpoints FINAL
WHERE credited > debited
SETTINGS
    max_threads = 4,
    max_memory_usage = 34359738368
