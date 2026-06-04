-- Per-token holder counts, refreshed on a schedule.
--
-- The explorer/API needs "how many holders does this token have?" on every
-- token-detail render. Answering it from `token_balances_snapshot` means
-- `count() … WHERE token = X AND balance > 0`, a primary-key range scan that
-- still touches one row per holder — millions of rows for tokens like PathUSD.
--
-- This refreshable materialized view collapses the snapshot to a single row
-- per token and stores it in its own MergeTree ordered by `(token)`, so the
-- count becomes a single-row primary-key lookup. It reads from
-- `token_balances_snapshot` (already deduped, one positive-balance row per
-- (token, holder)) rather than re-aggregating `token_holder_deltas`, so each
-- refresh is cheap. Counts are at most one refresh interval staler than the
-- snapshot they derive from.
--
-- Requires `allow_experimental_refreshable_materialized_view` at creation time
-- (still experimental as of ClickHouse 25.x); the sink sets it when applying
-- this DDL.
CREATE MATERIALIZED VIEW IF NOT EXISTS token_holder_counts
REFRESH EVERY 15 MINUTE
ENGINE = MergeTree
ORDER BY (token)
AS
SELECT
    token,
    count() AS holder_count
FROM token_balances_snapshot
GROUP BY token
