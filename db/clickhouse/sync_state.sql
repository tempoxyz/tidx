-- Sync-engine state for ClickHouse-only chains (mirrors db/sync_state.sql).
-- Partial updates are plain INSERTs; per-column aggregate functions merge them
-- with the same semantics as the Postgres upserts (GREATEST watermarks,
-- first-write-wins started_at). backfill_num uses min: the backfill cursor
-- only descends toward genesis. sync_rate reads use argMax(sync_rate,
-- updated_at); the anyLast column merge keeps the newest part's value.
CREATE TABLE IF NOT EXISTS sync_state (
    chain_id     UInt64,
    head_num     SimpleAggregateFunction(max, Int64),
    synced_num   SimpleAggregateFunction(max, Int64),
    tip_num      SimpleAggregateFunction(max, Int64),
    backfill_num SimpleAggregateFunction(min, Nullable(Int64)),
    sync_rate    SimpleAggregateFunction(anyLast, Nullable(Float64)),
    started_at   SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    updated_at   SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree()
ORDER BY chain_id
