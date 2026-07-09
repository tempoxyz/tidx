CREATE TABLE IF NOT EXISTS sync_state (
    chain_id        INT8 PRIMARY KEY,
    head_num        INT8 NOT NULL DEFAULT 0,
    synced_num      INT8 NOT NULL DEFAULT 0,
    tip_num         INT8 NOT NULL DEFAULT 0,
    backfill_num    INT8,
    sync_rate       FLOAT8,
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ClickHouse backfill cursor: highest block successfully written to all CH tables.
-- Persisted in PG so it survives restarts and isn't fooled by realtime sync writing ahead.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS ch_backfill_block INT8 NOT NULL DEFAULT 0;

-- Tiered-storage prune floor: highest block intentionally pruned from Postgres.
-- Gap detection and backfill ignore blocks at or below this.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS pruned_below INT8 NOT NULL DEFAULT 0;

COMMENT ON COLUMN sync_state.synced_num IS 'Highest contiguous block synced from genesis (no gaps up to here)';
COMMENT ON COLUMN sync_state.tip_num IS 'Highest block synced near chain head (may have gaps below)';
COMMENT ON COLUMN sync_state.backfill_num IS 'Lowest block synced going backwards (NULL=not started, 0=complete)';
COMMENT ON COLUMN sync_state.sync_rate IS 'Current sync rate in blocks/second (rolling 5s window)';
COMMENT ON COLUMN sync_state.started_at IS 'When this sync instance started';
COMMENT ON COLUMN sync_state.ch_backfill_block IS 'ClickHouse backfill cursor: highest block written to all CH tables (0=not started)';
COMMENT ON COLUMN sync_state.pruned_below IS 'Highest block intentionally pruned from Postgres (0=nothing pruned); gap detection ignores blocks at or below this';
