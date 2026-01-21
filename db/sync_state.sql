CREATE TABLE IF NOT EXISTS sync_state (
    chain_id        INT8 PRIMARY KEY,
    head_num        INT8 NOT NULL DEFAULT 0,
    synced_num      INT8 NOT NULL DEFAULT 0,
    tip_num         INT8 NOT NULL DEFAULT 0,
    backfill_num    INT8,
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN sync_state.synced_num IS 'Highest contiguous block synced from genesis (no gaps up to here)';
COMMENT ON COLUMN sync_state.tip_num IS 'Highest block synced near chain head (may have gaps below)';
COMMENT ON COLUMN sync_state.backfill_num IS 'Lowest block synced going backwards (NULL=not started, 0=complete)';
COMMENT ON COLUMN sync_state.started_at IS 'When this sync instance started';
