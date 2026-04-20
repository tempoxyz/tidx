-- Existing PostgreSQL deployments created before PR #167 already have a `logs`
-- table, so `CREATE TABLE IF NOT EXISTS` in db/logs.sql will not add the new
-- column. Run this migration once during upgrade, or let tidx apply it via
-- `run_migrations()` on startup.
ALTER TABLE logs
    ADD COLUMN IF NOT EXISTS is_virtual_forward BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_logs_virtual_forward ON logs (block_num DESC, log_idx)
WHERE is_virtual_forward = TRUE;

CREATE INDEX IF NOT EXISTS idx_logs_tx_hash_virtual_forward ON logs (tx_hash, log_idx)
WHERE is_virtual_forward = TRUE;
