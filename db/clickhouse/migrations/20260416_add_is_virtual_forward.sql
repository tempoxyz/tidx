-- Existing ClickHouse deployments created before PR #167 already have a `logs`
-- table, so `CREATE TABLE IF NOT EXISTS` in db/clickhouse/logs.sql will not add
-- the new column. Run this migration once during upgrade, or let tidx apply it
-- via `ClickHouseSink::ensure_schema()` on startup.
ALTER TABLE logs
    ADD COLUMN IF NOT EXISTS is_virtual_forward UInt8 DEFAULT 0;
