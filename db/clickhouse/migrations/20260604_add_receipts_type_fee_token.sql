-- Denormalize the tx-level `type` and `fee_token` onto receipt rows so the API
-- can serve receipt lists without joining `receipts` to `txs` on every page.
--
-- Existing ClickHouse deployments created before this change already have a
-- `receipts` table, so `CREATE TABLE IF NOT EXISTS` in db/clickhouse/receipts.sql
-- will not add the new columns. Run this migration once during upgrade, or let
-- tidx apply it via `ClickHouseSink::ensure_schema()` on startup. New rows are
-- populated by `enrich_receipts_from_txs` at decode time; pre-existing rows stay
-- NULL until re-synced.
ALTER TABLE receipts
    ADD COLUMN IF NOT EXISTS `type` Nullable(Int16),
    ADD COLUMN IF NOT EXISTS fee_token Nullable(String);
