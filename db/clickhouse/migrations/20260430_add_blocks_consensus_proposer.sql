-- TIP-1031: store the ed25519 consensus proposer pubkey for each block.
--
-- Existing ClickHouse deployments created before this change already have a
-- `blocks` table, so `CREATE TABLE IF NOT EXISTS` in db/clickhouse/blocks.sql
-- will not add the new column. Run this migration once during upgrade, or let
-- tidx apply it via `ClickHouseSink::ensure_schema()` on startup.
ALTER TABLE blocks
    ADD COLUMN IF NOT EXISTS consensus_proposer Nullable(String);
