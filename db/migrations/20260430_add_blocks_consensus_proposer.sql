-- Existing PostgreSQL deployments created before TIP-1031 already have a
-- `blocks` table, so `CREATE TABLE IF NOT EXISTS` in db/blocks.sql will not
-- add the new consensus proposer column. Run this migration once during
-- upgrade, or let tidx apply it via `run_migrations()` on startup.
ALTER TABLE blocks
    ADD COLUMN IF NOT EXISTS consensus_proposer BYTEA
    CHECK (consensus_proposer IS NULL OR octet_length(consensus_proposer) = 32);
