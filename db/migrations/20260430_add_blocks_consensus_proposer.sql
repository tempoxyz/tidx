-- TIP-1031: store the ed25519 consensus proposer pubkey for each block.
--
-- Existing PostgreSQL deployments created before this change already have a
-- `blocks` table, so `CREATE TABLE IF NOT EXISTS` in db/blocks.sql will not
-- add the new column. Run this migration once during upgrade, or let tidx
-- apply it via `run_migrations()` on startup.
ALTER TABLE blocks
    ADD COLUMN IF NOT EXISTS consensus_proposer BYTEA;

-- Pubkey must be exactly 32 bytes (or NULL for pre-fork blocks).
-- Add the constraint defensively in case the column already existed without it.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'blocks_consensus_proposer_len'
    ) THEN
        ALTER TABLE blocks
            ADD CONSTRAINT blocks_consensus_proposer_len
            CHECK (consensus_proposer IS NULL OR octet_length(consensus_proposer) = 32);
    END IF;
END$$;
