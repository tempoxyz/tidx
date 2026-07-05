CREATE TABLE IF NOT EXISTS txs (
    block_num               INT8 NOT NULL,
    block_timestamp         TIMESTAMPTZ NOT NULL,
    idx                     INT4 NOT NULL,
    hash                    BYTEA NOT NULL,
    type                    INT2 NOT NULL,
    "from"                  BYTEA NOT NULL,
    "to"                    BYTEA,
    value                   TEXT NOT NULL,
    input                   BYTEA NOT NULL,
    gas_limit               INT8 NOT NULL,
    max_fee_per_gas         TEXT NOT NULL,
    max_priority_fee_per_gas TEXT NOT NULL,
    gas_used                INT8,
    nonce_key               BYTEA NOT NULL,
    nonce                   INT8 NOT NULL,
    fee_token               BYTEA,
    fee_payer               BYTEA,
    call_count              INT2 NOT NULL DEFAULT 1,
    valid_before            INT8,
    valid_after             INT8,
    signature_type          INT2,
    PRIMARY KEY (block_timestamp, block_num, idx)
);

CREATE INDEX IF NOT EXISTS idx_txs_hash ON txs (hash);
CREATE INDEX IF NOT EXISTS idx_txs_block_num ON txs (block_num DESC);
DROP INDEX IF EXISTS idx_txs_block_num_asc;
CREATE INDEX IF NOT EXISTS idx_txs_from ON txs ("from", block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_txs_to ON txs ("to", block_timestamp DESC);
DROP INDEX IF EXISTS idx_txs_calls;
DROP INDEX IF EXISTS idx_txs_selector;
-- The former `calls` JSONB column and its GIN index (idx_txs_calls_partial)
-- are migrated into the `tx_calls` table by
-- db/migrations/20260705_normalize_tx_calls.sql.

-- LZ4 TOAST compression for wide values (PG14+). Metadata-only; applies to
-- newly written rows. Existing rows keep their current compression.
ALTER TABLE txs ALTER COLUMN input SET COMPRESSION lz4;
