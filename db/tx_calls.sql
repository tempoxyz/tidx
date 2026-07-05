-- Inner calls of multicall AA (type 0x76) transactions, one row per call.
--
-- Replaces the former `txs.calls` JSONB column and its GIN index. Only txs
-- with call_count > 1 have rows here: the AA envelope derives its top-level
-- `to`/`value`/`input` from calls[0] (value = sum of call values), so a
-- single-call AA tx mirrors its only call in the `txs` row itself.
CREATE TABLE IF NOT EXISTS tx_calls (
    block_num       INT8 NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    tx_idx          INT4 NOT NULL,
    call_idx        INT2 NOT NULL,
    "to"            BYTEA,              -- NULL = contract creation
    value           TEXT COLLATE "C" NOT NULL, -- decimal string, same format as txs.value
    input           BYTEA NOT NULL,
    PRIMARY KEY (block_timestamp, block_num, tx_idx, call_idx)
) PARTITION BY RANGE (block_num);

-- Serves the AA inner-call recipient filter (tempo-api2 `includeCallRecipients`):
-- ("to" = X OR EXISTS (SELECT 1 FROM tx_calls c WHERE ... AND c."to" = X))
CREATE INDEX IF NOT EXISTS idx_tx_calls_to ON tx_calls ("to", block_timestamp DESC) WHERE "to" IS NOT NULL;
-- Reorg deletes and block-range scans.
CREATE INDEX IF NOT EXISTS idx_tx_calls_block_num ON tx_calls (block_num DESC);

-- LZ4 TOAST compression for wide values on heap layouts (PG14+;
-- metadata-only, applies to newly written rows). Skipped when orioledb is
-- installed: orioledb tables use their own zstd compression and reject
-- SET COMPRESSION.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'orioledb') THEN
        ALTER TABLE tx_calls ALTER COLUMN input SET COMPRESSION lz4;
    END IF;
END $$;
