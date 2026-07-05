-- One-shot legacy upgrade: move multicall AA inner calls out of the
-- `txs.calls` JSONB column into the normalized `tx_calls` table, then drop
-- the column and its GIN index.
--
-- Runs as a post-startup migration. The condition makes it a no-op on fresh
-- schemas (which never have the column) and after the first successful run.
-- The whole DO block is one transaction, so the column is only dropped if
-- the backfill committed. New rows written by the current writer are not
-- touched: they have `calls IS NULL` and already produce tx_calls rows, and
-- ON CONFLICT DO NOTHING makes any overlap harmless.
--
-- JSONB call shape (serde of tempo Call): {"to": "0x…"|null, "value": "0x…",
-- "input": "0x…", "data": null}. `value` is a minimal-length hex quantity,
-- so it is left-padded to a whole number of bytes before decoding; abi_uint
-- (db/functions.sql) converts the bytes to NUMERIC, stored as decimal TEXT
-- to match the txs.value format.
DO $$
DECLARE
    min_block INT8;
    max_block INT8;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'txs'
          AND column_name = 'calls'
    ) THEN
        -- tx_calls may be a (fresh) partitioned parent even though txs is a
        -- legacy regular table; create the partitions the backfill will hit.
        SELECT MIN(block_num), MAX(block_num) INTO min_block, max_block
        FROM txs WHERE calls IS NOT NULL AND call_count > 1;
        IF min_block IS NOT NULL THEN
            PERFORM ensure_block_partitions('tx_calls', min_block, max_block);
        END IF;

        INSERT INTO tx_calls (block_num, block_timestamp, tx_idx, call_idx, "to", value, input)
        SELECT t.block_num,
               t.block_timestamp,
               t.idx,
               (c.ord - 1)::INT2,
               CASE WHEN (c.elem->>'to') IS NULL THEN NULL
                    ELSE decode(substr(c.elem->>'to', 3), 'hex') END,
               abi_uint(decode(lpad(substr(c.elem->>'value', 3),
                                    ((length(c.elem->>'value') - 1) / 2) * 2,
                                    '0'),
                               'hex'))::TEXT,
               decode(substr(coalesce(c.elem->>'input', c.elem->>'data'), 3), 'hex')
        FROM txs t
        CROSS JOIN LATERAL jsonb_array_elements(t.calls) WITH ORDINALITY AS c(elem, ord)
        WHERE t.calls IS NOT NULL AND t.call_count > 1
        ON CONFLICT DO NOTHING;

        DROP INDEX IF EXISTS idx_txs_calls_partial;
        ALTER TABLE txs DROP COLUMN calls;
    END IF;
END $$;
