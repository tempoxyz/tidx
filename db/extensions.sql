-- Optional extensions.
--
-- OrioleDB provides the compressed, index-organized access method used for
-- new chain-table partitions (see db/partitions.sql). It requires a patched
-- PostgreSQL (the orioledb/orioledb image); on stock PostgreSQL the
-- extension is unavailable and partitions fall back to heap.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'orioledb') THEN
        CREATE EXTENSION IF NOT EXISTS orioledb;
    END IF;
END $$;
