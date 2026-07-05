-- Storage layout settings, locked per database at first boot.
--
-- partition_blocks is seeded from config on first migration and never
-- changed afterwards: existing partition boundaries are fixed, and creating
-- partitions with a different width would overlap them.
CREATE TABLE IF NOT EXISTS storage_config (
    single_row       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (single_row),
    partition_blocks INT8 NOT NULL CHECK (partition_blocks > 0)
) USING heap;
