ALTER TABLE receipts
    ADD INDEX IF NOT EXISTS idx_to `to`
        TYPE bloom_filter(0.01) GRANULARITY 1;
