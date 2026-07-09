ALTER TABLE txs
    ADD INDEX IF NOT EXISTS idx_from `from` TYPE bloom_filter GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_to   `to`   TYPE bloom_filter GRANULARITY 1;
