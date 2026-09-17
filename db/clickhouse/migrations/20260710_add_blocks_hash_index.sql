ALTER TABLE blocks
    ADD INDEX IF NOT EXISTS idx_hash hash TYPE bloom_filter GRANULARITY 1;
