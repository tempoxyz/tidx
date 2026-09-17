ALTER TABLE logs
    ADD INDEX IF NOT EXISTS idx_topic3 topic3 TYPE bloom_filter GRANULARITY 1;
