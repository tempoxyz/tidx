ALTER TABLE blocks
    ADD INDEX IF NOT EXISTS idx_timestamp timestamp
        TYPE minmax GRANULARITY 1;
