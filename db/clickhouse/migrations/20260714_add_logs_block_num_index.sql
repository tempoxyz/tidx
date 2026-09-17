ALTER TABLE logs
    ADD INDEX IF NOT EXISTS idx_block_num block_num TYPE minmax GRANULARITY 1;
