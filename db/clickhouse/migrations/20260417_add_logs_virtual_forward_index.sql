ALTER TABLE logs
    ADD INDEX IF NOT EXISTS idx_virtual_forward is_virtual_forward TYPE set(2) GRANULARITY 1;
