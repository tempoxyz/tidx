CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_selector_topic2_block
ON logs (selector, topic2, block_num DESC, log_idx DESC);
