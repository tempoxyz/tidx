CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_selector_topic1_block
ON logs (selector, topic1, block_num DESC, log_idx DESC);
