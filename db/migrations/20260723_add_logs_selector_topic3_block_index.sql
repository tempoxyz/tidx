CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_selector_topic3_block
ON logs (selector, topic3, block_num DESC, log_idx DESC);
