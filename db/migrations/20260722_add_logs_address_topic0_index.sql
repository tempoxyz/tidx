CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_address_topic0_block
ON logs (address, topic0, block_num, log_idx);
