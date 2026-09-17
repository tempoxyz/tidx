CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_selector_indexed_address
ON logs (selector, abi_address(topic1));
