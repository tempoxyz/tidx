CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_virtual_forward
ON logs (block_num DESC, log_idx)
WHERE is_virtual_forward = TRUE;
