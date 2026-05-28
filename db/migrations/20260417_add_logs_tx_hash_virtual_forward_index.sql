CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_tx_hash_virtual_forward
ON logs (tx_hash, log_idx)
WHERE is_virtual_forward = TRUE;
