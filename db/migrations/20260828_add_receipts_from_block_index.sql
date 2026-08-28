CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_receipts_from_block
ON receipts ("from", block_num DESC, tx_idx DESC);
